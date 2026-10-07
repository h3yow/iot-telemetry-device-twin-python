"""设备孪生：期望状态 / 上报状态分离与一致性对账。

并发与版本
----------
* 每个孪生一把可重入锁，期望状态更新串行化，杜绝丢失更新；
* 期望状态带单调递增 ``version``，更新时可传 ``expected_version`` 做
  乐观并发控制，版本不符抛 ``VERSION_CONFLICT``；
* ``merge=True`` 为补丁合并，``merge=False`` 为整体替换。

冲突判定（确定性、可解释）
--------------------------
设备上报时携带它所看到的期望版本 ``ack_desired_version``：

* 上报版本 == 当前期望版本：逐键比对，完全一致 -> ``IN_SYNC``，
  任一键值不一致 -> ``CONFLICTED``；
* 上报版本 < 当前期望版本（设备基于旧期望执行）-> ``CONFLICTED``；
* 未携带确认版本或当前没有期望状态 -> ``PENDING``（含离线期间下发，
  重连后仍待设备确认）。

防倒退
------
上报状态按 ``(ts_ms, seq)`` 水位并入，过期样本不会倒退上报状态。

确认时限（ACK_TIMEOUT）
-----------------------
下发期望时可带 ``ack_timeout_ms``：设备在线时从下发时刻起算；离线期间
下发的，离线不计时，重连那一刻才开始计时。超时后结论为独立的
``ACK_TIMEOUT``，区别于"按旧版本执行/取值对不上"的 ``CONFLICTED``。
超时是可纠正的中间结论：迟到的正确确认会自动改判 ``IN_SYNC``，错误确认
改判 ``CONFLICTED``。超时判定为惰性复核，不依赖后台定时器。

撤销（rollback）
---------------
只允许撤销当前挂起、尚未确认的期望；撤销回到上一次已确认取值并另起一个
单调新版本（不回收旧版本号，避免与迟到确认混淆）。对同一版本重复撤销
幂等；已确认生效的期望抛出 ``ALREADY_CONFIRMED``，需重新下发覆盖。
"""
from __future__ import annotations

import threading
from dataclasses import replace

from .errors import DomainError, ErrorCode
from .models import (
    ConnectionState,
    DesiredState,
    ReportedState,
    RollbackOutcome,
    SyncStatus,
    TwinState,
)


def _values_conflict(desired: dict, reported: dict) -> tuple[bool, list[str]]:
    """逐键比对期望值与上报值，返回(是否冲突, 冲突键说明)。"""
    conflicts: list[str] = []
    for key, want in desired.items():
        if key in reported and reported[key] != want:
            conflicts.append(f"{key}: desired={want!r} reported={reported[key]!r}")
    return (len(conflicts) > 0, conflicts)


class DeviceTwin:
    def __init__(self, device_id: str) -> None:
        self.device_id = device_id
        self.state = TwinState(device_id=device_id)
        self._lock = threading.RLock()
        # 当前挂起期望的确认时限（None 表示未设定/已不计时）
        self._ack_deadline_ms: int | None = None
        # 当前挂起期望允许的确认时长（重连时据此从上线时刻重新起算）
        self._ack_timeout_ms: int | None = None
        # 最近一次被设备确认生效的期望（用于撤销回退）
        self._confirmed_desired: DesiredState | None = None
        # 已撤销记录：{被撤销的期望版本: 回退到的版本}，保证重复撤销幂等
        self._rolled_back_versions: dict[int, int | None] = {}

    # ---- 连接生命周期 ----
    def connect(self, now_ms: int) -> TwinState:
        with self._lock:
            self.state.connection = ConnectionState.ONLINE
            self.state.connected_since_ms = now_ms
            self.state.last_event_ms = now_ms
            # 重连时若仍有未完成的期望状态，保持 PENDING 等待重发对账
            if self.state.sync_status is SyncStatus.CONFLICTED:
                self.state.sync_status = SyncStatus.PENDING
            # 离线期间挂起（或上次在线未确认）的期望，时限从重新上线一刻重算，
            # 绝不从下发时刻起算，避免设备刚重连就被判超时。
            if self.state.desired is not None and self._ack_timeout_ms is not None:
                if self.state.sync_status in (
                    SyncStatus.PENDING,
                    SyncStatus.ACK_TIMEOUT,
                ):
                    # 时限重新起算意味着一个新的确认窗口：旧的超时结论复位
                    self.state.sync_status = SyncStatus.PENDING
                    self._ack_deadline_ms = now_ms + self._ack_timeout_ms
            return self.snapshot()

    def disconnect(self, now_ms: int) -> TwinState:
        with self._lock:
            self.state.connection = ConnectionState.OFFLINE
            self.state.connected_since_ms = None
            self.state.last_event_ms = now_ms
            # 离线不计时；重新上线后按剩余挂起期望重新起算
            self._ack_deadline_ms = None
            return self.snapshot()

    # ---- 上报状态 ----
    def apply_reported(
        self,
        values: dict,
        seq: int,
        ts_ms: int,
        now_ms: int,
        ack_desired_version: int | None = None,
    ) -> tuple[bool, SyncStatus, str]:
        """并入一次上报，返回 (是否计入, 同步状态, 判定说明)。"""
        with self._lock:
            reported = self.state.reported
            if reported is not None:
                if ts_ms < reported.updated_at_ms or (
                    ts_ms == reported.updated_at_ms and seq <= reported.last_seq
                ):
                    return (
                        False,
                        self.state.sync_status,
                        f"过期上报 ts={ts_ms},seq={seq} 低于水位 "
                        f"ts={reported.updated_at_ms},seq={reported.last_seq}",
                    )

            merged_values = dict(reported.values) if reported else {}
            merged_values.update(values)
            new_version = (reported.version + 1) if reported else 1
            self.state.reported = ReportedState(
                version=new_version,
                values=merged_values,
                updated_at_ms=ts_ms,
                last_seq=seq,
            )
            self.state.last_event_ms = now_ms
            self._reconcile(ack_desired_version)
            # 对账可能确认、冲突或纠正之前的超时结论；之后再惰性复核时限
            self._evaluate_ack_deadline_locked(now_ms)
            return True, self.state.sync_status, "上报已并入并完成对账"

    def _reconcile(self, ack_desired_version: int | None) -> None:
        """根据设备确认的期望版本做确定性对账。调用方须持锁。"""
        desired = self.state.desired
        reported = self.state.reported
        if desired is None or reported is None:
            self.state.sync_status = SyncStatus.PENDING
            return
        if ack_desired_version is None:
            # 不携带确认：保留既有对账结论，避免状态无故回退
            # （初始为 PENDING；新期望下发也会显式重置为 PENDING）。
            return
        if ack_desired_version < desired.version:
            self.state.sync_status = SyncStatus.CONFLICTED
            return
        if ack_desired_version > desired.version:
            # 确认的是已被撤销/被更新取代的版本：该确认已作废，结论保持不变
            return
        conflicting, details = _values_conflict(desired.values, reported.values)
        if conflicting:
            self.state.sync_status = SyncStatus.CONFLICTED
            return
        # 正确确认当前版本：记录"最近一次已确认生效"的期望并停止计时。
        # 即使此前已经判过 ACK_TIMEOUT，迟到的正确确认也会把结论纠正为 IN_SYNC。
        self.state.sync_status = SyncStatus.IN_SYNC
        self._confirmed_desired = replace(desired)
        self._ack_deadline_ms = None

    def conflict_detail(self) -> str:
        """返回当前冲突的可解释明细；未冲突时返回空串。"""
        with self._lock:
            if self.state.sync_status is not SyncStatus.CONFLICTED:
                return ""
            desired = self.state.desired
            reported = self.state.reported
            if desired is None or reported is None:
                return "缺少期望或上报状态"
            _, details = _values_conflict(desired.values, reported.values)
            if not details:
                return (
                    f"设备基于旧期望版本 v{reported.version} 执行，"
                    f"当前期望版本为 v{desired.version}"
                )
            return "键值不一致: " + "; ".join(details)

    # ---- 期望状态 ----
    def update_desired(
        self,
        values: dict,
        expected_version: int | None,
        now_ms: int,
        merge: bool = True,
        ack_timeout_ms: int | None = None,
    ) -> TwinState:
        with self._lock:
            current = self.state.desired
            if expected_version is not None:
                current_version = current.version if current else 0
                if expected_version != current_version:
                    raise DomainError(
                        ErrorCode.VERSION_CONFLICT,
                        f"期望版本冲突: 期望 v{expected_version}, 实际 v{current_version}",
                    )
            if current is None:
                new_version = 1
                new_values = dict(values)
            elif merge:
                new_version = current.version + 1
                new_values = dict(current.values)
                new_values.update(values)
            else:
                new_version = current.version + 1
                new_values = dict(values)
            self.state.desired = DesiredState(
                version=new_version,
                values=new_values,
                updated_at_ms=now_ms,
                expected_version=expected_version,
            )
            self.state.last_event_ms = now_ms
            # 新期望下发后：若设备在线则等待其对账；离线则保持 PENDING。
            # 已有的 CONFLICTED 结论被新期望重置，等待重新对账。
            self.state.sync_status = SyncStatus.PENDING
            # 确认时限：只有设定了时长且设备在线时才从下发时刻起算；
            # 设备离线时只记下时长，等重新上线那一刻再开始计时。
            self._ack_timeout_ms = ack_timeout_ms if ack_timeout_ms is not None else None
            if ack_timeout_ms is not None and self.state.connection is ConnectionState.ONLINE:
                self._ack_deadline_ms = now_ms + ack_timeout_ms
            else:
                self._ack_deadline_ms = None
            return self.snapshot()

    # ---- 确认时限 ----
    def evaluate_ack_deadline(self, now_ms: int) -> SyncStatus:
        """惰性检查挂起期望是否超过确认时限；超时则给出 ACK_TIMEOUT。

        超时是一个可被后续正确确认自动纠正的中间结论：它只在当前仍为
        PENDING（即设备既没确认、也没报出键值/版本冲突）时成立。
        """
        with self._lock:
            return self._evaluate_ack_deadline_locked(now_ms)

    def _evaluate_ack_deadline_locked(self, now_ms: int) -> SyncStatus:
        """调用方须持锁。"""
        if (
            self._ack_deadline_ms is not None
            and self.state.desired is not None
            and self.state.sync_status is SyncStatus.PENDING
            and now_ms >= self._ack_deadline_ms
        ):
            self.state.sync_status = SyncStatus.ACK_TIMEOUT
        return self.state.sync_status

    def ack_deadline(self) -> int | None:
        with self._lock:
            return self._ack_deadline_ms

    # ---- 撤销 ----
    def rollback_desired(
        self,
        now_ms: int,
        version: int | None = None,
    ) -> RollbackOutcome:
        """撤销一次尚未被确认的期望变更，回到上一次已确认取值。

        规则：

        * 只能撤销"当前挂起、尚未被确认"的期望；已确认生效的期望不允许
          撤销（``ALREADY_CONFIRMED``），需要走一次新的下发去覆盖；
        * 撤销不回收旧版本号，而是按单调递增另起一版承载回退后取值，
          杜绝与迟到确认/并发下发的版本错乱；
        * 对同一版本重复撤销直接返回首拍结论（``idempotent=True``），
          不会多出一版、不会重复回退。
        """
        with self._lock:
            current = self.state.desired
            target_version = version if version is not None else (
                current.version if current is not None else None
            )
            if target_version is None:
                return RollbackOutcome(
                    self.device_id, rolled_back=False,
                    detail="当前没有任何期望状态，无需撤销",
                )

            # 幂等：该版本此前已撤销过 -> 原样返回，不再产生新版本
            if target_version in self._rolled_back_versions:
                restored_to = self._rolled_back_versions[target_version]
                cur_version = current.version if current else None
                return RollbackOutcome(
                    self.device_id, rolled_back=True, idempotent=True,
                    desired_version=cur_version,
                    detail=f"期望 v{target_version} 已撤销过，回退到 v{restored_to}，重复撤销不产生新版本",
                )

            if current is None or current.version != target_version:
                raise DomainError(
                    ErrorCode.VERSION_CONFLICT,
                    f"期望 v{target_version} 已不是当前挂起版本"
                    f"（当前 v{current.version if current else None}），无法撤销",
                )
            if self.state.sync_status is SyncStatus.IN_SYNC:
                raise DomainError(
                    ErrorCode.ALREADY_CONFIRMED,
                    f"期望 v{target_version} 已被设备确认生效，不允许撤销，请重新下发覆盖",
                )

            confirmed = self._confirmed_desired
            new_version = current.version + 1
            if confirmed is None:
                # 此前从未确认过任何期望：撤销即清空期望
                self.state.desired = None
                self.state.sync_status = SyncStatus.PENDING
                restored_to: int | None = None
            else:
                self.state.desired = replace(
                    confirmed,
                    version=new_version,
                    updated_at_ms=now_ms,
                    expected_version=None,
                )
                restored_to = new_version
                # 回退后以最近一次上报重新对账：值仍相等则 IN_SYNC，
                # 设备实际取值已漂移则 CONFLICTED；没有上报则 PENDING。
                if self.state.reported is None:
                    self.state.sync_status = SyncStatus.PENDING
                else:
                    conflicting, _ = _values_conflict(
                        self.state.desired.values, self.state.reported.values
                    )
                    self.state.sync_status = (
                        SyncStatus.CONFLICTED if conflicting else SyncStatus.IN_SYNC
                    )
                    if self.state.sync_status is SyncStatus.IN_SYNC:
                        self._confirmed_desired = replace(self.state.desired)

            self._rolled_back_versions[target_version] = restored_to
            self._ack_deadline_ms = None
            self._ack_timeout_ms = None
            self.state.last_event_ms = now_ms
            return RollbackOutcome(
                self.device_id,
                rolled_back=True,
                desired_version=self.state.desired.version
                if self.state.desired else None,
                detail=(
                    f"已撤销未确认的期望 v{target_version}，回到上一次已确认取值"
                    + (f" v{restored_to}" if restored_to is not None else "（空）")
                ),
            )

    def mark_delivered(self, now_ms: int) -> None:
        """重连后把期望状态标记为已重发（仍为 PENDING，等待设备确认）。"""
        with self._lock:
            self.state.last_event_ms = now_ms

    def snapshot(self) -> TwinState:
        with self._lock:
            return replace(
                self.state,
                desired=(
                    replace(self.state.desired) if self.state.desired is not None else None
                ),
                reported=(
                    replace(self.state.reported) if self.state.reported is not None else None
                ),
            )
