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

确认时限与超时结论
------------------
下发期望时可带 ``ack_timeout_ms``：

* 设备**在线**：``deadline = 下发时刻 + 时限``；
* 设备**离线**：时限暂不激活（``deadline=None``），设备**重新上线那一刻**
  才开始计算，避免刚上线就被判超时；
* 超过时限仍处于 PENDING（未收到设备表态）-> 结论为独立的
  ``ACK_TIMEOUT``，与 ``CONFLICTED``（设备按旧版本执行 / 键值对不上）明确
  区分，``timeout_detail()`` 给出可解释明细；
* 超时是**可纠正的中间结论**：时限之后设备补来正确确认，对账会把它
  自动改回 ``IN_SYNC``，不会一直卡在超时；补来错误/旧版本确认则为
  ``CONFLICTED``。已 ``IN_SYNC`` / ``CONFLICTED`` 的结论不会被超时覆盖。

撤销未确认期望
--------------
``revoke_desired`` 撤销一次尚未被设备确认的变更，取值回到该变更之前
**最后一次已被确认**的期望取值：

* 撤销本身也是一次新版本，版本号单调递增、绝不回退或重用；
* 幂等：当前版本就是撤销版且尚未被确认时，重复撤销不多出一版；
* 已确认生效（``IN_SYNC`` / ``confirmed``）的期望不允许撤销，须重新下发
  覆盖（``ALREADY_CONFIRMED``）；
* 撤销、新下发、设备确认并发时由同一把锁串行化，配合 ``expected_version``
  乐观锁，杜绝丢失更新、取值无故回退与版本错乱。
"""
from __future__ import annotations

import threading
from dataclasses import replace

from .errors import DomainError, ErrorCode
from .models import (
    ConnectionState,
    DesiredState,
    ReportedState,
    SyncStatus,
    TwinState,
)

_ACK_TIMEOUT = SyncStatus.ACK_TIMEOUT


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
        # 期望版本单调序列：包括下发与撤销在内的每次期望变更都消费一个版本号，
        # 即使撤销后回到更早的取值，版本也绝不回退、绝不重用。
        self._desired_version_seq = 0

    # ---- 连接生命周期 ----
    def connect(self, now_ms: int) -> TwinState:
        with self._lock:
            was_offline = self.state.connection is ConnectionState.OFFLINE
            self.state.connection = ConnectionState.ONLINE
            self.state.connected_since_ms = now_ms
            self.state.last_event_ms = now_ms
            # 重连时若仍有未完成的期望状态，保持 PENDING 等待重发对账
            if self.state.sync_status is SyncStatus.CONFLICTED:
                self.state.sync_status = SyncStatus.PENDING
            # 离线期间下发（或离线期间超时未决）的期望：确认时限从重新上线
            # 这一刻开始算，而不是从下发时刻算，避免刚上线就被判超时。
            desired = self.state.desired
            if was_offline and desired is not None and not desired.confirmed:
                if desired.ack_timeout_ms is not None:
                    desired.deadline_ms = now_ms + desired.ack_timeout_ms
                if self.state.sync_status is _ACK_TIMEOUT:
                    self.state.sync_status = SyncStatus.PENDING
            self.evaluate_timeout(now_ms)
            return self.snapshot()

    def disconnect(self, now_ms: int) -> TwinState:
        with self._lock:
            self.state.connection = ConnectionState.OFFLINE
            self.state.connected_since_ms = None
            self.state.last_event_ms = now_ms
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
            # 先按时钟刷新超时结论，再让本次上报（可能携带迟到的正确确认）对账：
            # 迟到的正确确认可把 ACK_TIMEOUT 自动纠正为 IN_SYNC。
            self.evaluate_timeout(now_ms)
            self._reconcile(ack_desired_version)
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
        conflicting, details = _values_conflict(desired.values, reported.values)
        if conflicting:
            self.state.sync_status = SyncStatus.CONFLICTED
            return
        self.state.sync_status = SyncStatus.IN_SYNC
        # 记录本版本已确认，作为后续撤销可回退到的最后基线
        desired.confirmed = True
        self.state.last_confirmed_desired = DesiredState(
            version=desired.version,
            values=dict(desired.values),
            updated_at_ms=desired.updated_at_ms,
        )

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

    # ---- 确认时限 / 超时 ----
    def evaluate_timeout(self, now_ms: int) -> SyncStatus:
        """按时钟刷新确认结论：超过确认时限仍未确认 -> ACK_TIMEOUT。

        * 只在尚在等待确认（PENDING）时可能转入超时；已经 IN_SYNC /
          CONFLICTED 的结论不会被超时覆盖（它们是设备已表态后的结论）；
        * 离线期间时限未激活（``deadline_ms=None``），不计超时；
        * 超时是可纠正的中间结论：之后设备补来正确确认，``_reconcile``
          会把它自动改回 IN_SYNC。
        调用方须持锁（本方法也可被外部安全调用，内部自带锁）。
        """
        with self._lock:
            desired = self.state.desired
            if desired is None or desired.confirmed:
                return self.state.sync_status
            if self.state.sync_status is not SyncStatus.PENDING:
                return self.state.sync_status
            if desired.deadline_ms is not None and now_ms >= desired.deadline_ms:
                self.state.sync_status = _ACK_TIMEOUT
            return self.state.sync_status

    def timeout_detail(self) -> str:
        """返回当前超时结论的可解释明细；非超时返回空串。"""
        with self._lock:
            if self.state.sync_status is not _ACK_TIMEOUT:
                return ""
            desired = self.state.desired
            if desired is None or desired.deadline_ms is None:
                return "超过确认时限仍未收到设备确认"
            return (
                f"期望版本 v{desired.version} 超过确认时限 "
                f"(deadline_ms={desired.deadline_ms}) 仍未收到设备确认；"
                f"并非设备按旧版本执行，也非键值对不上"
            )

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
            if ack_timeout_ms is not None and ack_timeout_ms <= 0:
                raise DomainError(
                    ErrorCode.INVALID_REQUEST,
                    "ack_timeout_ms 必须为正数（毫秒）",
                )
            current = self.state.desired
            if expected_version is not None:
                current_version = current.version if current else 0
                if expected_version != current_version:
                    raise DomainError(
                        ErrorCode.VERSION_CONFLICT,
                        f"期望版本冲突: 期望 v{expected_version}, 实际 v{current_version}",
                    )
            if current is None:
                new_values = dict(values)
            elif merge:
                new_values = dict(current.values)
                new_values.update(values)
            else:
                new_values = dict(values)
            new_version = self._next_desired_version(current)
            # 在线：时限从下发时刻开始算；离线：时限暂不激活，重连时再开始算。
            deadline = None
            if ack_timeout_ms is not None and self.state.connection is ConnectionState.ONLINE:
                deadline = now_ms + ack_timeout_ms
            self.state.desired = DesiredState(
                version=new_version,
                values=new_values,
                updated_at_ms=now_ms,
                expected_version=expected_version,
                ack_timeout_ms=ack_timeout_ms,
                deadline_ms=deadline,
            )
            self.state.last_event_ms = now_ms
            # 新期望下发后：若设备在线则等待其对账；离线则保持 PENDING。
            # 已有的 CONFLICTED/ACK_TIMEOUT 结论被新期望重置，等待重新对账。
            self.state.sync_status = SyncStatus.PENDING
            return self.snapshot()

    def _next_desired_version(self, current: DesiredState | None) -> int:
        """取下一个单调期望版本号。调用方须持锁。"""
        if self._desired_version_seq == 0:
            self._desired_version_seq = current.version if current else 0
        self._desired_version_seq += 1
        return self._desired_version_seq

    def revoke_desired(self, now_ms: int, expected_version: int | None = None) -> TwinState:
        """撤销一次尚未被设备确认的期望变更。

        撤销后取值回到本次变更之前**最后一次已被确认**的期望取值；
        版本号仍然单调递增（撤销也是一次新版本），绝不回退或重用。

        * 幂等：重复撤销当前这个撤销版不会再多出一版；
        * 已确认生效（IN_SYNC/confirmed）的期望不允许直接撤销，需重新下发覆盖；
        * 可传 ``expected_version`` 做乐观并发控制。
        """
        with self._lock:
            self.evaluate_timeout(now_ms)
            current = self.state.desired
            if current is None:
                raise DomainError(
                    ErrorCode.STATE_CONFLICT,
                    "当前没有期望状态，无法撤销",
                )
            if expected_version is not None and expected_version != current.version:
                raise DomainError(
                    ErrorCode.VERSION_CONFLICT,
                    f"撤销版本冲突: 期望 v{expected_version}, 实际 v{current.version}",
                )
            # 已确认生效的期望（包括已被确认的撤销版）一律不允许撤销。
            # 该判断先于幂等早返回，避免"已确认的撤销版"被重复撤销静默放过。
            if current.confirmed or self.state.sync_status is SyncStatus.IN_SYNC:
                raise DomainError(
                    ErrorCode.ALREADY_CONFIRMED,
                    f"期望版本 v{current.version} 已被设备确认生效，"
                    "不允许撤销，请重新下发覆盖",
                )
            # 幂等：当前版本本身就是一次撤销版（且尚未被确认），重复撤销直接返回现状
            if current.revoke_of_version is not None:
                return self.snapshot()

            baseline = self.state.last_confirmed_desired
            new_version = self._next_desired_version(current)
            if baseline is None:
                # 从未确认过任何期望：撤销即清空，等待第一次确认
                self.state.desired = None
            else:
                self.state.desired = DesiredState(
                    version=new_version,
                    values=dict(baseline.values),
                    updated_at_ms=now_ms,
                    revoke_of_version=current.version,
                )
            self.state.last_event_ms = now_ms
            # 撤销版等待设备确认回到基线取值；无期望可对账时为 PENDING。
            self.state.sync_status = SyncStatus.PENDING
            return self.snapshot()

    def mark_delivered(self, now_ms: int) -> None:
        """重连后把期望状态标记为已重发（仍为 PENDING，等待设备确认）。"""
        with self._lock:
            self.state.last_event_ms = now_ms

    def snapshot(self) -> TwinState:
        with self._lock:
            snap = replace(
                self.state,
                desired=(
                    replace(self.state.desired) if self.state.desired is not None else None
                ),
                reported=(
                    replace(self.state.reported) if self.state.reported is not None else None
                ),
                last_confirmed_desired=(
                    replace(self.state.last_confirmed_desired)
                    if self.state.last_confirmed_desired is not None
                    else None
                ),
            )
            return snap
