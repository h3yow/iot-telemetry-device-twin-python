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

    # ---- 连接生命周期 ----
    def connect(self, now_ms: int) -> TwinState:
        with self._lock:
            self.state.connection = ConnectionState.ONLINE
            self.state.connected_since_ms = now_ms
            self.state.last_event_ms = now_ms
            # 重连时若仍有未完成的期望状态，保持 PENDING 等待重发对账
            if self.state.sync_status is SyncStatus.CONFLICTED:
                self.state.sync_status = SyncStatus.PENDING
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
            return self.snapshot()

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
