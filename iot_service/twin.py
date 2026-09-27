"""设备孪生：期望/上报状态分离与并发一致性。

规则（确定性、可解释）：
- update_desired 采用 CAS：调用方必须携带读取时的 desired_version，
  版本不匹配返回 VERSION_CONFLICT，杜绝并发丢失更新；
- apply_reported 以遥测 seq 单调递增为准，过期上报被拒绝，状态不倒退；
- sync_on_reconnect 在重连时逐键比对 desired 与 reported：
    * 全部一致  -> 期望状态已生效（applied=True）；
    * 存在差异  -> 判定为冲突（conflict=True），detail 中列出每个差异键的
      desired/reported 值与结论（desired 保持挂起，等待设备上报匹配值），
      结论只取决于当前 twin 内容，因此是确定且可重放的。
"""
from __future__ import annotations

import threading
from typing import Any

from iot_service.models import RejectReason, TwinState, TwinUpdateResult


class TwinManager:
    def __init__(self) -> None:
        self._twins: dict[str, TwinState] = {}
        self._last_reported_seq: dict[str, int] = {}
        self._lock = threading.Lock()

    def get(self, device_id: str) -> TwinState:
        with self._lock:
            twin = self._twins.get(device_id)
            if twin is None:
                return TwinState()
            return TwinState(
                desired=dict(twin.desired),
                reported=dict(twin.reported),
                desired_version=twin.desired_version,
                reported_version=twin.reported_version,
            )

    def _get_or_create(self, device_id: str) -> TwinState:
        return self._twins.setdefault(device_id, TwinState())

    def update_desired(self, device_id: str, patch: dict[str, Any], base_version: int) -> TwinUpdateResult:
        with self._lock:
            twin = self._get_or_create(device_id)
            if base_version != twin.desired_version:
                return TwinUpdateResult(
                    applied=False,
                    reason=RejectReason.VERSION_CONFLICT,
                    detail=(f"base_version={base_version} 与当前 desired_version="
                            f"{twin.desired_version} 不一致，更新被拒绝以避免丢失更新"),
                    version=twin.desired_version,
                )
            twin.desired.update(patch)
            twin.desired_version += 1
            return TwinUpdateResult(applied=True, version=twin.desired_version)

    def apply_reported(self, device_id: str, reported: dict[str, Any], seq: int) -> TwinUpdateResult:
        with self._lock:
            twin = self._get_or_create(device_id)
            last_seq = self._last_reported_seq.get(device_id, 0)
            if seq <= last_seq:
                return TwinUpdateResult(
                    applied=False,
                    reason=RejectReason.STALE_MESSAGE,
                    detail=f"reported seq={seq} 不大于已应用 seq={last_seq}，忽略以防状态回退",
                    version=twin.reported_version,
                )
            twin.reported.update(reported)
            twin.reported_version += 1
            self._last_reported_seq[device_id] = seq
            return TwinUpdateResult(applied=True, version=twin.reported_version)

    def sync_on_reconnect(self, device_id: str) -> TwinUpdateResult:
        with self._lock:
            twin = self._get_or_create(device_id)
            diffs = {
                k: (v, twin.reported.get(k))
                for k, v in twin.desired.items()
                if twin.reported.get(k) != v
            }
            if not diffs:
                return TwinUpdateResult(
                    applied=True,
                    detail="重连校验：desired 与 reported 完全一致，期望状态已生效",
                    version=twin.desired_version,
                )
            lines = [
                f"键 '{k}': desired={d!r} reported={r!r} -> desired 保持挂起，等待设备上报匹配值"
                for k, (d, r) in sorted(diffs.items())
            ]
            return TwinUpdateResult(
                applied=False,
                conflict=True,
                detail="重连校验发现 " + str(len(diffs)) + " 处差异；" + "；".join(lines),
                version=twin.desired_version,
            )
