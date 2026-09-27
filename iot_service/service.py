"""服务门面：编排注册鉴权、遥测接入、孪生同步与规则告警。

数据流：
- connect/disconnect 维护连接状态并驱动连接类规则与重连孪生校验；
- ingest 依次执行：鉴权 -> 权限校验 -> 去重/乱序合并 -> 孪生 reported 更新 -> 规则评估；
- 所有失败路径都返回可区分的 RejectReason，不抛异常给调用方。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

from iot_service.models import (
    Alert,
    ConnectionState,
    Device,
    IngestResult,
    RejectReason,
    TelemetryMessage,
    TwinState,
    TwinUpdateResult,
)
from iot_service.registry import AuthFailure, DeviceRegistry
from iot_service.rules import RuleEngine
from iot_service.telemetry import TelemetryStore
from iot_service.twin import TwinManager

_AUTH_FAILURE_MAP = {
    AuthFailure.DEVICE_NOT_REGISTERED: RejectReason.DEVICE_NOT_REGISTERED,
    AuthFailure.INVALID_CREDENTIALS: RejectReason.INVALID_CREDENTIALS,
}


@dataclass
class ConnectResult:
    ok: bool
    reason: RejectReason | None = None
    sync: TwinUpdateResult | None = None
    alerts: list[Alert] = field(default_factory=list)


class IoTService:
    def __init__(self, dedup_window_sec: float = 300.0) -> None:
        self.registry = DeviceRegistry()
        self.telemetry = TelemetryStore(dedup_window_sec=dedup_window_sec)
        self.twin = TwinManager()
        self.rules = RuleEngine()
        self._lock = threading.Lock()

    # ---- 设备生命周期 ----
    def register_device(self, device_id: str, token: str, scopes: set[str] | None = None) -> Device:
        return self.registry.register(device_id, token, scopes)

    def connect(self, device_id: str, token: str) -> ConnectResult:
        auth = self.registry.authenticate(device_id, token)
        if not auth.ok:
            return ConnectResult(ok=False, reason=_AUTH_FAILURE_MAP[auth.failure])
        with self._lock:
            auth.device.state = ConnectionState.ONLINE
            alerts = self.rules.evaluate_connection(device_id, ConnectionState.ONLINE)
            sync = self.twin.sync_on_reconnect(device_id)
        return ConnectResult(ok=True, sync=sync, alerts=alerts)

    def disconnect(self, device_id: str) -> list[Alert]:
        device = self.registry.get(device_id)
        with self._lock:
            if device is not None:
                device.state = ConnectionState.OFFLINE
            return self.rules.evaluate_connection(device_id, ConnectionState.OFFLINE)

    # ---- 遥测接入 ----
    def ingest(self, device_id: str, token: str, msg: TelemetryMessage) -> IngestResult:
        auth = self.registry.authenticate(device_id, token)
        if not auth.ok:
            return IngestResult(accepted=False, reason=_AUTH_FAILURE_MAP[auth.failure])
        if "telemetry:write" not in auth.device.scopes:
            return IngestResult(accepted=False, reason=RejectReason.PERMISSION_DENIED)
        msg.device_id = device_id  # 以认证身份为准，防止伪造
        with self._lock:
            result = self.telemetry.ingest(msg)
            if result.accepted:
                self.twin.apply_reported(device_id, msg.payload, msg.seq)
                result.alerts = self.rules.evaluate_telemetry(device_id, msg.payload, msg.seq)
        return result

    # ---- 孪生 ----
    def set_desired(self, device_id: str, patch: dict[str, Any], base_version: int) -> TwinUpdateResult:
        return self.twin.update_desired(device_id, patch, base_version)

    def get_twin(self, device_id: str) -> TwinState:
        return self.twin.get(device_id)
