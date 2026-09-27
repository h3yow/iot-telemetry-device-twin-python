"""设备注册与鉴权。

约定：
- 设备以 (device_id, token) 注册，token 为预共享密钥；
- 鉴权结果用 AuthResult 区分：未注册 / 凭证无效 / 成功；
- 权限以 scope 集合表达，例如 "telemetry:write"、"twin:write"。
"""
from __future__ import annotations

import hmac
import threading
from dataclasses import dataclass
from enum import Enum

from iot_service.models import Device


class AuthFailure(str, Enum):
    DEVICE_NOT_REGISTERED = "DEVICE_NOT_REGISTERED"
    INVALID_CREDENTIALS = "INVALID_CREDENTIALS"


@dataclass
class AuthResult:
    device: Device | None = None
    failure: AuthFailure | None = None

    @property
    def ok(self) -> bool:
        return self.device is not None


class DeviceRegistry:
    def __init__(self) -> None:
        self._devices: dict[str, Device] = {}
        self._lock = threading.Lock()

    def register(self, device_id: str, token: str, scopes: set[str] | None = None) -> Device:
        if not device_id or not token:
            raise ValueError("device_id 与 token 均不能为空")
        with self._lock:
            device = Device(
                device_id=device_id,
                token=token,
                scopes=frozenset(scopes) if scopes else frozenset({"telemetry:write"}),
            )
            self._devices[device_id] = device
            return device

    def authenticate(self, device_id: str, token: str) -> AuthResult:
        with self._lock:
            device = self._devices.get(device_id)
        if device is None:
            return AuthResult(failure=AuthFailure.DEVICE_NOT_REGISTERED)
        if not hmac.compare_digest(device.token, token or ""):
            return AuthResult(failure=AuthFailure.INVALID_CREDENTIALS)
        return AuthResult(device=device)

    def get(self, device_id: str) -> Device | None:
        with self._lock:
            return self._devices.get(device_id)
