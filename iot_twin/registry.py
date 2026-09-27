"""设备注册与凭证鉴权。

失败原因通过 :class:`DomainError` 的 ``code`` 区分：

* ``DEVICE_NOT_REGISTERED``：设备未注册；
* ``INVALID_CREDENTIAL``：凭证无效；
* ``PERMISSION_DENIED``：已鉴权身份无权操作目标设备；
* ``DEVICE_ALREADY_REGISTERED``：重复注册。
"""
from __future__ import annotations

import threading

from .errors import DomainError, ErrorCode
from .models import Device, Principal, Role


class DeviceRegistry:
    def __init__(self) -> None:
        self._devices: dict[str, Device] = {}
        self._lock = threading.RLock()

    def register(self, device_id: str, credential: str, now_ms: int = 0) -> Device:
        if not device_id or not isinstance(device_id, str):
            raise DomainError(ErrorCode.BAD_TELEMETRY, "device_id 不能为空")
        if not credential:
            raise DomainError(ErrorCode.BAD_TELEMETRY, "credential 不能为空")
        with self._lock:
            if device_id in self._devices:
                raise DomainError(
                    ErrorCode.DEVICE_ALREADY_REGISTERED,
                    f"设备 {device_id} 已注册",
                )
            device = Device(
                device_id=device_id,
                credential=credential,
                registered_at_ms=now_ms,
            )
            self._devices[device_id] = device
            return device

    def get(self, device_id: str) -> Device:
        with self._lock:
            device = self._devices.get(device_id)
            if device is None:
                raise DomainError(
                    ErrorCode.DEVICE_NOT_REGISTERED,
                    f"设备 {device_id} 未注册",
                )
            return device

    def is_registered(self, device_id: str) -> bool:
        with self._lock:
            return device_id in self._devices

    def authenticate(self, device_id: str, credential: str) -> Principal:
        """校验设备凭证，返回该请求的身份主体。

        顺序刻意为：先判定注册状态，再判定凭证，便于区分失败原因。
        """
        device = self.get(device_id)
        if credential != device.credential:
            raise DomainError(
                ErrorCode.INVALID_CREDENTIAL,
                f"设备 {device_id} 凭证无效",
            )
        return Principal(device_id=device_id, role=Role.DEVICE)

    def authorize(
        self,
        principal: Principal,
        target_device_id: str,
        *,
        device_only: bool = False,
    ) -> None:
        """校验身份是否有权操作目标设备。

        :param device_only: 为 True 时仅允许设备自身（遥测上报场景），
            管理员身份也会被拒绝。
        """
        if principal.role is Role.ADMIN:
            if device_only:
                raise DomainError(
                    ErrorCode.PERMISSION_DENIED,
                    "管理员身份不允许以设备名义上报遥测",
                )
            return
        if principal.role is Role.DEVICE and principal.device_id == target_device_id:
            return
        raise DomainError(
            ErrorCode.PERMISSION_DENIED,
            f"主体 {principal.device_id} 无权操作设备 {target_device_id}",
        )
