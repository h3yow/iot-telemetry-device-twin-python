"""注册与鉴权测试：各类失败原因必须可区分。"""
from __future__ import annotations

import logging

import pytest

from iot_twin.errors import DomainError, ErrorCode
from iot_twin.models import Principal, Role
from iot_twin.registry import DeviceRegistry

log = logging.getLogger("iot_twin.test.registry")


def _expect_code(exc_info: pytest.ExceptionInfo[DomainError], code: str) -> None:
    assert exc_info.value.code == code
    log.info("拒绝原因=%s 详情=%s", code, exc_info.value.message)


def test_register_and_authenticate_success():
    reg = DeviceRegistry()
    device = reg.register("dev-1", "secret", now_ms=100)
    log.info("输入: register dev-1; 判定依据: 首次注册; 最终状态: %s", device)
    assert device.registered_at_ms == 100

    principal = reg.authenticate("dev-1", "secret")
    assert principal.role is Role.DEVICE
    assert principal.device_id == "dev-1"
    log.info("输入: 正确凭证; 判定依据: 凭证匹配; 最终身份: %s", principal)


def test_duplicate_registration_rejected():
    reg = DeviceRegistry()
    reg.register("dev-1", "secret")
    with pytest.raises(DomainError) as exc:
        reg.register("dev-1", "other")
    _expect_code(exc, ErrorCode.DEVICE_ALREADY_REGISTERED)


def test_unregistered_invalid_credential_and_permission_are_distinguishable():
    reg = DeviceRegistry()
    reg.register("dev-1", "secret")
    reg.register("dev-2", "secret-2")

    # 1) 未注册
    with pytest.raises(DomainError) as exc:
        reg.authenticate("ghost", "x")
    _expect_code(exc, ErrorCode.DEVICE_NOT_REGISTERED)

    # 2) 凭证无效
    with pytest.raises(DomainError) as exc:
        reg.authenticate("dev-1", "wrong")
    _expect_code(exc, ErrorCode.INVALID_CREDENTIAL)

    # 3) 权限不足：设备 A 操作设备 B
    principal_a = reg.authenticate("dev-1", "secret")
    with pytest.raises(DomainError) as exc:
        reg.authorize(principal_a, "dev-2")
    _expect_code(exc, ErrorCode.PERMISSION_DENIED)

    # 设备自身有权
    reg.authorize(principal_a, "dev-1")

    # 管理员可管理任意设备，但不能以设备名义上报
    admin = Principal(device_id=None, role=Role.ADMIN)
    reg.authorize(admin, "dev-1")
    with pytest.raises(DomainError) as exc:
        reg.authorize(admin, "dev-1", device_only=True)
    _expect_code(exc, ErrorCode.PERMISSION_DENIED)
