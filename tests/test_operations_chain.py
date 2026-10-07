"""端到端运营链路：下发—确认—收尾（超时/纠正/撤销/批量）经门面服务串联。"""
from __future__ import annotations

import logging

import pytest

from iot_twin.errors import DomainError, ErrorCode
from iot_twin.models import Principal, Role, SyncStatus

log = logging.getLogger("iot_twin.test.operations")


@pytest.fixture
def admin() -> Principal:
    return Principal(device_id=None, role=Role.ADMIN)


def test_service_offline_dispatch_then_reconnect_timeout_and_late_ack(
    service, admin, fake_clock
):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")

    # 离线期间下发（设备此前从未上线）
    twin = service.update_desired(admin, "dev-1", {"led": "on"}, ack_timeout_ms=100)
    assert twin.sync_status is SyncStatus.PENDING
    assert twin.desired.deadline_ms is None
    log.info("输入: 离线下发时限100ms; 判定依据: deadline 未激活; 连接=%s deadline=%s",
             twin.connection.value, twin.desired.deadline_ms)

    # 时钟走过远超时限也不应超时（设备离线）
    fake_clock.advance(500)
    assert service.get_twin(admin, "dev-1").sync_status is SyncStatus.PENDING

    # 上线：时限从此刻起算
    service.connect(device, "dev-1")
    base = fake_clock.now_ms()
    assert service.get_twin(admin, "dev-1").desired.deadline_ms == base + 100

    fake_clock.advance(100)
    twin = service.get_twin(admin, "dev-1")
    log.info("输入: 上线后越过时限仍无确认; 判定依据: %s",
             service._twin("dev-1").timeout_detail())
    assert twin.sync_status is SyncStatus.ACK_TIMEOUT

    # 迟到的正确确认 -> 自动纠正
    fake_clock.advance(5)
    result = service.ingest_telemetry(
        device, "dev-1", 1, fake_clock.now_ms(), {"led": "on"},
        ack_desired_version=twin.desired.version,
    )
    assert result.accepted
    final = service.get_twin(admin, "dev-1")
    log.info("输入: 迟到正确确认; 最终结论=%s", final.sync_status.value)
    assert final.sync_status is SyncStatus.IN_SYNC


def test_service_revoke_round_trip_and_device_role_rejected(service, admin, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    service.connect(device, "dev-1")

    service.update_desired(admin, "dev-1", {"led": "on"})
    service.ingest_telemetry(
        device, "dev-1", 1, fake_clock.now_ms(), {"led": "on"}, ack_desired_version=1
    )
    service.update_desired(admin, "dev-1", {"led": "blink"}, expected_version=1)

    # 设备身份不能撤销
    with pytest.raises(DomainError) as exc:
        service.revoke_desired(device, "dev-1")
    assert exc.value.code == ErrorCode.PERMISSION_DENIED

    # 管理员撤销未确认的 v2 -> 回到 v1 的 on，版本前进到 v3
    revoked = service.revoke_desired(admin, "dev-1")
    log.info("输入: 管理员撤销未确认 v2; 最终版本=v%s values=%s revoke_of=%s",
             revoked.desired.version, revoked.desired.values,
             revoked.desired.revoke_of_version)
    assert revoked.desired.values == {"led": "on"}
    assert revoked.desired.version == 3
    assert revoked.desired.revoke_of_version == 2

    # 重复撤销不多出版本
    again = service.revoke_desired(admin, "dev-1")
    assert again.desired.version == 3

    # 撤销已被确认的期望应被拒绝：先让设备确认撤销版 v3
    service.ingest_telemetry(
        device, "dev-1", 2, fake_clock.now_ms(), {"led": "on"}, ack_desired_version=3
    )
    with pytest.raises(DomainError) as exc:
        service.revoke_desired(admin, "dev-1")
    assert exc.value.code == ErrorCode.ALREADY_CONFIRMED
    log.info("输入: 撤销已确认的 v3; 判定依据: 已生效需重新下发; 错误=%s", exc.value.code)


def test_single_dispatch_without_timeout_keeps_legacy_behaviour(service, admin, fake_clock):
    """兼容：不传确认时限的单台下发永远不会出现 ACK_TIMEOUT。"""
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    service.connect(device, "dev-1")
    service.update_desired(admin, "dev-1", {"led": "on"})  # 无 ack_timeout_ms
    fake_clock.advance(10_000_000)
    twin = service.get_twin(admin, "dev-1")
    assert twin.sync_status is SyncStatus.PENDING
    assert twin.desired.deadline_ms is None
    log.info("输入: 未设时限且长期不确认; 判定依据: 无 deadline; 结论=%s(兼容旧行为)",
             twin.sync_status.value)
