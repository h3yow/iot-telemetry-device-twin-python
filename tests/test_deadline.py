"""确认时限：起算规则、超时结论、迟到确认纠正、与冲突结论的区分。"""
from __future__ import annotations

import logging

import pytest

from iot_twin.models import Principal, Role, SyncStatus
from iot_twin.errors import DomainError, ErrorCode

log = logging.getLogger("iot_twin.test.deadline")


def test_timeout_before_and_after_deadline(service, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)
    service.connect(device, "dev-1")

    service.update_desired(admin, "dev-1", {"led": "on"}, ack_timeout_ms=100)
    log.info("输入: 在线下发 led=on, 确认时限=100ms, 下发时刻=%s", fake_clock.now_ms())

    fake_clock.advance(99)
    twin = service.get_twin(admin, "dev-1")
    log.info("输入: 推进到 %s(时限前1ms); 判定依据: now<deadline; 结论=%s",
             fake_clock.now_ms(), twin.sync_status.value)
    assert twin.sync_status is SyncStatus.PENDING

    fake_clock.advance(1)
    twin = service.get_twin(admin, "dev-1")
    log.info("输入: 推进到 %s(到时限); 判定依据: 仍未确认且仍PENDING; 结论=%s",
             fake_clock.now_ms(), twin.sync_status.value)
    assert twin.sync_status is SyncStatus.ACK_TIMEOUT


def test_late_correct_ack_auto_corrects_timeout(service, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)
    service.connect(device, "dev-1")
    service.update_desired(admin, "dev-1", {"led": "on"}, ack_timeout_ms=100)

    fake_clock.advance(150)
    timed_out = service.get_twin(admin, "dev-1").sync_status
    assert timed_out is SyncStatus.ACK_TIMEOUT
    log.info("输入: 超时未确认; 判定依据: now>=deadline; 结论=%s", timed_out.value)

    # 时限之后设备才把正确确认补上来 -> 结论自动纠正，不一直卡超时
    result = service.ingest_telemetry(
        device, "dev-1", 1, fake_clock.now_ms(), {"led": "on"}, ack_desired_version=1
    )
    status = service.get_twin(admin, "dev-1").sync_status
    log.info("输入: 迟到的正确确认 ack=v1 values=led:on; 判定依据: %s; 最终结论=%s",
             result.detail, status.value)
    assert status is SyncStatus.IN_SYNC


def test_late_wrong_ack_becomes_conflict_not_timeout(service, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)
    service.connect(device, "dev-1")
    service.update_desired(admin, "dev-1", {"led": "on"}, ack_timeout_ms=100)
    fake_clock.advance(150)
    assert service.get_twin(admin, "dev-1").sync_status is SyncStatus.ACK_TIMEOUT

    # 超时后补上来的是错误取值 -> 键值冲突，结论必须能与"超时没确认"区分
    service.ingest_telemetry(
        device, "dev-1", 1, fake_clock.now_ms(), {"led": "off"}, ack_desired_version=1
    )
    twin = service.get_twin(admin, "dev-1")
    log.info("输入: 超时后上报 led=off(ack v1); 判定依据: 版本一致但键值不一致; 结论=%s 明细=%s",
             twin.sync_status.value, service._twin("dev-1").conflict_detail())
    assert twin.sync_status is SyncStatus.CONFLICTED
    assert "led" in service._twin("dev-1").conflict_detail()


def test_stale_version_ack_is_conflict_even_after_deadline(service, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)
    service.connect(device, "dev-1")
    service.update_desired(admin, "dev-1", {"led": "on"}, ack_timeout_ms=100)  # v1
    service.update_desired(
        admin, "dev-1", {"led": "blink"}, expected_version=1, ack_timeout_ms=100
    )  # v2
    fake_clock.advance(200)
    # 设备按老版本 v1 确认 -> 必须判 CONFLICTED，而不是超时
    service.ingest_telemetry(
        device, "dev-1", 1, fake_clock.now_ms(), {"led": "on"}, ack_desired_version=1
    )
    status = service.get_twin(admin, "dev-1").sync_status
    log.info("输入: 超时后设备仅 ack v1(当前 v2); 判定依据: ack<当前版本=按旧期望执行; 结论=%s",
             status.value)
    assert status is SyncStatus.CONFLICTED


def test_deadline_starts_from_reconnect_when_issued_offline(service, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)
    service.connect(device, "dev-1")
    service.disconnect(device, "dev-1")
    log.info("输入: 设备离线后(时刻=%s)下发期望, 时限=100ms", fake_clock.now_ms())

    # 离线很久之后下发——时限绝不能从下发时刻起算
    fake_clock.advance(10_000)
    service.update_desired(admin, "dev-1", {"led": "on"}, ack_timeout_ms=100)
    assert service.get_twin(admin, "dev-1").sync_status is SyncStatus.PENDING

    service.connect(device, "dev-1")  # 重连时刻才开始计时
    reconnect_at = fake_clock.now_ms()
    log.info("输入: 设备重连时刻=%s; 判定依据: deadline=重连时刻+100", reconnect_at)
    assert service._twin("dev-1").ack_deadline() == reconnect_at + 100

    # 刚上线时不能直接被判超时
    assert service.get_twin(admin, "dev-1").sync_status is SyncStatus.PENDING
    fake_clock.advance(100)
    assert service.get_twin(admin, "dev-1").sync_status is SyncStatus.ACK_TIMEOUT

    # 重连后时限内确认 -> IN_SYNC
    service2 = service  # 同一台设备重新走一遍会有重复 seq，用新设备隔离更直观


def test_offline_issue_then_reconnect_ack_in_time(service, fake_clock):
    service.register_device("dev-2", "secret")
    device = service.authenticate("dev-2", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)
    service.connect(device, "dev-2")
    service.disconnect(device, "dev-2")
    fake_clock.advance(5_000)
    service.update_desired(admin, "dev-2", {"led": "on"}, ack_timeout_ms=100)
    service.connect(device, "dev-2")
    fake_clock.advance(99)
    service.ingest_telemetry(
        device, "dev-2", 1, fake_clock.now_ms(), {"led": "on"}, ack_desired_version=1
    )
    status = service.get_twin(admin, "dev-2").sync_status
    log.info("输入: 离线下发, 重连后99ms内确认; 判定依据: 时限从重连起算且确认正确; 结论=%s",
             status.value)
    assert status is SyncStatus.IN_SYNC


def test_no_deadline_means_never_timeout(service, fake_clock):
    service.register_device("dev-3", "secret")
    device = service.authenticate("dev-3", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)
    service.connect(device, "dev-3")
    # 兼容旧行为：不带 ack_timeout_ms 的下发永远不会被判超时
    service.update_desired(admin, "dev-3", {"led": "on"})
    fake_clock.advance(10_000_000)
    status = service.get_twin(admin, "dev-3").sync_status
    log.info("输入: 未设定时限的下发经过极长时间; 判定依据: deadline=None; 结论=%s", status.value)
    assert status is SyncStatus.PENDING

    with pytest.raises(DomainError) as exc:
        service.update_desired(admin, "dev-3", {"led": "x"}, ack_timeout_ms=0)
    assert exc.value.code == ErrorCode.BAD_TELEMETRY


def test_timeout_resets_on_reconnect_with_fresh_window(service, fake_clock):
    service.register_device("dev-4", "secret")
    device = service.authenticate("dev-4", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)
    service.connect(device, "dev-4")
    service.update_desired(admin, "dev-4", {"led": "on"}, ack_timeout_ms=100)
    fake_clock.advance(150)
    assert service.get_twin(admin, "dev-4").sync_status is SyncStatus.ACK_TIMEOUT
    log.info("输入: 首次在线超时; 判定依据: now>=deadline; 结论=ACK_TIMEOUT")

    # 断线后重连：旧超时结论复位，时限从重新上线那一刻起算
    service.disconnect(device, "dev-4")
    fake_clock.advance(5_000)
    twin = service.connect(device, "dev-4")
    reconnect_at = fake_clock.now_ms()
    log.info("输入: 重连时刻=%s; 判定依据: 新确认窗口重置超时并重算时限", reconnect_at)
    assert twin.sync_status is SyncStatus.PENDING
    assert service._twin("dev-4").ack_deadline() == reconnect_at + 100

    fake_clock.advance(100)
    assert service.get_twin(admin, "dev-4").sync_status is SyncStatus.ACK_TIMEOUT
    # 再重连一次，窗口内确认成功
    service.disconnect(device, "dev-4")
    service.connect(device, "dev-4")
    service.ingest_telemetry(
        device, "dev-4", 1, fake_clock.now_ms(), {"led": "on"}, ack_desired_version=1
    )
    assert service.get_twin(admin, "dev-4").sync_status is SyncStatus.IN_SYNC
    log.info("输入: 再次重连后时限内正确确认; 最终结论=IN_SYNC")
