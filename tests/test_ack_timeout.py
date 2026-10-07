"""确认时限：在线/离线下发的超时判定与迟到确认自动纠正。"""
from __future__ import annotations

import logging

from iot_twin.models import SyncStatus
from iot_twin.twin import DeviceTwin

log = logging.getLogger("iot_twin.test.ack_timeout")


def test_online_dispatch_times_out_then_late_correct_ack_self_corrects():
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.update_desired(
        {"led": "on"}, expected_version=None, now_ms=0, ack_timeout_ms=100
    )

    twin.evaluate_timeout(now_ms=99)
    log.info("输入: deadline=100, 当前 t=99; 判定依据: now<deadline; 结论=%s",
             twin.state.sync_status.value)
    assert twin.state.sync_status is SyncStatus.PENDING

    twin.evaluate_timeout(now_ms=100)
    log.info("输入: t=100 仍无确认; 判定依据: %s; 结论=%s",
             twin.timeout_detail(), twin.state.sync_status.value)
    assert twin.state.sync_status is SyncStatus.ACK_TIMEOUT

    # 超时结论必须与"设备按旧版本执行""键值对不上"可区分
    assert twin.conflict_detail() == ""
    assert twin.state.desired.confirmed is False

    # 时限过后设备补来正确确认 -> 自动纠正，不能一直卡在超时
    accepted, status, reason = twin.apply_reported(
        {"led": "on"}, seq=1, ts_ms=105, now_ms=105, ack_desired_version=1
    )
    log.info("输入: t=105 迟到的正确确认 ack=v1; 判定依据: %s; 最终结论=%s",
             reason, status.value)
    assert accepted is True
    assert status is SyncStatus.IN_SYNC
    assert twin.state.desired.confirmed is True


def test_offline_dispatch_deadline_starts_at_reconnect():
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.disconnect(now_ms=10)

    # 离线期间 t=20 下发，时限 100ms：此刻不应激活 deadline
    twin.update_desired(
        {"led": "on"}, expected_version=None, now_ms=20, ack_timeout_ms=100
    )
    log.info("输入: 离线 t=20 下发时限100ms; 判定依据: 设备离线, 时限暂不激活; deadline=%s",
             twin.state.desired.deadline_ms)
    assert twin.state.desired.deadline_ms is None
    assert twin.state.sync_status is SyncStatus.PENDING

    # 即使时钟已经远超"下发时刻+时限"，离线期间也不能判超时
    twin.evaluate_timeout(now_ms=9999)
    assert twin.state.sync_status is SyncStatus.PENDING

    # 重连 t=5000：时限从 5000 开始算 -> deadline=5100
    twin.connect(now_ms=5000)
    log.info("输入: t=5000 重连; 判定依据: 时限从上线时刻起算; deadline=%s",
             twin.state.desired.deadline_ms)
    assert twin.state.desired.deadline_ms == 5100
    assert twin.state.sync_status is SyncStatus.PENDING

    twin.evaluate_timeout(now_ms=5099)
    assert twin.state.sync_status is SyncStatus.PENDING
    twin.evaluate_timeout(now_ms=5100)
    assert twin.state.sync_status is SyncStatus.ACK_TIMEOUT
    log.info("输入: t=5100; 判定依据: 越过重连起算的时限; 最终结论=%s",
             twin.state.sync_status.value)


def test_timeout_distinct_from_stale_version_and_value_conflict():
    """超时、旧版本执行、键值不一致三种结论必须两两可区分。"""
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.update_desired({"led": "on"}, None, 0, ack_timeout_ms=50)
    twin.evaluate_timeout(50)
    assert twin.state.sync_status is SyncStatus.ACK_TIMEOUT
    assert twin.timeout_detail() != "" and twin.conflict_detail() == ""

    # 迟到但键值对不上 -> CONFLICTED（不是超时，也不是同步）
    _, status, _ = twin.apply_reported(
        {"led": "off"}, seq=1, ts_ms=60, now_ms=60, ack_desired_version=1
    )
    log.info("输入: 迟到确认但 led=off; 判定依据: 键值不一致; 结论=%s 冲突=%s",
             status.value, twin.conflict_detail())
    assert status is SyncStatus.CONFLICTED
    assert twin.timeout_detail() == ""
