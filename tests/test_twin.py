"""孪生一致性测试：离线下发、重连生效、冲突判定、并发更新。"""
from __future__ import annotations

import logging
import threading

import pytest

from iot_twin.errors import DomainError, ErrorCode
from iot_twin.models import ConnectionState, SyncStatus
from iot_twin.twin import DeviceTwin

log = logging.getLogger("iot_twin.test.twin")


def test_reported_watermark_never_regresses():
    twin = DeviceTwin("dev-1")
    accepted, status, reason = twin.apply_reported({"power": 1}, seq=1, ts_ms=100, now_ms=100)
    assert accepted
    stale, _, reason = twin.apply_reported({"power": 0}, seq=0, ts_ms=90, now_ms=101)
    log.info("输入: ts=100 后到 ts=90; 判定依据: %s; 结果(计入)=%s", reason, stale)
    assert stale is False
    assert twin.state.reported.values == {"power": 1}
    log.info("最终状态: %s", twin.state.reported.values)


def test_desired_issued_offline_takes_effect_after_reconnect():
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.disconnect(now_ms=10)
    # 离线期间下发期望
    twin.update_desired({"led": "on"}, expected_version=None, now_ms=20)
    log.info("输入: 离线下发 desired={led:on}; 判定依据: 设备离线; 状态=%s", twin.state.sync_status)
    assert twin.state.sync_status is SyncStatus.PENDING
    assert twin.state.connection is ConnectionState.OFFLINE

    # 重连，设备确认并执行成功
    twin.connect(now_ms=30)
    accepted, status, _ = twin.apply_reported(
        {"led": "on"}, seq=1, ts_ms=40, now_ms=40, ack_desired_version=1
    )
    assert accepted
    assert status is SyncStatus.IN_SYNC
    log.info("输入: 重连后上报 led=on(ack v1); 判定依据: 版本一致且键值一致; 最终状态=%s", status)


def test_conflict_is_deterministic_and_explainable():
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.update_desired({"led": "on"}, expected_version=None, now_ms=5)
    # 设备执行成 off -> 键值冲突
    twin.apply_reported({"led": "off"}, seq=1, ts_ms=10, now_ms=10, ack_desired_version=1)
    assert twin.state.sync_status is SyncStatus.CONFLICTED
    detail = twin.conflict_detail()
    log.info("输入: 上报 led=off 对照期望 led=on; 判定依据: %s", detail)
    assert "led" in detail

    # 设备基于旧版本执行 -> 版本冲突
    twin.update_desired({"led": "blink"}, expected_version=1, now_ms=15)
    twin.apply_reported({"led": "on"}, seq=2, ts_ms=20, now_ms=20, ack_desired_version=1)
    assert twin.state.sync_status is SyncStatus.CONFLICTED
    log.info("输入: 设备 ack v1 但期望已到 v2; 判定依据: %s", twin.conflict_detail())


def test_optimistic_concurrency_rejects_lost_update():
    twin = DeviceTwin("dev-1")
    twin.update_desired({"a": 1}, expected_version=None, now_ms=0)
    # 两个并发方都基于 v1 更新，只有一个能成功
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def worker(value: int) -> None:
        barrier.wait()
        try:
            twin.update_desired({"a": value}, expected_version=1, now_ms=1)
            outcomes.append("ok")
        except DomainError as exc:
            outcomes.append(exc.code)

    t1 = threading.Thread(target=worker, args=(2,))
    t2 = threading.Thread(target=worker, args=(3,))
    t1.start(); t2.start(); t1.join(); t2.join()
    log.info("输入: 两个并发更新均期望 v1; 判定依据: 乐观锁; 结果=%s", sorted(outcomes))
    assert sorted(outcomes) == [ErrorCode.VERSION_CONFLICT, "ok"]
    assert twin.state.desired.version == 2
    log.info("最终期望版本: v%s values=%s", twin.state.desired.version, twin.state.desired.values)


def test_merge_vs_replace_and_version_progression():
    twin = DeviceTwin("dev-1")
    twin.update_desired({"a": 1, "b": 1}, expected_version=None, now_ms=0)
    twin.update_desired({"b": 2}, expected_version=1, now_ms=1, merge=True)
    assert twin.state.desired.values == {"a": 1, "b": 2}
    assert twin.state.desired.version == 2
    twin.update_desired({"c": 3}, expected_version=2, now_ms=2, merge=False)
    assert twin.state.desired.values == {"c": 3}
    assert twin.state.desired.version == 3
    log.info("输入: 补丁后整体替换; 最终状态: v%s %s",
             twin.state.desired.version, twin.state.desired.values)

    with pytest.raises(DomainError) as exc:
        twin.update_desired({"x": 0}, expected_version=1, now_ms=3)
    assert exc.value.code == ErrorCode.VERSION_CONFLICT
