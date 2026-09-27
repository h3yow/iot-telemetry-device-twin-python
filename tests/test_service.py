"""端到端：断连重连 + 乱序补传 + 并发操作下的可预期、可追溯结论。"""
from __future__ import annotations

import logging
import threading

import pytest

from iot_twin.errors import DomainError, ErrorCode
from iot_twin.models import (
    AlertState,
    ConnectionState,
    Principal,
    Role,
    SyncStatus,
    TelemetrySample,
)
from iot_twin.rules import Comparator, MetricKind, Rule
from iot_twin.service import IoTService

log = logging.getLogger("iot_twin.test.service")


def _sample(seq: int, ts_ms: int, metrics: dict[str, float]) -> TelemetrySample:
    return TelemetrySample(device_id="dev-1", seq=seq, ts_ms=ts_ms, metrics=metrics)


def test_rejection_reasons_are_distinguishable(service: IoTService):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")

    # 未注册设备
    r = service.ingest_telemetry(device, "ghost", 1, 1, {"t": 1})
    log.info("输入: 已鉴权主体上报 ghost 数据; 判定依据: 设备不匹配; 结果=%s", r.reject_reason)
    assert r.reject_reason == "PERMISSION_DENIED"

    with pytest.raises(DomainError) as exc:
        service.authenticate("ghost", "x")
    assert exc.value.code == ErrorCode.DEVICE_NOT_REGISTERED
    with pytest.raises(DomainError) as exc:
        service.authenticate("dev-1", "bad")
    assert exc.value.code == ErrorCode.INVALID_CREDENTIAL

    # 另一台设备越权上报 dev-1
    service.register_device("dev-2", "secret2")
    other = service.authenticate("dev-2", "secret2")
    r2 = service.ingest_telemetry(other, "dev-1", 1, 1, {"t": 1})
    assert r2.reject_reason == "PERMISSION_DENIED"
    log.info("输入: dev-2 上报 dev-1; 判定依据: 跨设备; 最终拒绝原因=%s", r2.reject_reason)


def test_duplicate_report_not_counted_twice(service: IoTService, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    fake_clock.advance(10)

    first = service.ingest_telemetry(device, "dev-1", 1, 100, {"temp": 30})
    duplicate = service.ingest_telemetry(device, "dev-1", 1, 100, {"temp": 30})
    log.info("输入: seq=1 短时间内上报两次; 判定依据: 去重窗口; 结果=%s/%s",
             first.reject_reason, duplicate.reject_reason)
    assert first.accepted is True
    assert duplicate.accepted is False
    assert duplicate.reject_reason == "DUPLICATE_SAMPLE"
    assert len(service._store("dev-1").samples()) == 1
    log.info("最终状态: 汇聚样本数=1")


def test_disconnect_reconnect_and_unordered_backfill(service: IoTService, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)

    # 在线上报一条
    service.connect(device, "dev-1")
    service.ingest_telemetry(device, "dev-1", seq=1, ts_ms=100, metrics={"temp": 30})
    service.disconnect(device, "dev-1")
    log.info("输入: 设备上报 ts=100 后离线; 判定依据: 连接事件; 连接=%s",
             service.get_twin(device, "dev-1").connection.value)

    # 离线期间管理员下发期望
    twin = service.update_desired(admin, "dev-1", {"led": "on"})
    assert twin.sync_status is SyncStatus.PENDING

    # 重连后乱序补传（接收顺序 5,3,2,4,1-重复）
    service.connect(device, "dev-1")
    batch = [
        _sample(5, 150, {"temp": 45}),
        _sample(3, 130, {"temp": 35}),
        _sample(2, 120, {"temp": 32, "led": "on"}),
        _sample(4, 140, {"temp": 40}),
        _sample(1, 100, {"temp": 30}),
    ]
    results = service.backfill(device, "dev-1", batch, ack_desired_version=1)
    statuses = [(r.seq, r.accepted, r.reject_reason) for r in results]
    log.info("输入: 补传 seq=5,3,2,4,1(乱序); 判定依据: (ts,seq)排序+去重+水位; 结果=%s", statuses)

    # seq=1 已在在线时计入 -> DUPLICATE；seq=2..5 按 ts 顺序并入；
    # seq=2 携带 ack v1 且 led=on -> IN_SYNC，其后不带 ack 的普通遥测保持结论不回退
    twin = service.get_twin(device, "dev-1")
    log.info("最终状态: watermark=%s last_seq=%s metrics=%s 同步=%s",
             service._store("dev-1").watermark_ms, service._store("dev-1").last_seq,
             service._store("dev-1").latest_metrics(), twin.sync_status.value)
    assert service._store("dev-1").watermark_ms == 150
    assert service._store("dev-1").last_seq == 5
    assert service._store("dev-1").latest_metrics()["temp"] == 45
    assert twin.sync_status is SyncStatus.IN_SYNC


def test_concurrent_desired_updates_no_lost_update(service: IoTService, fake_clock):
    service.register_device("dev-1", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)
    service.update_desired(admin, "dev-1", {"k": 0})  # v1

    n_threads = 16
    barrier = threading.Barrier(n_threads)
    results: list[str] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        barrier.wait()
        for attempt in range(20):
            current = service._twin("dev-1").state.desired.version
            try:
                service.update_desired(
                    admin, "dev-1", {f"k{i}": i}, expected_version=current, merge=True
                )
                with lock:
                    results.append("ok")
                return
            except DomainError as exc:
                assert exc.code == ErrorCode.VERSION_CONFLICT
        with lock:
            results.append("failed")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    twin = service._twin("dev-1").snapshot()
    log.info("输入: %s 线程并发补丁更新; 判定依据: 每线程重试+乐观锁; 成功=%s 最终版本=v%s",
             n_threads, results.count("ok"), twin.desired.version)
    assert results.count("ok") == n_threads
    # 每次成功更新恰好推进一个版本，没有丢失更新
    assert twin.desired.version == 1 + n_threads
    for i in range(n_threads):
        assert twin.desired.values[f"k{i}"] == i
    log.info("最终期望: v%s 键数=%s", twin.desired.version, len(twin.desired.values))


def test_alert_chain_fire_resolve_and_offline_recovery(service: IoTService, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    admin = Principal(device_id=None, role=Role.ADMIN)
    service.add_rule(
        admin,
        Rule(
            rule_id="hot",
            device_id="dev-1",
            metric="temp",
            kind=MetricKind.METRIC,
            comparator=Comparator.GT,
            threshold=40.0,
            description="温度过高",
        ),
    )
    service.connect(device, "dev-1")
    service.ingest_telemetry(device, "dev-1", 1, 100, {"temp": 42})
    active = service.list_alerts(admin, "dev-1")
    assert len(active) == 1 and active[0].state is AlertState.FIRING

    # 过期乱序样本不得倒退告警
    stale = service.ingest_telemetry(device, "dev-1", 2, 90, {"temp": 10})
    assert stale.reject_reason == "STALE_SAMPLE"
    assert len(service.list_alerts(admin, "dev-1")) == 1
    log.info("输入: ts=90 的降温样本迟到; 判定依据: 低于水位=STALE_SAMPLE; 活动告警保持 FIRING")

    service.ingest_telemetry(device, "dev-1", 3, 200, {"temp": 20})
    assert service.list_alerts(admin, "dev-1") == []
    history = service.alert_history(admin, "dev-1")
    assert [a.state for a in history] == [AlertState.FIRING, AlertState.RESOLVED]

    # 再次超阈 -> FIRING，随后离线自动恢复
    service.ingest_telemetry(device, "dev-1", 4, 300, {"temp": 99})
    service.disconnect(device, "dev-1")
    assert service.list_alerts(admin, "dev-1") == []
    states = [a.state for a in service.alert_history(admin, "dev-1")]
    log.info("输入: 再超阈后离线; 判定依据: 离线自动恢复; 告警历史=%s",
             [s.value for s in states])
    assert states == [AlertState.FIRING, AlertState.RESOLVED, AlertState.FIRING, AlertState.RESOLVED]
