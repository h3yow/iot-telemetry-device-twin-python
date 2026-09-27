"""遥测汇聚测试：乱序并入、补传防倒退、latest-wins。"""
from __future__ import annotations

import logging

from iot_twin.models import TelemetrySample
from iot_twin.telemetry import TelemetryStore

log = logging.getLogger("iot_twin.test.telemetry")


def _sample(seq: int, ts_ms: int, **metrics: float) -> TelemetrySample:
    return TelemetrySample(
        device_id="dev-1", seq=seq, ts_ms=ts_ms, metrics=metrics, received_at_ms=ts_ms
    )


def test_out_of_order_merge_keeps_latest_metrics():
    store = TelemetryStore()
    # 乱序到达：ts=10, ts=30, ts=20
    assert store.add(_sample(1, 10, temp=10.0)) is True
    assert store.add(_sample(3, 30, temp=30.0, hum=50.0)) is True
    accepted_stale = store.add(_sample(2, 20, temp=20.0))
    log.info(
        "输入: ts=10,30 后到达 ts=20; 判定依据: 20 低于水位 30; 结果(计入)=%s",
        accepted_stale,
    )
    assert accepted_stale is False
    assert store.watermark_ms == 30
    assert store.latest_metrics() == {"temp": 30.0, "hum": 50.0}
    log.info("最终状态: watermark=30 metrics=%s", store.latest_metrics())


def test_backfill_after_reconnect_merges_in_order_without_regression():
    store = TelemetryStore()
    # 断线前最后已知 ts=100, temp=100
    store.add(_sample(5, 100, temp=100.0))
    # 重连后补传离线期间的乱序批量数据（服务端接收顺序乱）
    batch = [
        _sample(9, 140, temp=140.0),
        _sample(7, 120, temp=120.0, hum=40.0),
        _sample(8, 130, temp=130.0),
        _sample(6, 110, temp=110.0),
        _sample(5, 100, temp=100.0),  # 与水位重复
    ]
    results = store.add_sorted(batch)
    log.info(
        "输入: 补传 seq=9,7,8,6,5(乱序); 判定依据: 按(ts,seq)排序并入; 结果=%s",
        results,
    )
    assert results == [False, True, True, True, True]
    assert store.watermark_ms == 140
    assert store.last_seq == 9
    assert store.latest_metrics() == {"temp": 140.0, "hum": 40.0}
    log.info("最终状态: watermark=140 last_seq=9 metrics=%s", store.latest_metrics())


def test_late_duplicate_at_same_timestamp_compared_by_seq():
    store = TelemetryStore()
    assert store.add(_sample(2, 50, temp=1.0)) is True
    assert store.add(_sample(2, 50, temp=2.0)) is False  # 同 ts 同 seq
    assert store.add(_sample(3, 50, temp=3.0)) is True   # 同 ts seq 更新
    assert store.latest_metrics() == {"temp": 3.0}
    log.info("输入: 同ts不同seq; 判定依据: ts相同比较seq; 最终状态 metrics=%s", store.latest_metrics())
