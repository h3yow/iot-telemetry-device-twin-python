"""本地端到端演示：注册鉴权 -> 断连重连 -> 乱序补传 -> 期望对账 -> 规则告警。

运行：uv run python examples/demo.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from iot_twin.clock import FakeClock
from iot_twin.models import Principal, Role, TelemetrySample
from iot_twin.rules import Comparator, MetricKind, Rule
from iot_twin.service import IoTService


def show(title: str, result) -> None:
    print(f"\n===== {title} =====")
    print(result)


def main() -> None:
    clock = FakeClock(start_ms=1_700_000_000_000)
    svc = IoTService(clock=clock)
    admin = Principal(device_id=None, role=Role.ADMIN)

    # 1. 注册 + 鉴权
    svc.register_device("sensor-A", "cred-A")
    device = svc.authenticate("sensor-A", "cred-A")
    svc.add_rule(admin, Rule(
        rule_id="high-temp", device_id="sensor-A", metric="temp",
        kind=MetricKind.METRIC, comparator=Comparator.GT, threshold=40.0,
        description="温度高于40度",
    ))

    # 2. 上线，正常上报
    svc.connect(device, "sensor-A")
    r1 = svc.ingest_telemetry(device, "sensor-A", seq=1, ts_ms=1000, metrics={"temp": 30.0})
    r1_dup = svc.ingest_telemetry(device, "sensor-A", seq=1, ts_ms=1000, metrics={"temp": 30.0})
    show("首次上报 / 重复上报", (r1, r1_dup))

    # 3. 离线期间管理员下发期望
    svc.disconnect(device, "sensor-A")
    twin = svc.update_desired(admin, "sensor-A", {"led": "on"})
    show("离线期间下发期望", (twin.sync_status.value, twin.desired.version))

    # 4. 重连 + 乱序补传（seq 故意打乱，含一条旧重复）
    svc.connect(device, "sensor-A")
    batch = [
        TelemetrySample("sensor-A", seq=5, ts_ms=5000, metrics={"temp": 45.0}),
        TelemetrySample("sensor-A", seq=3, ts_ms=3000, metrics={"temp": 35.0}),
        TelemetrySample("sensor-A", seq=2, ts_ms=2000, metrics={"led": "on"}),
        TelemetrySample("sensor-A", seq=4, ts_ms=4000, metrics={"temp": 41.0}),
        TelemetrySample("sensor-A", seq=1, ts_ms=1000, metrics={"temp": 30.0}),
    ]
    results = svc.backfill(device, "sensor-A", batch, ack_desired_version=1)
    show("乱序补传结果（按ts,seq排序后）",
         [(r.seq, r.accepted, r.reject_reason) for r in results])

    # 5. 最终孪生与告警
    twin = svc.get_twin(device, "sensor-A")
    store = svc._store("sensor-A")
    show("最终孪生", {
        "connection": twin.connection.value,
        "sync_status": twin.sync_status.value,
        "desired": twin.desired.values if twin.desired else None,
        "reported": twin.reported.values if twin.reported else None,
        "conflict_detail": svc._twin("sensor-A").conflict_detail(),
    })
    show("最终汇聚", {
        "watermark_ms": store.watermark_ms,
        "last_seq": store.last_seq,
        "metrics": store.latest_metrics(),
    })
    show("活动告警 / 告警历史", (
        [(a.rule_id, a.state.value, a.trigger_value) for a in svc.list_alerts(admin, "sensor-A")],
        [(a.state.value, a.detail) for a in svc.alert_history(admin, "sensor-A")],
    ))

    # 6. 降温恢复 + 离线自动恢复
    svc.ingest_telemetry(device, "sensor-A", seq=6, ts_ms=6000, metrics={"temp": 20.0})
    show("降温后活动告警", svc.list_alerts(admin, "sensor-A"))


if __name__ == "__main__":
    main()
