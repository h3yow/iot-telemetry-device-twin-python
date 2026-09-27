"""乱序补传、离线批量补传与重连恢复测试。"""
from iot_service.models import AlertStatus, ConnectionState, RejectReason, TelemetryMessage

from conftest import log_case


def _msg(device: str, seq: int, temp: float) -> TelemetryMessage:
    return TelemetryMessage(device_id=device, payload={"temp": temp},
                            message_id=f"{device}-m{seq}", seq=seq)


def test_out_of_order_backfill_no_regression(service):
    service.register_device("d1", "tok")
    service.connect("d1", "tok")
    r5 = service.ingest("d1", "tok", _msg("d1", 5, 50.0))
    r3 = service.ingest("d1", "tok", _msg("d1", 3, 30.0))  # 迟到的旧数据
    latest = service.telemetry.latest("d1")
    log_case("乱序补传不倒退", {"到达顺序": [5, 3]}, "seq<=已应用序号则丢弃",
             {"seq5": r5.accepted, "seq3_reason": r3.reason, "latest": latest})
    assert r5.accepted
    assert not r3.accepted and r3.reason == RejectReason.STALE_MESSAGE
    assert latest["temp"] == 50.0  # 状态未倒退


def test_offline_batch_backfill_merged_in_order(service):
    service.register_device("d1", "tok")
    service.connect("d1", "tok")
    service.ingest("d1", "tok", _msg("d1", 1, 10.0))
    service.disconnect("d1")
    # 离线期间设备缓存了 seq 2,3,4，重连后乱序补传
    service.connect("d1", "tok")
    results = [service.ingest("d1", "tok", _msg("d1", s, s * 10.0)) for s in (3, 2, 4)]
    latest = service.telemetry.latest("d1")
    log_case("离线批量补传并入", {"补传顺序": [3, 2, 4]},
             "按 seq 并入，乱序过期条目安全丢弃",
             {"accepted": [r.accepted for r in results],
              "reasons": [str(r.reason) for r in results], "latest": latest})
    assert [r.accepted for r in results] == [True, False, True]
    assert results[1].reason == RejectReason.STALE_MESSAGE
    assert latest["temp"] == 40.0 and service.telemetry.last_seq("d1") == 4


def test_reconnect_recovers_offline_alert(service):
    service.register_device("d1", "tok")
    service.rules.add_connection_rule("rule-offline", "设备离线")
    service.connect("d1", "tok")
    service.disconnect("d1")
    offline_snapshot = [(a.rule_id, a.status) for a in service.rules.active_alerts("d1")]
    service.connect("d1", "tok")  # 重连
    active_after = service.rules.active_alerts("d1")
    log_case("重连告警恢复", {"事件": ["离线", "重连"]}, "连接规则：OFFLINE 触发，ONLINE 恢复",
             {"离线时告警": [(r, str(s)) for r, s in offline_snapshot],
              "重连后活跃告警": active_after})
    assert offline_snapshot == [("rule-offline", AlertStatus.ACTIVE)]
    assert active_after == []


def test_reconnect_applies_pending_desired(service):
    service.register_device("d1", "tok")
    service.connect("d1", "tok")
    service.disconnect("d1")
    # 离线期间下发期望状态
    twin = service.get_twin("d1")
    service.set_desired("d1", {"mode": "eco"}, twin.desired_version)
    conn = service.connect("d1", "tok")
    # 设备重连后上报了匹配值 -> 期望生效
    service.ingest("d1", "tok", TelemetryMessage(device_id="d1", payload={"mode": "eco"},
                                                 message_id="d1-sync", seq=1))
    sync = service.twin.sync_on_reconnect("d1")
    log_case("离线期望状态重连后生效", {"desired": {"mode": "eco"}, "设备重连后上报": {"mode": "eco"}},
             "重连校验 desired==reported 则生效",
             {"首次sync_conflict": conn.sync.conflict, "上报后sync": sync.detail})
    assert conn.sync.conflict  # 重连瞬间尚未上报，判定为挂起冲突
    assert sync.applied and not sync.conflict
