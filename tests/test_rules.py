"""规则判定与告警链路：阈值、去重、恢复、乱序防护、异常归一化、并发上报。"""
import threading

from iot_service.models import AlertStatus, TelemetryMessage

from conftest import log_case


def _temp_msg(device: str, seq: int, temp: float) -> TelemetryMessage:
    return TelemetryMessage(device_id=device, payload={"temp": temp},
                            message_id=f"{device}-t{seq}", seq=seq)


def test_threshold_alert_dedup_and_recovery(service):
    service.register_device("d1", "tok")
    service.rules.add_threshold_rule("high-temp", "temp", ">", 30.0, "温度过高")
    service.connect("d1", "tok")
    s1 = [(a.rule_id, a.alert_id, a.status) for a in service.ingest("d1", "tok", _temp_msg("d1", 1, 35.0)).alerts]
    s2 = [(a.rule_id, a.alert_id, a.status) for a in service.ingest("d1", "tok", _temp_msg("d1", 2, 36.0)).alerts]
    s3 = [(a.rule_id, a.alert_id, a.status) for a in service.ingest("d1", "tok", _temp_msg("d1", 3, 25.0)).alerts]
    log_case("阈值告警去重与恢复", {"temp序列": [35, 36, 25], "阈值": ">30"},
             "ACTIVE 期间不重复触发；条件不满足则恢复",
             {"t1": [(r, str(s)) for r, _, s in s1],
              "t2去重": [(r, str(s)) for r, _, s in s2],
              "t3恢复": [(r, str(s)) for r, _, s in s3]})
    assert [s for _, _, s in s1] == [AlertStatus.ACTIVE]
    assert {aid for _, aid, _ in s1} == {aid for _, aid, _ in s2}  # 同一条告警，未重复
    assert [s for _, _, s in s3] == [AlertStatus.RESOLVED]
    assert service.rules.active_alerts("d1") == []


def test_stale_telemetry_no_regressive_alert(service):
    service.register_device("d1", "tok")
    service.rules.add_threshold_rule("high-temp", "temp", ">", 30.0)
    service.connect("d1", "tok")
    service.ingest("d1", "tok", _temp_msg("d1", 5, 20.0))   # 最新：正常
    stale = service.ingest("d1", "tok", _temp_msg("d1", 4, 99.0))  # 迟到的高温
    log_case("过期遥测不产生告警", {"最新": (5, 20.0), "迟到": (4, 99.0)},
             "seq 过期直接拒绝，不进入规则评估",
             {"stale_reason": stale.reason, "alerts": stale.alerts,
              "active": service.rules.active_alerts("d1")})
    assert stale.alerts == [] and service.rules.active_alerts("d1") == []


def test_rule_error_normalized_with_context(service):
    service.register_device("d1", "tok")
    service.rules.add_threshold_rule("bad-op-field", "temp", ">", 30.0)
    service.rules.add_custom_rule("boom", lambda p, c: 1 / 0, "总是炸")
    service.connect("d1", "tok")
    service.ingest("d1", "tok", TelemetryMessage(device_id="d1", payload={"temp": "not-a-number"},
                                                 message_id="x1", seq=1))
    errors = service.rules.errors()
    log_case("规则异常归一化", {"payload": {"temp": "not-a-number"}, "自定义规则": "1/0"},
             "异常捕获为 RuleError，保留 rule_id/device_id/类型/原始信息",
             {"errors": [(e.rule_id, e.error_type) for e in errors]})
    kinds = {(e.rule_id, e.error_type) for e in errors}
    assert ("boom", "ZeroDivisionError") in kinds
    assert ("bad-op-field", "ValueError") in kinds
    assert all(e.device_id == "d1" and "context=" in e.detail for e in errors)


def test_concurrent_ingest_consistent(service):
    service.register_device("d1", "tok")
    service.rules.add_threshold_rule("high-temp", "temp", ">", 30.0)
    service.connect("d1", "tok")
    n_threads, n_msgs = 8, 20
    barrier = threading.Barrier(n_threads)

    def worker(tid: int) -> None:
        barrier.wait()
        for i in range(n_msgs):
            seq = tid * n_msgs + i + 1
            service.ingest("d1", "tok", _temp_msg("d1", seq, float(seq)))

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    last = service.telemetry.last_seq("d1")
    active = service.rules.active_alerts("d1")
    log_case("并发上报一致性", {"线程": n_threads, "每线程消息": n_msgs},
             "全局锁保证 seq 与规则评估串行化",
             {"last_seq": last, "期望": n_threads * n_msgs,
              "active_alerts": [(a.rule_id, str(a.status)) for a in active]})
    assert last == n_threads * n_msgs
    assert len(active) == 1 and active[0].rule_id == "high-temp"  # 高温告警恰好一条
