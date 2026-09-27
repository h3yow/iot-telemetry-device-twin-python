"""告警链路测试：去重、恢复、乱序防倒退、异常归一化。"""
from __future__ import annotations

import logging

from iot_twin.alerts import AlertManager
from iot_twin.errors import ErrorCode
from iot_twin.models import AlertState, ConnectionState
from iot_twin.rules import Comparator, MetricKind, Rule, RuleEngine

log = logging.getLogger("iot_twin.test.alerts")


def _high_temp_rule() -> Rule:
    return Rule(
        rule_id="high-temp",
        device_id="dev-1",
        metric="temp",
        kind=MetricKind.METRIC,
        comparator=Comparator.GT,
        threshold=40.0,
        description="温度过高",
    )


def test_fire_dedupe_resolve_lifecycle():
    engine = RuleEngine()
    manager = AlertManager()
    rule = _high_temp_rule()

    a1 = manager.ingest_hit(engine.evaluate(rule, {"temp": 41}, ConnectionState.ONLINE), "dev-1", 100)
    assert a1 is not None and a1.state is AlertState.FIRING
    # 持续命中：去重，不产生新告警
    a2 = manager.ingest_hit(engine.evaluate(rule, {"temp": 42}, ConnectionState.ONLINE), "dev-1", 110)
    assert a2 is None
    assert len(manager.active_alerts("dev-1")) == 1
    log.info("输入: temp=41,42 持续超阈; 判定依据: FIRING 去重; 活动告警数=1")

    # 恢复
    resolved = manager.ingest_hit(engine.evaluate(rule, {"temp": 30}, ConnectionState.ONLINE), "dev-1", 120)
    assert resolved is not None and resolved.state is AlertState.RESOLVED
    assert manager.active_alerts("dev-1") == []
    # 再次超阈：产生新告警
    a3 = manager.ingest_hit(engine.evaluate(rule, {"temp": 50}, ConnectionState.ONLINE), "dev-1", 130)
    assert a3 is not None and a3.state is AlertState.FIRING
    history = manager.history("dev-1")
    assert [a.state for a in history] == [
        AlertState.FIRING, AlertState.RESOLVED, AlertState.FIRING
    ]
    log.info("输入: 30 恢复后 50 再命中; 最终历史=%s", [a.state.value for a in history])


def test_stale_evaluation_never_regresses_alert():
    engine = RuleEngine()
    manager = AlertManager()
    rule = _high_temp_rule()
    # 先恢复（ts=200），后来了一个更早 ts=150 的超阈乱序评估
    manager.ingest_hit(engine.evaluate(rule, {"temp": 30}, ConnectionState.ONLINE), "dev-1", 200)
    stale = manager.ingest_hit(engine.evaluate(rule, {"temp": 99}, ConnectionState.ONLINE), "dev-1", 150)
    assert stale is None
    assert manager.active_alerts("dev-1") == []
    log.info("输入: ts=200 已恢复, ts=150 乱序超阈迟到; 判定依据: 事件时间防倒退; 结论=忽略")


def test_rule_error_normalized_and_recoverable():
    engine = RuleEngine()
    manager = AlertManager()
    rule = _high_temp_rule()
    bad = engine.evaluate(rule, {"temp": "N/A"}, ConnectionState.ONLINE)  # type: ignore[dict-item]
    alert = manager.ingest_hit(bad, "dev-1", 100)
    assert alert is not None and alert.state is AlertState.ERROR
    assert alert.error_code == ErrorCode.RULE_EVALUATION_ERROR
    assert "N/A" in alert.detail
    log.info("输入: 非法指标值; 判定依据: 异常归一化 ERROR; 明细=%s", alert.detail)

    recovered = manager.ingest_hit(engine.evaluate(rule, {"temp": 10}, ConnectionState.ONLINE), "dev-1", 110)
    assert recovered is not None and recovered.state is AlertState.RESOLVED
    log.info("输入: 后续正常指标 temp=10; 最终状态=%s", recovered.state)


def test_offline_auto_resolves_alerts():
    engine = RuleEngine()
    manager = AlertManager()
    rule = _high_temp_rule()
    manager.ingest_hit(engine.evaluate(rule, {"temp": 90}, ConnectionState.ONLINE), "dev-1", 100)
    changed = manager.resolve_all("dev-1", ts_ms=200)
    assert len(changed) == 1 and changed[0].state is AlertState.RESOLVED
    assert manager.active_alerts("dev-1") == []
    log.info("输入: 设备离线; 判定依据: 活动告警自动恢复; 恢复数=1")
