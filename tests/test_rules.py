"""规则判定测试。"""
from __future__ import annotations

import logging

import pytest

from iot_twin.errors import ErrorCode
from iot_twin.models import ConnectionState
from iot_twin.rules import Comparator, MetricKind, Rule, RuleEngine

log = logging.getLogger("iot_twin.test.rules")


def _rule(rule_id: str, metric: str, kind: MetricKind, op: Comparator, threshold: float) -> Rule:
    return Rule(
        rule_id=rule_id,
        device_id="dev-1",
        metric=metric,
        kind=kind,
        comparator=op,
        threshold=threshold,
        description=f"{metric} {op} {threshold}",
    )


def test_threshold_comparators():
    engine = RuleEngine()
    rule = _rule("high-temp", "temp", MetricKind.METRIC, Comparator.GT, 40.0)
    hit = engine.evaluate(rule, {"temp": 41.0}, ConnectionState.ONLINE)
    assert hit.matched is True
    hit2 = engine.evaluate(rule, {"temp": 40.0}, ConnectionState.ONLINE)
    assert hit2.matched is False
    log.info("输入: temp=41/40 阈值>40; 判定依据: GT; 结果=%s/%s", hit.matched, hit2.matched)


def test_connection_rule():
    engine = RuleEngine()
    offline_rule = _rule(
        "offline", "connection", MetricKind.CONNECTION, Comparator.EQ, 0.0
    )
    hit = engine.evaluate(offline_rule, {}, ConnectionState.OFFLINE)
    assert hit.matched is True
    assert hit.value == 0.0
    log.info("输入: 设备离线; 判定依据: CONNECTION==0; 结果=%s", hit.matched)


def test_missing_metric_is_inconclusive_and_bad_value_is_normalized():
    engine = RuleEngine()
    rule = _rule("high-temp", "temp", MetricKind.METRIC, Comparator.GT, 40.0)
    missing = engine.evaluate(rule, {"hum": 10}, ConnectionState.ONLINE)
    assert missing.matched is False and missing.error is None and missing.value is None
    log.info("输入: 缺 temp 指标; 判定依据: 信息不足不做结论; hit=%s", missing)

    bad = engine.evaluate(rule, {"temp": "N/A"}, ConnectionState.ONLINE)  # type: ignore[dict-item]
    assert bad.matched is False and bad.error and "ValueError" in bad.error or (
        bad.error and "无法转为数值" in bad.error
    )
    assert bad.value == "N/A"
    log.info("输入: temp='N/A'; 判定依据: 异常归一化; 可定位原始值=%s", bad.value)


def test_rule_lifecycle():
    engine = RuleEngine()
    rule = _rule("r1", "temp", MetricKind.METRIC, Comparator.LT, 5.0)
    engine.add_rule(rule)
    with pytest.raises(Exception) as exc:
        engine.add_rule(rule)
    assert exc.value.code == ErrorCode.RULE_ALREADY_EXISTS
    assert [r.rule_id for r in engine.rules_for("dev-1")] == ["r1"]
    engine.remove_rule("r1")
    assert engine.rules_for("dev-1") == []
    with pytest.raises(Exception) as exc:
        engine.remove_rule("r1")
    assert exc.value.code == ErrorCode.RULE_NOT_FOUND
