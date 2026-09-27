"""规则判定：基于遥测阈值或连接状态。

判定本身不抛异常：指标缺失、类型不可比较等执行异常归一化为
``RuleHit.matched=False, error=<可定位信息>``，原始规则与输入值
保留在 ``RuleHit`` 中，交由告警链路统一记录。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum

from .errors import DomainError, ErrorCode
from .models import ConnectionState


class Comparator(str, Enum):
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"
    EQ = "EQ"

    def compare(self, left: float, right: float) -> bool:
        if self is Comparator.GT:
            return left > right
        if self is Comparator.GTE:
            return left >= right
        if self is Comparator.LT:
            return left < right
        if self is Comparator.LTE:
            return left <= right
        return left == right


class MetricKind(str, Enum):
    METRIC = "METRIC"               # 比较 metrics[metric] 与 threshold
    CONNECTION = "CONNECTION"       # threshold: 1=ONLINE, 0=OFFLINE


@dataclass(frozen=True)
class Rule:
    rule_id: str
    device_id: str
    metric: str
    kind: MetricKind
    comparator: Comparator
    threshold: float
    description: str = ""


@dataclass
class RuleHit:
    rule: Rule
    value: float | None
    matched: bool
    error: str | None = None
    conclusive: bool = True  # False = 信息不足（如指标缺失），不产生任何告警结论


class RuleEngine:
    def __init__(self) -> None:
        self._rules: dict[str, Rule] = {}
        self._by_device: dict[str, set[str]] = {}
        self._lock = threading.RLock()

    def add_rule(self, rule: Rule) -> None:
        with self._lock:
            if rule.rule_id in self._rules:
                raise DomainError(
                    ErrorCode.RULE_ALREADY_EXISTS,
                    f"规则 {rule.rule_id} 已存在",
                )
            self._rules[rule.rule_id] = rule
            self._by_device.setdefault(rule.device_id, set()).add(rule.rule_id)

    def remove_rule(self, rule_id: str) -> None:
        with self._lock:
            rule = self._rules.pop(rule_id, None)
            if rule is None:
                raise DomainError(
                    ErrorCode.RULE_NOT_FOUND,
                    f"规则 {rule_id} 不存在",
                )
            self._by_device.get(rule.device_id, set()).discard(rule_id)

    def get(self, rule_id: str) -> Rule:
        with self._lock:
            rule = self._rules.get(rule_id)
            if rule is None:
                raise DomainError(ErrorCode.RULE_NOT_FOUND, f"规则 {rule_id} 不存在")
            return rule

    def rules_for(self, device_id: str) -> list[Rule]:
        with self._lock:
            return [self._rules[r] for r in sorted(self._by_device.get(device_id, ()))]

    def evaluate(
        self,
        rule: Rule,
        metrics: dict,
        connection: ConnectionState,
    ) -> RuleHit:
        try:
            if rule.kind is MetricKind.CONNECTION:
                value = 1.0 if connection is ConnectionState.ONLINE else 0.0
            else:
                if rule.metric not in metrics:
                    # 指标缺失 = 该批数据不包含此指标，信息不足，不做任何结论
                    return RuleHit(rule=rule, value=None, matched=False, conclusive=False)
                try:
                    value = float(metrics[rule.metric])
                except (TypeError, ValueError):
                    return RuleHit(
                        rule=rule,
                        value=metrics.get(rule.metric),
                        matched=False,
                        error=(
                            f"规则 {rule.rule_id} 执行异常: 指标 '{rule.metric}' "
                            f"无法转为数值, 原始值={metrics[rule.metric]!r}"
                        ),
                    )
            return RuleHit(
                rule=rule,
                value=value,
                matched=rule.comparator.compare(value, float(rule.threshold)),
            )
        except (TypeError, ValueError) as exc:  # pragma: no cover - 兜底
            return RuleHit(
                rule=rule,
                value=metrics.get(rule.metric) if rule.kind is MetricKind.METRIC else None,
                matched=False,
                error=f"规则 {rule.rule_id} 执行异常: {type(exc).__name__}: {exc}",
            )
