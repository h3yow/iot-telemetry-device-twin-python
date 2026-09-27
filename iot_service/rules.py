"""规则判定与告警链路。

规则：
- 阈值规则：对遥测字段做比较（>, >=, <, <=, ==, !=）；
- 连接规则：设备连接状态变为 OFFLINE 时触发；
- 自定义规则：可注入判定函数，便于扩展；
- 告警去重：同一 (device_id, rule_id) 同时只存在一条 ACTIVE 告警，
  条件不再满足时自动置为 RESOLVED（恢复）；
- 乱序/过期防护：按设备记录已评估的最大遥测 seq，seq 不倒退，
  过期遥测不产生倒退性告警结论；
- 异常归一化：规则执行抛出的异常被捕获为 RuleError，
  保留 rule_id、device_id、异常类型与原始信息，可定位。
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from iot_service.models import Alert, AlertStatus, ConnectionState, RuleError

_OPS: dict[str, Callable[[float, float], bool]] = {
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


@dataclass
class _ThresholdRule:
    rule_id: str
    field: str
    op: str
    threshold: float
    message: str


@dataclass
class _ConnectionRule:
    rule_id: str
    message: str


@dataclass
class _CustomRule:
    rule_id: str
    fn: Callable[[dict[str, Any], ConnectionState], bool]
    message: str


class RuleEngine:
    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._threshold_rules: list[_ThresholdRule] = []
        self._connection_rules: list[_ConnectionRule] = []
        self._custom_rules: list[_CustomRule] = []
        self._alerts: dict[tuple[str, str], Alert] = {}  # (device_id, rule_id) -> 当前告警
        self._errors: list[RuleError] = []
        self._last_eval_seq: dict[str, int] = {}
        self._conn_state: dict[str, ConnectionState] = {}
        self._lock = threading.Lock()
        self._clock = clock

    # ---- 规则注册 ----
    def add_threshold_rule(self, rule_id: str, field: str, op: str, threshold: float, message: str = "") -> None:
        if op not in _OPS:
            raise ValueError(f"不支持的比较符: {op}")
        with self._lock:
            self._threshold_rules.append(_ThresholdRule(rule_id, field, op, threshold, message))

    def add_connection_rule(self, rule_id: str, message: str = "") -> None:
        with self._lock:
            self._connection_rules.append(_ConnectionRule(rule_id, message))

    def add_custom_rule(self, rule_id: str, fn: Callable[[dict[str, Any], ConnectionState], bool],
                        message: str = "") -> None:
        with self._lock:
            self._custom_rules.append(_CustomRule(rule_id, fn, message))

    # ---- 告警生命周期 ----
    def _raise_or_keep(self, device_id: str, rule_id: str, message: str, seq: int) -> Alert:
        key = (device_id, rule_id)
        existing = self._alerts.get(key)
        if existing is not None and existing.status == AlertStatus.ACTIVE:
            return existing  # 去重：不重复触发
        alert = Alert(
            alert_id=uuid.uuid4().hex,
            device_id=device_id,
            rule_id=rule_id,
            status=AlertStatus.ACTIVE,
            message=message,
            raised_at=self._clock(),
            source_seq=seq,
        )
        self._alerts[key] = alert
        return alert

    def _resolve_if_active(self, device_id: str, rule_id: str) -> Alert | None:
        key = (device_id, rule_id)
        existing = self._alerts.get(key)
        if existing is not None and existing.status == AlertStatus.ACTIVE:
            existing.status = AlertStatus.RESOLVED
            existing.resolved_at = self._clock()
            return existing
        return None

    def _record_error(self, rule_id: str, device_id: str, exc: BaseException, context: str) -> None:
        self._errors.append(RuleError(
            rule_id=rule_id,
            device_id=device_id,
            error_type=type(exc).__name__,
            detail=f"{exc!r}; context={context}",
        ))

    # ---- 评估入口 ----
    def evaluate_telemetry(self, device_id: str, payload: dict[str, Any], seq: int) -> list[Alert]:
        with self._lock:
            last = self._last_eval_seq.get(device_id, 0)
            if seq <= last:
                return []  # 乱序/过期遥测：不产生倒退性结论
            self._last_eval_seq[device_id] = seq
            conn = self._conn_state.get(device_id, ConnectionState.ONLINE)
            fired: list[Alert] = []
            for rule in self._threshold_rules:
                try:
                    if rule.field not in payload:
                        continue  # 字段缺失：本条规则不适用，不视为异常
                    hit = _OPS[rule.op](float(payload[rule.field]), rule.threshold)
                except Exception as exc:  # 归一化异常
                    self._record_error(rule.rule_id, device_id, exc,
                                       f"payload={payload!r} seq={seq}")
                    continue
                if hit:
                    fired.append(self._raise_or_keep(device_id, rule.rule_id,
                                                     rule.message or f"{rule.field}{rule.op}{rule.threshold}", seq))
                else:
                    resolved = self._resolve_if_active(device_id, rule.rule_id)
                    if resolved is not None:
                        fired.append(resolved)
            for rule in self._custom_rules:
                try:
                    hit = bool(rule.fn(payload, conn))
                except Exception as exc:
                    self._record_error(rule.rule_id, device_id, exc,
                                       f"payload={payload!r} conn={conn} seq={seq}")
                    continue
                if hit:
                    fired.append(self._raise_or_keep(device_id, rule.rule_id, rule.message, seq))
                else:
                    resolved = self._resolve_if_active(device_id, rule.rule_id)
                    if resolved is not None:
                        fired.append(resolved)
            return fired

    def evaluate_connection(self, device_id: str, state: ConnectionState) -> list[Alert]:
        with self._lock:
            self._conn_state[device_id] = state
            fired: list[Alert] = []
            for rule in self._connection_rules:
                if state == ConnectionState.OFFLINE:
                    fired.append(self._raise_or_keep(device_id, rule.rule_id,
                                                     rule.message or "设备离线", seq=0))
                else:
                    resolved = self._resolve_if_active(device_id, rule.rule_id)
                    if resolved is not None:
                        fired.append(resolved)
            return fired

    # ---- 查询 ----
    def active_alerts(self, device_id: str | None = None) -> list[Alert]:
        with self._lock:
            return [a for (did, _), a in self._alerts.items()
                    if a.status == AlertStatus.ACTIVE and (device_id is None or did == device_id)]

    def errors(self) -> list[RuleError]:
        with self._lock:
            return list(self._errors)
