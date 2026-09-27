"""告警状态机。

每个 (规则, 设备) 维护一条当前告警状态，状态迁移只允许：

    None     -> FIRING / ERROR   （首次命中或规则执行异常）
    FIRING   -> RESOLVED         （不再命中）
    FIRING   -> FIRING           （持续命中：去重，不产生新告警，刷新值）
    RESOLVED -> FIRING / ERROR   （再次命中，产生新一条历史）
    ERROR    -> FIRING / RESOLVED / ERROR

防倒退：所有迁移附带事件时间戳，比当前告警时间戳更旧的评估结果一律忽略
（过期遥测不得产生倒退性结论），返回 None。
"""
from __future__ import annotations

import threading

from .errors import ErrorCode
from .models import Alert, AlertState
from .rules import RuleHit


class AlertManager:
    def __init__(self) -> None:
        self._current: dict[tuple[str, str], Alert] = {}
        self._history: list[Alert] = []
        # 每键最近一次评估的事件时间水位，即使没有现存告警也要拦截过期样本
        self._watermark: dict[tuple[str, str], int] = {}
        self._lock = threading.RLock()

    def ingest_hit(
        self,
        hit: RuleHit,
        device_id: str,
        ts_ms: int,
        raw: dict | None = None,
    ) -> Alert | None:
        with self._lock:
            key = (hit.rule.rule_id, device_id)

            # 过期（事件时间倒退）：无论当前有无告警都忽略
            if ts_ms < self._watermark.get(key, -1):
                return None
            self._watermark[key] = ts_ms

            # 信息不足（如本批数据缺少该指标）：维持现状，不产生任何迁移
            if not hit.conclusive:
                return None

            current = self._current.get(key)

            if hit.error is not None:
                target = AlertState.ERROR
                detail = hit.error
                error_code = ErrorCode.RULE_EVALUATION_ERROR
                value = hit.value if isinstance(hit.value, (int, float)) else None
            elif hit.matched:
                target = AlertState.FIRING
                detail = hit.rule.description or "规则命中"
                error_code = None
                value = hit.value
            else:
                target = AlertState.RESOLVED
                detail = (hit.rule.description or "规则") + " 恢复"
                error_code = None
                value = hit.value

            # 状态去重：状态未变且非 ERROR 细节变化，不产生新告警
            if (
                current is not None
                and current.state is target
                and current.error_code == error_code
            ):
                current.changed_at_ms = ts_ms
                if value is not None:
                    current.trigger_value = float(value)
                return None

            # 从未告警过则无需记录"恢复"（恢复只对已存在的 FIRING/ERROR 有意义）
            if target is AlertState.RESOLVED and (
                current is None or current.state is AlertState.RESOLVED
            ):
                return None

            alert = Alert(
                rule_id=hit.rule.rule_id,
                device_id=device_id,
                state=target,
                changed_at_ms=ts_ms,
                detail=detail,
                trigger_value=float(value) if isinstance(value, (int, float)) else None,
                error_code=error_code,
            )
            self._current[key] = alert
            self._history.append(alert)
            return alert

    def resolve_all(self, device_id: str, ts_ms: int) -> list[Alert]:
        """设备离线/复位时，把该设备所有 FIRING/ERROR 告警置为 RESOLVED。"""
        with self._lock:
            changed: list[Alert] = []
            for (rule_id, dev), current in list(self._current.items()):
                if dev != device_id:
                    continue
                if current.state in (AlertState.FIRING, AlertState.ERROR):
                    alert = Alert(
                        rule_id=rule_id,
                        device_id=device_id,
                        state=AlertState.RESOLVED,
                        changed_at_ms=ts_ms,
                        detail="设备离线，告警自动恢复",
                        trigger_value=current.trigger_value,
                    )
                    self._current[(rule_id, dev)] = alert
                    self._history.append(alert)
                    self._watermark[(rule_id, dev)] = ts_ms
                    changed.append(alert)
            return changed

    def active_alerts(self, device_id: str | None = None) -> list[Alert]:
        with self._lock:
            return [
                a
                for a in self._current.values()
                if a.state in (AlertState.FIRING, AlertState.ERROR)
                and (device_id is None or a.device_id == device_id)
            ]

    def history(self, device_id: str | None = None) -> list[Alert]:
        with self._lock:
            return [
                a for a in self._history if device_id is None or a.device_id == device_id
            ]
