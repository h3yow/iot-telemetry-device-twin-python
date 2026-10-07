"""门面服务：编排注册鉴权、遥测汇聚、孪生对账、规则告警。

所有判定都经过同一入口，保证：

* 未注册 / 凭证无效 / 权限不足三类失败在鉴权阶段就被可区分地拒绝
  （``ingest_telemetry`` 用 ``IngestResult`` 表达拒绝原因，不抛出到调用方）；
* 重复上报 -> ``DUPLICATE_SAMPLE``；过期/乱序倒退 -> ``STALE_SAMPLE``；
* 只有真正计入汇聚的样本才会驱动孪生对账与规则告警；
* 重连时自动重放挂起期望（状态保持 PENDING）并对设备规则做一次
  连接态评估；断连时活动告警自动恢复。
"""
from __future__ import annotations

import logging
import threading

from .alerts import AlertManager
from .batch import BatchManager
from .clock import Clock, SystemClock
from .dedupe import DeduplicationWindow
from .errors import DomainError, ErrorCode
from .models import (
    Alert,
    BatchDesiredItem,
    BatchItemResult,
    BatchOperation,
    Device,
    IngestResult,
    ItemStatus,
    Principal,
    RejectReason,
    RollbackOutcome,
    Role,
    SyncStatus,
    TelemetrySample,
    TwinState,
)
from .registry import DeviceRegistry
from .rules import MetricKind, Rule
from .rules import RuleEngine
from .telemetry import TelemetryStore
from .twin import DeviceTwin

log = logging.getLogger("iot_twin.service")

_REASON_BY_CODE = {
    ErrorCode.DEVICE_NOT_REGISTERED: RejectReason.DEVICE_NOT_REGISTERED,
    ErrorCode.INVALID_CREDENTIAL: RejectReason.INVALID_CREDENTIAL,
    ErrorCode.PERMISSION_DENIED: RejectReason.PERMISSION_DENIED,
}


class IoTService:
    def __init__(self, clock: Clock | None = None) -> None:
        self.clock: Clock = clock or SystemClock()
        self.registry = DeviceRegistry()
        self.dedupe = DeduplicationWindow()
        self._stores: dict[str, TelemetryStore] = {}
        self._twins: dict[str, DeviceTwin] = {}
        self.rules = RuleEngine()
        self.alerts = AlertManager()
        self.batches = BatchManager()
        self._lock = threading.RLock()

    # ---- 内部访问器 ----
    def _store(self, device_id: str) -> TelemetryStore:
        with self._lock:
            store = self._stores.get(device_id)
            if store is None:
                store = TelemetryStore()
                self._stores[device_id] = store
            return store

    def _twin(self, device_id: str) -> DeviceTwin:
        with self._lock:
            twin = self._twins.get(device_id)
            if twin is None:
                twin = DeviceTwin(device_id)
                self._twins[device_id] = twin
            return twin

    # ---- 设备管理 ----
    def register_device(self, device_id: str, credential: str) -> Device:
        now = self.clock.now_ms()
        device = self.registry.register(device_id, credential, now_ms=now)
        log.info("注册设备 device_id=%s registered_at=%s", device_id, now)
        return device

    def authenticate(self, device_id: str, credential: str) -> Principal:
        return self.registry.authenticate(device_id, credential)

    def _authorize(self, principal: Principal, device_id: str, *, device_only: bool = False) -> None:
        self.registry.authorize(principal, device_id, device_only=device_only)

    # ---- 连接生命周期 ----
    def connect(self, principal: Principal, device_id: str) -> TwinState:
        self._authorize(principal, device_id)
        now = self.clock.now_ms()
        twin = self._twin(device_id)
        snapshot = twin.connect(now)
        twin.mark_delivered(now)
        # 重连已在孪生内把确认时限从上线时刻重算；这里惰性复核一次
        twin.evaluate_ack_deadline(now)
        log.info(
            "设备上线 device_id=%s at=%s 期望版本=%s 同步状态=%s 确认时限=%s",
            device_id,
            now,
            snapshot.desired.version if snapshot.desired else None,
            twin.state.sync_status.value,
            twin.ack_deadline(),
        )
        self._evaluate_connection_rules(device_id, now)
        return twin.snapshot()

    def disconnect(self, principal: Principal, device_id: str) -> TwinState:
        self._authorize(principal, device_id)
        now = self.clock.now_ms()
        twin = self._twin(device_id)
        snapshot = twin.disconnect(now)
        log.info("设备离线 device_id=%s at=%s", device_id, now)
        self.alerts.resolve_all(device_id, ts_ms=now)
        self._evaluate_connection_rules(device_id, now)
        return snapshot

    def get_twin(self, principal: Principal, device_id: str) -> TwinState:
        self._authorize(principal, device_id)
        twin = self._twin(device_id)
        # 查询时惰性复核确认时限：无需后台定时器即可得到超时结论
        twin.evaluate_ack_deadline(self.clock.now_ms())
        return twin.snapshot()

    # ---- 遥测 ----
    def ingest_telemetry(
        self,
        principal: Principal,
        device_id: str,
        seq: int,
        ts_ms: int,
        metrics: dict,
        *,
        ack_desired_version: int | None = None,
    ) -> IngestResult:
        try:
            self._authorize(principal, device_id, device_only=True)
        except DomainError as exc:
            log.info("遥测被拒绝 device_id=%s seq=%s 原因=%s", device_id, seq, exc.code)
            return IngestResult(False, device_id, seq, _REASON_BY_CODE[exc.code].value, exc.message)

        if not isinstance(metrics, dict) or not all(isinstance(k, str) for k in metrics):
            return IngestResult(
                False, device_id, seq,
                RejectReason.BAD_TELEMETRY.value,
                "metrics 必须为以字符串为键的字典",
            )

        now = self.clock.now_ms()

        # 1) 幂等去重
        if not self.dedupe.check_and_remember(device_id, seq):
            log.info("遥测重复 device_id=%s seq=%s -> DUPLICATE_SAMPLE", device_id, seq)
            return IngestResult(
                False, device_id, seq,
                RejectReason.DUPLICATE_SAMPLE.value,
                f"seq={seq} 在去重窗口内已上报",
            )

        store = self._store(device_id)
        # 2) 乱序/过期防倒退（去重先占位，过期样本也不应再次计入）
        if not store.is_acceptable(ts_ms, seq):
            log.info(
                "遥测过期 device_id=%s seq=%s ts=%s 水位=%s -> STALE_SAMPLE",
                device_id, seq, ts_ms, store.watermark_ms,
            )
            return IngestResult(
                False, device_id, seq,
                RejectReason.STALE_SAMPLE.value,
                f"ts={ts_ms},seq={seq} 不高于水位 ts={store.watermark_ms},seq={store.last_seq}",
            )

        sample = TelemetrySample(
            device_id=device_id,
            seq=seq,
            ts_ms=ts_ms,
            metrics=dict(metrics),
            received_at_ms=now,
        )
        store.add(sample)

        # 3) 孪生对账
        twin = self._twin(device_id)
        accepted, sync_status, reason = twin.apply_reported(
            dict(metrics), seq, ts_ms, now, ack_desired_version=ack_desired_version
        )
        log.info(
            "遥测计入 device_id=%s seq=%s ts=%s metrics=%s 对账=%s 依据=%s 冲突明细=%s",
            device_id, seq, ts_ms, metrics, sync_status.value, reason,
            twin.conflict_detail() if sync_status is SyncStatus.CONFLICTED else "",
        )

        # 4) 规则与告警（仅被计入的新样本触发）
        self._evaluate_metric_rules(device_id, metrics, ts_ms)
        return IngestResult(True, device_id, seq, None, reason)

    def backfill(
        self,
        principal: Principal,
        device_id: str,
        samples: list[TelemetrySample],
        ack_desired_version: int | None = None,
    ) -> list[IngestResult]:
        """重连后批量补传：按 (ts,seq) 排序逐条并入，不允许倒退。"""
        self._authorize(principal, device_id, device_only=True)
        ordered = sorted(samples, key=lambda s: (s.ts_ms, s.seq))
        log.info("批量补传 device_id=%s 数量=%s 顺序seq=%s",
                 device_id, len(ordered), [s.seq for s in ordered])
        results: list[IngestResult] = []
        for sample in ordered:
            results.append(
                self.ingest_telemetry(
                    principal,
                    device_id,
                    sample.seq,
                    sample.ts_ms,
                    dict(sample.metrics),
                    ack_desired_version=ack_desired_version,
                )
            )
        return results

    # ---- 期望状态 ----
    def update_desired(
        self,
        principal: Principal,
        device_id: str,
        values: dict,
        expected_version: int | None = None,
        merge: bool = True,
        ack_timeout_ms: int | None = None,
    ) -> TwinState:
        # 设备自身与管理员都可以查看，但下发期望属于管理操作：仅管理员
        if principal.role is not Role.ADMIN:
            raise DomainError(
                ErrorCode.PERMISSION_DENIED,
                "只有管理员可以下发期望状态",
            )
        if not isinstance(values, dict) or not all(isinstance(k, str) for k in values):
            raise DomainError(
                ErrorCode.BAD_TELEMETRY,
                "values 必须为以字符串为键的字典",
            )
        if ack_timeout_ms is not None and ack_timeout_ms <= 0:
            raise DomainError(
                ErrorCode.BAD_TELEMETRY,
                "ack_timeout_ms 必须为正数",
            )
        self.registry.get(device_id)
        now = self.clock.now_ms()
        twin = self._twin(device_id)
        snapshot = twin.update_desired(
            values, expected_version, now, merge=merge,
            ack_timeout_ms=ack_timeout_ms,
        )
        # 边界：下发时刻恰好已到时限的极端情况也惰性复核一次
        twin.evaluate_ack_deadline(now)
        log.info(
            "期望状态下发 device_id=%s 版本=v%s merge=%s 连接=%s 同步=%s 时限=%s values=%s",
            device_id,
            snapshot.desired.version if snapshot.desired else 0,
            merge,
            snapshot.connection.value,
            twin.state.sync_status.value,
            twin.ack_deadline(),
            values,
        )
        return twin.snapshot()

    def rollback_desired(
        self,
        principal: Principal,
        device_id: str,
        version: int | None = None,
    ) -> RollbackOutcome:
        """撤销一次尚未被设备确认的期望变更（管理员操作，幂等）。"""
        if principal.role is not Role.ADMIN:
            raise DomainError(
                ErrorCode.PERMISSION_DENIED,
                "只有管理员可以撤销期望状态",
            )
        self.registry.get(device_id)
        now = self.clock.now_ms()
        twin = self._twin(device_id)
        outcome = twin.rollback_desired(now, version=version)
        log.info(
            "期望状态撤销 device_id=%s 指定版本=%s 是否撤销=%s 幂等=%s 当前版本=%s 依据=%s",
            device_id, version, outcome.rolled_back, outcome.idempotent,
            outcome.desired_version, outcome.detail,
        )
        return outcome

    def batch_update_desired(
        self,
        principal: Principal,
        batch_id: str,
        items: list[BatchDesiredItem],
    ) -> BatchOperation:
        """批量下发期望状态：逐台隔离、batch_id 幂等、结论可查。

        每台设备各自独立鉴权/校验/下发，一台被拒绝不影响其它设备；
        对同一 ``batch_id`` 的相同重试直接返回首拍结论，不重复下发、
        不造成版本乱跳。
        """
        if principal.role is not Role.ADMIN:
            raise DomainError(
                ErrorCode.PERMISSION_DENIED,
                "只有管理员可以批量下发期望状态",
            )
        now = self.clock.now_ms()

        def dispatch_one(item: BatchDesiredItem) -> BatchItemResult:
            # 单台内部复用单台下发的全部校验与并发规则；
            # DomainError 由 BatchManager 归一化为逐台 REJECTED。
            self.registry.get(item.device_id)
            twin = self._twin(item.device_id)
            snapshot = twin.update_desired(
                item.values, item.expected_version, now, merge=item.merge
            )
            twin.evaluate_ack_deadline(now)
            return BatchItemResult(
                device_id=item.device_id,
                status=ItemStatus.DELIVERED,
                desired_version=snapshot.desired.version if snapshot.desired else None,
                detail="已下发，等待设备确认",
            )

        operation = self.batches.get_or_dispatch(batch_id, items, now, dispatch_one)
        delivered = sum(r.status is ItemStatus.DELIVERED for r in operation.results)
        log.info(
            "批量期望下发 batch_id=%s 总台数=%s 成功=%s 失败=%s 逐台=%s",
            batch_id, len(operation.results), delivered,
            len(operation.results) - delivered,
            [(r.device_id, r.status.value, r.reject_reason) for r in operation.results],
        )
        return operation

    def get_batch(self, principal: Principal, batch_id: str) -> BatchOperation:
        """查询一批操作的逐台首拍结论。"""
        if principal.role is not Role.ADMIN:
            raise DomainError(ErrorCode.PERMISSION_DENIED, "只有管理员可以查询批量操作")
        operation = self.batches.get(batch_id)
        if operation is None:
            raise DomainError(ErrorCode.BATCH_NOT_FOUND, f"批次 {batch_id} 不存在")
        return operation

    def batch_device_status(
        self, principal: Principal, batch_id: str
    ) -> list[BatchItemResult]:
        """查询一批操作各设备的最新逐台结论（含后续确认/超时演进）。

        首拍成功的设备会附带当前对账结论（PENDING / IN_SYNC / CONFLICTED /
        ACK_TIMEOUT），被拒设备保持首拍拒绝原因。
        """
        operation = self.get_batch(principal, batch_id)
        now = self.clock.now_ms()
        latest: list[BatchItemResult] = []
        for result in operation.results:
            if result.status is not ItemStatus.DELIVERED:
                latest.append(result)
                continue
            twin = self._twin(result.device_id)
            status = twin.evaluate_ack_deadline(now)
            latest.append(
                BatchItemResult(
                    device_id=result.device_id,
                    status=ItemStatus.DELIVERED,
                    desired_version=(
                        twin.state.desired.version if twin.state.desired else None
                    ),
                    detail=f"已下发，当前对账结论={status.value}",
                )
            )
        return latest

    # ---- 规则与告警 ----
    def add_rule(self, principal: Principal, rule: Rule) -> None:
        if principal.role is not Role.ADMIN:
            raise DomainError(ErrorCode.PERMISSION_DENIED, "只有管理员可以配置规则")
        self.registry.get(rule.device_id)
        self.rules.add_rule(rule)
        log.info("规则新增 rule_id=%s device_id=%s", rule.rule_id, rule.device_id)

    def _evaluate_metric_rules(
        self, device_id: str, metrics: dict, ts_ms: int
    ) -> None:
        twin = self._twin(device_id)
        connection = twin.state.connection
        for rule in self.rules.rules_for(device_id):
            hit = self.rules.evaluate(rule, metrics, connection)
            alert = self.alerts.ingest_hit(hit, device_id, ts_ms, raw=dict(metrics))
            if alert is not None:
                log.info(
                    "告警迁移 rule_id=%s device_id=%s -> %s value=%s detail=%s",
                    rule.rule_id, device_id, alert.state.value, alert.trigger_value, alert.detail,
                )

    def _evaluate_connection_rules(self, device_id: str, ts_ms: int) -> None:
        twin = self._twin(device_id)
        for rule in self.rules.rules_for(device_id):
            if rule.kind is not MetricKind.CONNECTION:
                continue
            hit = self.rules.evaluate(rule, {}, twin.state.connection)
            alert = self.alerts.ingest_hit(hit, device_id, ts_ms)
            if alert is not None:
                log.info(
                    "连接告警 rule_id=%s -> %s detail=%s",
                    rule.rule_id, alert.state.value, alert.detail,
                )

    def list_alerts(self, principal: Principal, device_id: str | None = None) -> list[Alert]:
        if principal.role is not Role.ADMIN and device_id != principal.device_id:
            raise DomainError(ErrorCode.PERMISSION_DENIED, "无权查看该设备告警")
        return self.alerts.active_alerts(device_id)

    def alert_history(self, principal: Principal, device_id: str | None = None) -> list[Alert]:
        if principal.role is not Role.ADMIN and device_id != principal.device_id:
            raise DomainError(ErrorCode.PERMISSION_DENIED, "无权查看该设备告警历史")
        return self.alerts.history(device_id)
