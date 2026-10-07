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
import uuid

from .alerts import AlertManager
from .clock import Clock, SystemClock
from .dedupe import DeduplicationWindow
from .dispatch import BatchDispatcher
from .errors import DomainError, ErrorCode
from .models import (
    Alert,
    BatchDesiredItem,
    BatchItemResult,
    BatchItemStatus,
    BatchRecord,
    Device,
    IngestResult,
    Principal,
    RejectReason,
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
        self.batches = BatchDispatcher()
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
        log.info(
            "设备上线 device_id=%s at=%s 期望版本=%s 同步状态=%s",
            device_id,
            now,
            snapshot.desired.version if snapshot.desired else None,
            snapshot.sync_status.value,
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
        with twin._lock:
            twin.evaluate_timeout(self.clock.now_ms())
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
            "遥测计入 device_id=%s seq=%s ts=%s metrics=%s 对账=%s 依据=%s 冲突明细=%s 超时明细=%s",
            device_id, seq, ts_ms, metrics, sync_status.value, reason,
            twin.conflict_detail() if sync_status is SyncStatus.CONFLICTED else "",
            twin.timeout_detail() if sync_status is SyncStatus.ACK_TIMEOUT else "",
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
        self.registry.get(device_id)
        now = self.clock.now_ms()
        twin = self._twin(device_id)
        snapshot = twin.update_desired(
            values, expected_version, now,
            merge=merge, ack_timeout_ms=ack_timeout_ms,
        )
        log.info(
            "期望状态下发 device_id=%s 版本=v%s merge=%s 时限ms=%s deadline=%s "
            "连接=%s 同步=%s values=%s",
            device_id,
            snapshot.desired.version if snapshot.desired else 0,
            merge,
            ack_timeout_ms,
            snapshot.desired.deadline_ms if snapshot.desired else None,
            snapshot.connection.value,
            snapshot.sync_status.value,
            values,
        )
        return snapshot

    def revoke_desired(
        self,
        principal: Principal,
        device_id: str,
        expected_version: int | None = None,
    ) -> TwinState:
        """撤销该设备尚未确认的期望变更（管理员操作）。"""
        if principal.role is not Role.ADMIN:
            raise DomainError(
                ErrorCode.PERMISSION_DENIED,
                "只有管理员可以撤销期望状态",
            )
        self.registry.get(device_id)
        now = self.clock.now_ms()
        twin = self._twin(device_id)
        before = twin.state.desired.version if twin.state.desired else None
        snapshot = twin.revoke_desired(now, expected_version=expected_version)
        after = snapshot.desired.version if snapshot.desired else None
        log.info(
            "期望状态撤销 device_id=%s 撤销前版本=v%s 撤销后版本=%s 同步=%s",
            device_id, before, after, snapshot.sync_status.value,
        )
        return snapshot

    # ---- 批量下发 ----
    def dispatch_batch(
        self,
        principal: Principal,
        items: list[BatchDesiredItem],
        *,
        batch_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> BatchRecord:
        """一次给一批设备下发期望；逐台独立出结果，单台失败不影响其它。

        相同 ``idempotency_key`` 重复提交返回首次批次，不重复下发、版本不乱跳。
        """
        if principal.role is not Role.ADMIN:
            raise DomainError(
                ErrorCode.PERMISSION_DENIED,
                "只有管理员可以批量下发期望状态",
            )
        # 批次整体合法性先校验（空批次 / 批次内重复设备）
        BatchDispatcher._validate(items)
        # 所有设备须已注册（逐台结论里体现，不在这里整批拒绝）；实际逐台执行时判定
        now = self.clock.now_ms()
        resolved_batch_id = batch_id or f"batch-{uuid.uuid4().hex}"

        def _dispatch_one(item: BatchDesiredItem) -> BatchItemResult:
            return self._dispatch_one(principal, item)

        record = self.batches.submit(
            resolved_batch_id, items, now, idempotency_key, _dispatch_one
        )
        record = self._fill_current_status(record)
        accepted = sum(1 for i in record.items if i.status is BatchItemStatus.DISPATCHED)
        log.info(
            "批量下发完成 batch_id=%s 幂等键=%s 总数=%s 成功=%s 失败=%s 明细=%s",
            record.batch_id, idempotency_key, len(record.items),
            accepted, len(record.items) - accepted,
            [(i.device_id, i.status.value, i.reject_reason) for i in record.items],
        )
        return record

    def _dispatch_one(
        self, principal: Principal, item: BatchDesiredItem
    ) -> BatchItemResult:
        """逐台下发：任何单台失败都收敛为 REJECTED 结论，不抛出。"""
        # update_desired 内部先查注册/权限，未注册不会提前造出空孪生
        snapshot = self.update_desired(
            principal,
            item.device_id,
            dict(item.values),
            expected_version=item.expected_version,
            merge=item.merge,
            ack_timeout_ms=item.ack_timeout_ms,
        )
        return BatchItemResult(
            device_id=item.device_id,
            status=BatchItemStatus.DISPATCHED,
            desired_version=snapshot.desired.version if snapshot.desired else None,
            current_sync_status=snapshot.sync_status.value,
        )

    def get_batch(self, principal: Principal, batch_id: str) -> BatchRecord:
        """查询一批操作最终的逐台结论与原因（管理员）。"""
        if principal.role is not Role.ADMIN:
            raise DomainError(
                ErrorCode.PERMISSION_DENIED,
                "只有管理员可以查询批量下发结果",
            )
        record = self.batches.get(batch_id)
        return self._fill_current_status(record)

    def _fill_current_status(self, record: BatchRecord) -> BatchRecord:
        """为批次中每台成功下发的设备回填当前对账结论（含超时纠正后的最新值）。"""
        for item in record.items:
            if item.status is not BatchItemStatus.DISPATCHED:
                continue
            twin = self._twins.get(item.device_id)
            if twin is None:
                continue
            with twin._lock:
                twin.evaluate_timeout(self.clock.now_ms())
                item.current_sync_status = twin.state.sync_status.value
        return record

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
