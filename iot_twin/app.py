"""可选 HTTP 适配层：把本地服务暴露为 FastAPI 接口。

* 仅绑定本地回环，不依赖任何外部中间件或联网服务；
* 设备接口使用简单的 Bearer Token（token 即注册凭证）鉴权；
* 业务失败统一返回结构化 JSON，``error_code`` 与领域错误码一一对应。

测试不依赖该层，核心行为全部在服务层验证。
"""
from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from .errors import DomainError, ErrorCode
from .models import Principal, Role
from .rules import Comparator, MetricKind, Rule
from .service import IoTService


class RegisterIn(BaseModel):
    device_id: str
    credential: str


class ConnectIn(BaseModel):
    online: bool = True


class TelemetryIn(BaseModel):
    seq: int
    ts_ms: int
    # 状态值可以是非数值（如 led="on"）；只有数值指标参与阈值规则
    metrics: dict[str, Any]
    ack_desired_version: int | None = None


class BackfillIn(BaseModel):
    samples: list[TelemetryIn]
    ack_desired_version: int | None = None


class DesiredIn(BaseModel):
    values: dict[str, Any]
    expected_version: int | None = None
    merge: bool = True
    ack_timeout_ms: int | None = None


class RollbackIn(BaseModel):
    version: int | None = None


class BatchItemIn(BaseModel):
    device_id: str
    values: dict[str, Any] = Field(default_factory=dict)
    expected_version: int | None = None
    merge: bool = True


class BatchIn(BaseModel):
    batch_id: str
    items: list[BatchItemIn]


class RuleIn(BaseModel):
    rule_id: str
    device_id: str
    metric: str
    kind: MetricKind
    comparator: Comparator
    threshold: float
    description: str = ""


def create_app() -> FastAPI:
    app = FastAPI(title="iot-telemetry-device-twin", version="0.1.0")
    svc = IoTService()
    app.state.service = svc

    def _principal(device_id: str, authorization: str | None) -> Principal:
        # 管理接口约定: Authorization: Bearer admin:<token 占位>
        if authorization and authorization.startswith("Bearer admin"):
            return Principal(device_id=None, role=Role.ADMIN)
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail={"error_code": ErrorCode.INVALID_CREDENTIAL})
        credential = authorization.removeprefix("Bearer ").strip()
        try:
            return svc.authenticate(device_id, credential)
        except DomainError as exc:
            status = 404 if exc.code == ErrorCode.DEVICE_NOT_REGISTERED else 401
            raise HTTPException(status_code=status, detail={"error_code": exc.code, "message": exc.message})

    def admin_principal(authorization: str | None = Header(default=None)) -> Principal:
        if authorization == "Bearer admin":
            return Principal(device_id=None, role=Role.ADMIN)
        raise HTTPException(status_code=403, detail={"error_code": ErrorCode.PERMISSION_DENIED})

    @app.post("/admin/devices")
    def register_device(body: RegisterIn, admin: Principal = Depends(admin_principal)) -> dict:
        device = svc.register_device(body.device_id, body.credential)
        return {"device_id": device.device_id, "registered_at_ms": device.registered_at_ms}

    @app.post("/admin/rules")
    def add_rule(body: RuleIn, admin: Principal = Depends(admin_principal)) -> dict:
        rule = Rule(
            rule_id=body.rule_id,
            device_id=body.device_id,
            metric=body.metric,
            kind=body.kind,
            comparator=body.comparator,
            threshold=body.threshold,
            description=body.description,
        )
        try:
            svc.add_rule(admin, rule)
        except DomainError as exc:
            raise HTTPException(status_code=409, detail={"error_code": exc.code, "message": exc.message})
        return {"rule_id": rule.rule_id, "status": "created"}

    @app.post("/admin/desired/{device_id}")
    def update_desired(
        device_id: str, body: DesiredIn, admin: Principal = Depends(admin_principal)
    ) -> dict:
        twin = svc.update_desired(
            admin, device_id, body.values,
            expected_version=body.expected_version, merge=body.merge,
            ack_timeout_ms=body.ack_timeout_ms,
        )
        return _twin_dict(twin, ack_deadline_ms=svc._twin(device_id).ack_deadline())

    @app.post("/admin/desired/{device_id}/rollback")
    def rollback_desired(
        device_id: str, body: RollbackIn, admin: Principal = Depends(admin_principal)
    ) -> dict:
        try:
            outcome = svc.rollback_desired(admin, device_id, version=body.version)
        except DomainError as exc:
            status_code = (
                409 if exc.code in (
                    ErrorCode.VERSION_CONFLICT, ErrorCode.ALREADY_CONFIRMED
                ) else 400
            )
            raise HTTPException(
                status_code=status_code,
                detail={"error_code": exc.code, "message": exc.message},
            )
        return {
            "device_id": outcome.device_id,
            "rolled_back": outcome.rolled_back,
            "idempotent": outcome.idempotent,
            "desired_version": outcome.desired_version,
            "detail": outcome.detail,
        }

    @app.post("/admin/desired-batch")
    def batch_update_desired(body: BatchIn, admin: Principal = Depends(admin_principal)) -> dict:
        from .models import BatchDesiredItem

        items = [
            BatchDesiredItem(
                device_id=i.device_id,
                values=i.values,
                expected_version=i.expected_version,
                merge=i.merge,
            )
            for i in body.items
        ]
        try:
            operation = svc.batch_update_desired(admin, body.batch_id, items)
        except DomainError as exc:
            status_code = 409 if exc.code == ErrorCode.IDEMPOTENCY_KEY_REUSED else 400
            raise HTTPException(
                status_code=status_code,
                detail={"error_code": exc.code, "message": exc.message},
            )
        return _batch_dict(operation)

    @app.get("/admin/desired-batch/{batch_id}")
    def get_batch(batch_id: str, admin: Principal = Depends(admin_principal)) -> dict:
        try:
            operation = svc.get_batch(admin, batch_id)
        except DomainError as exc:
            raise HTTPException(
                status_code=404,
                detail={"error_code": exc.code, "message": exc.message},
            )
        initial = _batch_dict(operation)
        latest = svc.batch_device_status(admin, batch_id)
        initial["latest"] = [vars(r) for r in latest]
        return initial

    @app.get("/admin/alerts/{device_id}")
    def get_alerts(device_id: str, admin: Principal = Depends(admin_principal)) -> dict:
        return {
            "active": [vars(a) for a in svc.list_alerts(admin, device_id)],
            "history": [vars(a) for a in svc.alert_history(admin, device_id)],
        }

    @app.post("/devices/{device_id}/connection")
    def set_connection(
        device_id: str, body: ConnectIn, authorization: str | None = Header(default=None)
    ) -> dict:
        principal = _principal(device_id, authorization)
        twin = svc.connect(principal, device_id) if body.online else svc.disconnect(principal, device_id)
        return _twin_dict(twin, ack_deadline_ms=svc._twin(device_id).ack_deadline())

    @app.post("/devices/{device_id}/telemetry")
    def post_telemetry(
        device_id: str, body: TelemetryIn, authorization: str | None = Header(default=None)
    ) -> dict:
        principal = _principal(device_id, authorization)
        result = svc.ingest_telemetry(
            principal, device_id, body.seq, body.ts_ms, body.metrics,
            ack_desired_version=body.ack_desired_version,
        )
        if not result.accepted:
            raise HTTPException(
                status_code=422 if result.reject_reason in ("DUPLICATE_SAMPLE", "STALE_SAMPLE", "BAD_TELEMETRY") else 403,
                detail={"error_code": result.reject_reason, "message": result.detail},
            )
        return vars(result)

    @app.post("/devices/{device_id}/backfill")
    def post_backfill(
        device_id: str, body: BackfillIn, authorization: str | None = Header(default=None)
    ) -> dict:
        principal = _principal(device_id, authorization)
        from .models import TelemetrySample

        samples = [
            TelemetrySample(
                device_id=device_id, seq=s.seq, ts_ms=s.ts_ms, metrics=s.metrics
            )
            for s in body.samples
        ]
        results = svc.backfill(principal, device_id, samples, body.ack_desired_version)
        return {"results": [vars(r) for r in results]}

    @app.get("/devices/{device_id}/twin")
    def get_twin(device_id: str, authorization: str | None = Header(default=None)) -> dict:
        principal = _principal(device_id, authorization)
        twin_state = svc.get_twin(principal, device_id)
        return _twin_dict(twin_state, ack_deadline_ms=svc._twin(device_id).ack_deadline())

    return app


def _batch_dict(operation: Any) -> dict:
    return {
        "batch_id": operation.batch_id,
        "created_at_ms": operation.created_at_ms,
        "results": [vars(r) for r in operation.results],
    }


def _twin_dict(twin: Any, ack_deadline_ms: int | None = None) -> dict:
    return {
        "device_id": twin.device_id,
        "connection": twin.connection.value,
        "sync_status": twin.sync_status.value,
        "ack_deadline_ms": ack_deadline_ms,
        "desired": (
            {"version": twin.desired.version, "values": twin.desired.values,
             "updated_at_ms": twin.desired.updated_at_ms}
            if twin.desired else None
        ),
        "reported": (
            {"version": twin.reported.version, "values": twin.reported.values,
             "updated_at_ms": twin.reported.updated_at_ms, "last_seq": twin.reported.last_seq}
            if twin.reported else None
        ),
    }


app = create_app()
