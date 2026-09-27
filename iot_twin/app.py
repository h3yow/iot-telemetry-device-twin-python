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
        )
        return _twin_dict(twin)

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
        return _twin_dict(twin)

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
        return _twin_dict(svc.get_twin(principal, device_id))

    return app


def _twin_dict(twin: Any) -> dict:
    return {
        "device_id": twin.device_id,
        "connection": twin.connection.value,
        "sync_status": twin.sync_status.value,
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
