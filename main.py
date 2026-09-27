"""本地 HTTP 接入层：将 IoTService 暴露为 REST API（仅本地回环，无外部依赖）。"""
from typing import Any

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from iot_service import IoTService
from iot_service.models import TelemetryMessage

app = FastAPI(title="IoT Telemetry & Device Twin (local)")
service = IoTService()
service.rules.add_threshold_rule("high-temp", "temp", ">", 30.0, "温度过高")
service.rules.add_connection_rule("device-offline", "设备离线")


class RegisterReq(BaseModel):
    device_id: str
    token: str
    scopes: list[str] | None = None


class ConnectReq(BaseModel):
    device_id: str
    token: str


class TelemetryReq(BaseModel):
    device_id: str
    token: str
    message_id: str
    seq: int
    payload: dict[str, Any]


class DesiredReq(BaseModel):
    patch: dict[str, Any]
    base_version: int


@app.post("/devices")
def register(req: RegisterReq):
    d = service.register_device(req.device_id, req.token, set(req.scopes) if req.scopes else None)
    return {"device_id": d.device_id, "scopes": sorted(d.scopes)}


@app.post("/connect")
def connect(req: ConnectReq):
    r = service.connect(req.device_id, req.token)
    return {"ok": r.ok, "reason": r.reason,
            "sync": None if r.sync is None else {"conflict": r.sync.conflict, "detail": r.sync.detail},
            "alerts": [(a.rule_id, a.status) for a in r.alerts]}


@app.post("/disconnect/{device_id}")
def disconnect(device_id: str):
    return {"alerts": [(a.rule_id, a.status) for a in service.disconnect(device_id)]}


@app.post("/telemetry")
def telemetry(req: TelemetryReq):
    r = service.ingest(req.device_id, req.token, TelemetryMessage(
        device_id=req.device_id, payload=req.payload, message_id=req.message_id, seq=req.seq))
    return {"accepted": r.accepted, "reason": r.reason, "applied_seq": r.applied_seq,
            "alerts": [(a.rule_id, a.status) for a in r.alerts]}


@app.post("/devices/{device_id}/desired")
def set_desired(device_id: str, req: DesiredReq):
    r = service.set_desired(device_id, req.patch, req.base_version)
    return {"applied": r.applied, "reason": r.reason, "version": r.version, "detail": r.detail}


@app.get("/devices/{device_id}/twin")
def get_twin(device_id: str):
    t = service.get_twin(device_id)
    return {"desired": t.desired, "reported": t.reported,
            "desired_version": t.desired_version, "reported_version": t.reported_version}


@app.get("/alerts")
def alerts(device_id: str | None = None):
    return [{"device_id": a.device_id, "rule_id": a.rule_id, "status": a.status,
             "message": a.message} for a in service.rules.active_alerts(device_id)]


@app.get("/rule-errors")
def rule_errors():
    return [{"rule_id": e.rule_id, "device_id": e.device_id,
             "error_type": e.error_type, "detail": e.detail} for e in service.rules.errors()]


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
