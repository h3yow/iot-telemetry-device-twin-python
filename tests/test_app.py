"""HTTP 适配层本地冒烟测试（FastAPI TestClient，不绑定网络端口）。"""
from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from iot_twin.app import create_app

log = logging.getLogger("iot_twin.test.app")


def _client() -> TestClient:
    return TestClient(create_app())


def test_http_register_auth_telemetry_and_rejection_codes():
    client = _client()

    # 管理员注册设备
    r = client.post(
        "/admin/devices",
        json={"device_id": "dev-1", "credential": "secret"},
        headers={"Authorization": "Bearer admin"},
    )
    assert r.status_code == 200
    log.info("输入: 管理员注册 dev-1; 最终状态=%s", r.json())

    # 无凭证 -> 401 INVALID_CREDENTIAL
    r = client.post("/devices/dev-1/telemetry", json={"seq": 1, "ts_ms": 1, "metrics": {}})
    assert r.status_code == 401 and r.json()["detail"]["error_code"] == "INVALID_CREDENTIAL"

    # 未注册设备 -> 404 DEVICE_NOT_REGISTERED
    r = client.post(
        "/devices/ghost/telemetry",
        json={"seq": 1, "ts_ms": 1, "metrics": {}},
        headers={"Authorization": "Bearer x"},
    )
    assert r.status_code == 404 and r.json()["detail"]["error_code"] == "DEVICE_NOT_REGISTERED"
    log.info("输入: 上报未注册设备; 判定依据: 注册查询; 结果=404 DEVICE_NOT_REGISTERED")

    headers = {"Authorization": "Bearer secret"}

    # 上线 -> 正常遥测 -> 重复上报
    assert client.post("/devices/dev-1/connection", json={"online": True}, headers=headers).status_code == 200
    r = client.post(
        "/devices/dev-1/telemetry",
        json={"seq": 1, "ts_ms": 100, "metrics": {"temp": 42}},
        headers=headers,
    )
    assert r.status_code == 200 and r.json()["accepted"] is True
    r = client.post(
        "/devices/dev-1/telemetry",
        json={"seq": 1, "ts_ms": 100, "metrics": {"temp": 42}},
        headers=headers,
    )
    assert r.status_code == 422 and r.json()["detail"]["error_code"] == "DUPLICATE_SAMPLE"
    log.info("输入: 相同 seq 二次上报; 最终拒绝=%s", r.json()["detail"]["error_code"])

    # 管理员下发期望，设备确认执行
    r = client.post(
        "/admin/desired/dev-1",
        json={"values": {"led": "on"}, "merge": True},
        headers={"Authorization": "Bearer admin"},
    )
    assert r.status_code == 200
    r = client.post(
        "/devices/dev-1/telemetry",
        json={"seq": 2, "ts_ms": 200, "metrics": {"led": "on"}, "ack_desired_version": 1},
        headers=headers,
    )
    assert r.status_code == 200
    twin = client.get("/devices/dev-1/twin", headers=headers).json()
    assert twin["sync_status"] == "IN_SYNC"
    log.info("输入: 设备确认期望 v1; 最终孪生=%s", twin)
