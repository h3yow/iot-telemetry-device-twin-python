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


def test_http_batch_dispatch_partial_failure_and_idempotent_retry():
    client = _client()
    admin = {"Authorization": "Bearer admin"}
    for d in ("dev-1", "dev-2"):
        assert client.post("/admin/devices", json={"device_id": d, "credential": "s"},
                           headers=admin).status_code == 200

    payload = {
        "batch_id": "b-http-1",
        "items": [
            {"device_id": "dev-1", "values": {"led": "on"}},
            {"device_id": "ghost", "values": {"x": 1}},
            {"device_id": "dev-2", "values": {"a": 1}, "expected_version": 9},
        ],
    }
    r = client.post("/admin/desired-batch", json=payload, headers=admin)
    assert r.status_code == 200
    results = r.json()["results"]
    log.info("输入: HTTP 批量3台; 判定依据: 逐台独立; 逐台=%s",
             [(x["device_id"], x["status"], x["reject_reason"]) for x in results])
    assert results[0]["status"] == "DELIVERED"
    assert results[1]["reject_reason"] == "DEVICE_NOT_REGISTERED"
    assert results[2]["reject_reason"] == "VERSION_CONFLICT"

    # 相同请求重试 -> 仍是 200 且返回首拍结果（版本不重复跳）
    r2 = client.post("/admin/desired-batch", json=payload, headers=admin)
    assert r2.status_code == 200
    assert r2.json()["created_at_ms"] == r.json()["created_at_ms"]
    twin = client.get("/devices/dev-1/twin", headers={"Authorization": "Bearer s"}).json()
    assert twin["desired"]["version"] == 1

    # 结论查询
    r3 = client.get("/admin/desired-batch/b-http-1", headers=admin)
    assert r3.status_code == 200 and len(r3.json()["results"]) == 3
    assert r3.json()["latest"][0]["status"] == "DELIVERED"
    log.info("输入: 查询批次 b-http-1; 最终逐台=%s",
             [(x["device_id"], x.get("detail")) for x in r3.json()["latest"]])

    r4 = client.get("/admin/desired-batch/missing", headers=admin)
    assert r4.status_code == 404 and r4.json()["detail"]["error_code"] == "BATCH_NOT_FOUND"


def test_http_rollback_confirmed_and_unconfirmed():
    client = _client()
    admin = {"Authorization": "Bearer admin"}
    device = {"Authorization": "Bearer s"}
    client.post("/admin/devices", json={"device_id": "dev-9", "credential": "s"}, headers=admin)
    client.post("/devices/dev-9/connection", json={"online": True}, headers=device)

    # v1 下发并被确认
    assert client.post("/admin/desired/dev-9", json={"values": {"led": "on"}},
                       headers=admin).status_code == 200
    client.post("/devices/dev-9/telemetry",
                json={"seq": 1, "ts_ms": 100, "metrics": {"led": "on"},
                      "ack_desired_version": 1},
                headers=device)
    # 已确认的期望不允许撤销 -> 409 ALREADY_CONFIRMED
    r = client.post("/admin/desired/dev-9/rollback", json={"version": 1}, headers=admin)
    assert r.status_code == 409 and r.json()["detail"]["error_code"] == "ALREADY_CONFIRMED"
    log.info("输入: HTTP 撤销已确认 v1; 判定依据: IN_SYNC; 结果=%s",
             r.json()["detail"]["error_code"])

    # v2 未确认 -> 可撤销
    client.post("/admin/desired/dev-9", json={"values": {"led": "off"},
                                              "expected_version": 1}, headers=admin)
    r = client.post("/admin/desired/dev-9/rollback", json={"version": 2}, headers=admin)
    assert r.status_code == 200 and r.json()["rolled_back"] is True
    assert r.json()["desired_version"] == 3
    # 重复撤销幂等
    r2 = client.post("/admin/desired/dev-9/rollback", json={"version": 2}, headers=admin)
    assert r2.status_code == 200 and r2.json()["idempotent"] is True
    assert r2.json()["desired_version"] == 3
    twin = client.get("/devices/dev-9/twin", headers=device).json()
    assert twin["desired"]["values"] == {"led": "on"}
    log.info("输入: HTTP 撤销 v2 后重复撤销; 最终孪生=%s", twin)
