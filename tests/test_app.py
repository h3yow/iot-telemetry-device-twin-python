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


def test_http_desired_with_timeout_revoke_and_batch_endpoints():
    client = _client()
    admin = {"Authorization": "Bearer admin"}

    client.post("/admin/devices", json={"device_id": "dev-1", "credential": "s1"}, headers=admin)
    client.post("/admin/devices", json={"device_id": "dev-2", "credential": "s2"}, headers=admin)
    h1 = {"Authorization": "Bearer s1"}
    client.post("/devices/dev-1/connection", json={"online": True}, headers=h1)

    # 下发带确认时限的期望（时钟为系统时钟，只验证字段透传与状态结构）
    r = client.post(
        "/admin/desired/dev-1",
        json={"values": {"led": "on"}, "ack_timeout_ms": 500},
        headers=admin,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["desired"]["ack_timeout_ms"] == 500
    assert body["desired"]["deadline_ms"] is not None
    assert body["sync_status"] == "PENDING"
    log.info("输入: HTTP 下发带时限期望; 结论=%s", body["sync_status"])

    # 批量下发：dev-1 成功、ghost 未注册被拒（互不影响），查询逐台结论
    r = client.post(
        "/admin/desired-batch",
        json={"idempotency_key": "k-1", "items": [
            {"device_id": "dev-1", "values": {"led": "blink"}},
            {"device_id": "ghost", "values": {"led": "on"}},
        ]},
        headers=admin,
    )
    assert r.status_code == 200
    batch = r.json()
    by_dev = {i["device_id"]: i for i in batch["items"]}
    assert by_dev["dev-1"]["status"] == "DISPATCHED"
    assert by_dev["ghost"]["status"] == "REJECTED"
    assert by_dev["ghost"]["reject_reason"] == "DEVICE_NOT_REGISTERED"
    log.info("输入: HTTP 批量含坏设备; 逐台=%s",
             [(d, i["status"], i["reject_reason"]) for d, i in by_dev.items()])

    # 幂等重试：同键返回同 batch_id，版本不乱跳
    r2 = client.post(
        "/admin/desired-batch",
        json={"idempotency_key": "k-1", "items": [{"device_id": "dev-2", "values": {"x": 1}}]},
        headers=admin,
    )
    assert r2.json()["batch_id"] == batch["batch_id"]

    # 批次查询
    got = client.get(f"/admin/desired-batch/{batch['batch_id']}", headers=admin)
    assert got.status_code == 200 and len(got.json()["items"]) == 2
    missing = client.get("/admin/desired-batch/nope", headers=admin)
    assert missing.status_code == 404
    assert missing.json()["detail"]["error_code"] == "BATCH_NOT_FOUND"

    # 撤销一个尚未确认的期望
    r = client.post("/admin/desired/dev-2", json={"values": {"led": "on"}}, headers=admin)
    rev = client.post("/admin/desired/dev-2/revoke", json={}, headers=admin)
    assert rev.status_code == 200
    assert rev.json()["desired"] is None
    log.info("输入: HTTP 撤销 dev-2 首次未确认期望; 最终 desired=%s", rev.json()["desired"])

    # 空批次 -> 400 INVALID_REQUEST
    r = client.post("/admin/desired-batch", json={"items": []}, headers=admin)
    assert r.status_code == 400 and r.json()["detail"]["error_code"] == "INVALID_REQUEST"
