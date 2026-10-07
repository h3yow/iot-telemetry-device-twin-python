"""批量下发：逐台独立结果、部分失败隔离、幂等重试、逐台结论查询。"""
from __future__ import annotations

import logging
import threading

import pytest

from iot_twin.errors import ErrorCode
from iot_twin.models import (
    BatchDesiredItem,
    BatchItemStatus,
    Principal,
    Role,
    SyncStatus,
)
from iot_twin.service import IoTService

log = logging.getLogger("iot_twin.test.batch")


@pytest.fixture
def admin() -> Principal:
    return Principal(device_id=None, role=Role.ADMIN)


def _register(service: IoTService, *ids: str) -> None:
    for i, device_id in enumerate(ids):
        service.register_device(device_id, f"secret-{i}")


def test_batch_partial_failure_isolated_per_device(
    service: IoTService, admin: Principal
):
    _register(service, "dev-1", "dev-2")
    items = [
        BatchDesiredItem("dev-1", {"led": "on"}),
        BatchDesiredItem("ghost", {"led": "on"}),       # 未注册 -> 拒绝
        BatchDesiredItem("dev-2", {"led": "off"},
                         expected_version=99),           # 版本不符 -> 拒绝
    ]
    record = service.dispatch_batch(admin, items, batch_id="b1")

    by_id = {i.device_id: i for i in record.items}
    log.info("输入: 三台(含未注册/版本错误); 判定依据: 逐台独立; 结果=%s",
             [(d, i.status.value, i.reject_reason) for d, i in by_id.items()])

    assert by_id["dev-1"].status is BatchItemStatus.DISPATCHED
    assert by_id["dev-1"].desired_version == 1
    assert by_id["ghost"].status is BatchItemStatus.REJECTED
    assert by_id["ghost"].reject_reason == ErrorCode.DEVICE_NOT_REGISTERED
    assert by_id["dev-2"].status is BatchItemStatus.REJECTED
    assert by_id["dev-2"].reject_reason == ErrorCode.VERSION_CONFLICT

    # 一台失败不影响其它：dev-1 确实生效，dev-2 没有产生版本
    assert service._twin("dev-1").state.desired.version == 1
    assert service._twin("dev-2").state.desired is None


def test_batch_retry_with_same_idempotency_key_no_duplicate_dispatch(
    service: IoTService, admin: Principal
):
    _register(service, "dev-1", "dev-2")
    items = [
        BatchDesiredItem("dev-1", {"led": "on"}),
        BatchDesiredItem("dev-2", {"led": "off"}),
    ]
    first = service.dispatch_batch(admin, items, idempotency_key="idem-9")
    second = service.dispatch_batch(
        admin, [BatchDesiredItem("dev-1", {"led": "DIFFERENT"})],
        idempotency_key="idem-9",
    )

    log.info("输入: 同幂等键重复提交(内容不同); 判定依据: 直接返回首次批次; "
             "batch=%s/%s 版本=%s/%s",
             first.batch_id, second.batch_id,
             service._twin("dev-1").state.desired.version,
             service._twin("dev-2").state.desired.version)

    assert second.batch_id == first.batch_id
    assert len(second.items) == 2  # 仍是首次的两台、首次的结论
    assert second.result_for("dev-1").status is BatchItemStatus.DISPATCHED
    # 没有重复下发：两台版本仍停在 v1，第二次的值没有落进去
    assert service._twin("dev-1").state.desired.version == 1
    assert service._twin("dev-1").state.desired.values == {"led": "on"}
    assert service._twin("dev-2").state.desired.version == 1


def test_batch_query_returns_per_device_conclusion(
    service: IoTService, admin: Principal, fake_clock
):
    _register(service, "dev-1", "dev-2")
    device = service.authenticate("dev-1", "secret-0")
    service.connect(device, "dev-1")

    items = [
        BatchDesiredItem("dev-1", {"led": "on"}, ack_timeout_ms=100),
        BatchDesiredItem("dev-2", {"led": "on"}),
    ]
    record = service.dispatch_batch(admin, items, batch_id="bq1")
    assert {i.current_sync_status for i in record.items} == {SyncStatus.PENDING.value}

    # dev-1 正确确认 -> IN_SYNC；dev-2 不动 -> PENDING
    service.ingest_telemetry(
        device, "dev-1", 1, fake_clock.now_ms(), {"led": "on"},
        ack_desired_version=1,
    )
    queried = service.get_batch(admin, "bq1")
    status = {i.device_id: i.current_sync_status for i in queried.items}
    log.info("输入: dev-1 确认, dev-2 未动; 判定依据: 查询回填当前对账; 逐台=%s", status)
    assert status["dev-1"] == SyncStatus.IN_SYNC.value
    assert status["dev-2"] == SyncStatus.PENDING.value

    # dev-1 超时后查询也应反映纠正/超时演变（这里 dev-1 已 IN_SYNC，不受超时影响）
    fake_clock.advance(200)
    queried2 = service.get_batch(admin, "bq1")
    status2 = {i.device_id: i.current_sync_status for i in queried2.items}
    assert status2["dev-1"] == SyncStatus.IN_SYNC.value  # 已确认不会被超时覆盖


def test_batch_rejects_empty_and_duplicate_devices(
    service: IoTService, admin: Principal
):
    with pytest.raises(Exception) as exc:
        service.dispatch_batch(admin, [])
    assert exc.value.code == ErrorCode.INVALID_REQUEST

    _register(service, "dev-1")
    with pytest.raises(Exception) as exc:
        service.dispatch_batch(
            admin,
            [BatchDesiredItem("dev-1", {"a": 1}),
             BatchDesiredItem("dev-1", {"a": 2})],
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST
    log.info("输入: 空批次/批次内重复设备; 判定依据: 整批合法性; 错误=%s", exc.value.code)
    # 被整批拒绝后不应有任何下发
    assert service._twin("dev-1").state.desired is None


def test_batch_permission_denied_for_device_role(service: IoTService):
    _register(service, "dev-1")
    device = service.authenticate("dev-1", "secret-0")
    with pytest.raises(Exception) as exc:
        service.dispatch_batch(device, [BatchDesiredItem("dev-1", {"a": 1})])
    assert exc.value.code == ErrorCode.PERMISSION_DENIED


def test_concurrent_batch_same_idempotency_key_dispatches_once(
    service: IoTService, admin: Principal
):
    n = 20
    _register(service, *(f"dev-{i}" for i in range(n)))
    items = [BatchDesiredItem(f"dev-{i}", {"k": i}) for i in range(n)]

    n_threads = 8
    barrier = threading.Barrier(n_threads)
    batch_ids: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        barrier.wait()
        record = service.dispatch_batch(admin, items, idempotency_key="race-key")
        with lock:
            batch_ids.append(record.batch_id)

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log.info("输入: %s 线程并发同幂等键提交; 判定依据: 批次锁串行+幂等; 唯一批次=%s",
             n_threads, set(batch_ids))
    assert len(set(batch_ids)) == 1
    # 每台只被下发一次：版本统一停在 v1
    assert {
        service._twin(f"dev-{i}").state.desired.version for i in range(n)
    } == {1}
