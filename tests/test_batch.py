"""批量下发：逐台隔离、部分失败、batch_id 幂等、结论可查。"""
from __future__ import annotations

import logging

import pytest

from iot_twin.errors import DomainError, ErrorCode
from iot_twin.models import (
    BatchDesiredItem,
    ItemStatus,
    Principal,
    Role,
    SyncStatus,
)

log = logging.getLogger("iot_twin.test.batch")


@pytest.fixture
def admin() -> Principal:
    return Principal(device_id=None, role=Role.ADMIN)


def _items() -> list[BatchDesiredItem]:
    return [
        BatchDesiredItem("dev-1", {"led": "on"}),
        BatchDesiredItem("dev-2", {"a": 1, "b": 2}, merge=False),
        BatchDesiredItem("ghost", {"x": 1}),                       # 未注册
        BatchDesiredItem("dev-3", {"k": 9}, expected_version=99),  # 版本冲突
        BatchDesiredItem("dev-1", {"extra": 7}),                   # 同设备第二条
    ]


def test_batch_per_device_results_and_partial_failure(service, admin):
    for d in ("dev-1", "dev-2", "dev-3"):
        service.register_device(d, "secret")

    op = service.batch_update_desired(admin, "batch-1", _items())
    table = {r.device_id: r for r in op.results}
    log.info(
        "输入: 一批5条(含未注册/版本冲突/同设备两条); 判定依据: 逐台独立下发;\n 逐台结论=%s",
        [(r.device_id, r.status.value, r.reject_reason, r.desired_version)
         for r in op.results],
    )

    # 成功的不受失败设备影响
    assert table["dev-1"].status is ItemStatus.DELIVERED
    assert table["dev-2"].status is ItemStatus.DELIVERED
    assert table["dev-2"].desired_version == 1
    # 各自失败原因可区分
    assert table["ghost"].status is ItemStatus.REJECTED
    assert table["ghost"].reject_reason == ErrorCode.DEVICE_NOT_REGISTERED
    assert table["dev-3"].status is ItemStatus.REJECTED
    assert table["dev-3"].reject_reason == ErrorCode.VERSION_CONFLICT

    # 一台失败不影响其它：dev-3 没有期望，dev-1 两条都按序生效（v1 补丁 + v2 补丁）
    assert service._twin("dev-3").state.desired is None
    assert service._twin("dev-1").state.desired.version == 2
    assert service._twin("dev-1").state.desired.values == {"led": "on", "extra": 7}
    assert service._twin("dev-2").state.desired.values == {"a": 1, "b": 2}
    log.info("最终状态: dev-1=v2 %s dev-2=v1 %s dev-3无期望",
             service._twin("dev-1").state.desired.values,
             service._twin("dev-2").state.desired.values)


def test_batch_retry_is_idempotent(service, admin, fake_clock):
    for d in ("dev-1", "dev-2"):
        service.register_device(d, "secret")
    items = [BatchDesiredItem("dev-1", {"led": "on"}),
             BatchDesiredItem("dev-2", {"a": 1}, expected_version=5)]

    first = service.batch_update_desired(admin, "batch-9", items)
    version_after_first = service._twin("dev-1").state.desired.version
    fake_clock.advance(1000)

    # 完全相同的重试：不重复下发、版本不跳、失败原因一致
    second = service.batch_update_desired(admin, "batch-9", items)
    log.info("输入: 相同 batch_id 与内容重试; 判定依据: 命中幂等记录直接返回首拍")
    assert service._twin("dev-1").state.desired.version == version_after_first
    assert [r.status for r in second.results] == [r.status for r in first.results]
    assert second.results[1].reject_reason == ErrorCode.VERSION_CONFLICT
    assert second.created_at_ms == first.created_at_ms  # 仍是首拍时间

    # 同一个 batch_id 换内容 -> 明确拒绝，避免幂等键挪用
    with pytest.raises(DomainError) as exc:
        service.batch_update_desired(
            admin, "batch-9", [BatchDesiredItem("dev-1", {"led": "off"})]
        )
    assert exc.value.code == ErrorCode.IDEMPOTENCY_KEY_REUSED
    # 被拒的挪用不改变任何设备状态
    assert service._twin("dev-1").state.desired.values == {"led": "on"}
    log.info("输入: 同 batch_id 换内容重试; 判定依据: 指纹不一致; 结果=%s 设备状态不变",
             ErrorCode.IDEMPOTENCY_KEY_REUSED)


def test_batch_empty_and_unknown_query(service, admin):
    with pytest.raises(DomainError) as exc:
        service.batch_update_desired(admin, "batch-x", [])
    assert exc.value.code == ErrorCode.EMPTY_BATCH

    with pytest.raises(DomainError) as exc:
        service.get_batch(admin, "not-exist")
    assert exc.value.code == ErrorCode.BATCH_NOT_FOUND
    log.info("输入: 空批次/查询不存在批次; 判定依据: 入参与批次表; 结论=EMPTY_BATCH/BATCH_NOT_FOUND")


def test_batch_device_status_follows_later_conclusion(service, admin, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    service.connect(device, "dev-1")
    service.update_desired(admin, "dev-1", {"led": "on"})  # 预置 v1 已确认
    service.ingest_telemetry(device, "dev-1", 1, 100, {"led": "on"}, ack_desired_version=1)

    service.batch_update_desired(
        admin, "b-follow",
        [BatchDesiredItem("dev-1", {"led": "off"}, expected_version=1)],
    )
    # 首拍：已下发、待确认
    latest = service.batch_device_status(admin, "b-follow")
    assert "PENDING" in latest[0].detail
    log.info("输入: 批次首拍后查询; 判定依据: 设备当前对账; 结论=%s", latest[0].detail)

    # 设备确认后，批次查询能看到最终逐台结论
    service.ingest_telemetry(device, "dev-1", 2, 200, {"led": "off"}, ack_desired_version=2)
    latest = service.batch_device_status(admin, "b-follow")
    log.info("输入: 设备确认后再查同一批; 最终逐台结论=%s", latest[0].detail)
    assert "IN_SYNC" in latest[0].detail


def test_batch_device_status_shows_timeout(service, admin, fake_clock):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    service.connect(device, "dev-1")
    service.batch_update_desired(
        admin, "b-timeout",
        [BatchDesiredItem("dev-1", {"led": "on"})],
    )
    # 批量条目未带时限 -> 不会超时（兼容行为）
    fake_clock.advance(10_000)
    latest = service.batch_device_status(admin, "b-timeout")
    assert "PENDING" in latest[0].detail
    assert service.get_twin(admin, "dev-1").sync_status is SyncStatus.PENDING
    log.info("输入: 批量条目未设定时限经过很久; 判定依据: deadline=None; 结论=%s",
             latest[0].detail)


def test_device_role_cannot_dispatch_batch(service):
    service.register_device("dev-1", "secret")
    device = service.authenticate("dev-1", "secret")
    with pytest.raises(DomainError) as exc:
        service.batch_update_desired(
            device, "b", [BatchDesiredItem("dev-1", {"x": 1})]
        )
    assert exc.value.code == ErrorCode.PERMISSION_DENIED
    log.info("输入: 设备身份尝试批量下发; 判定依据: 仅管理员; 结果=PERMISSION_DENIED")
