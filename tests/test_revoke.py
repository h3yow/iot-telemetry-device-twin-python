"""撤销未确认期望：回退到最后确认取值、幂等、并发安全、已确认不可撤销。"""
from __future__ import annotations

import logging
import threading

import pytest

from iot_twin.errors import DomainError, ErrorCode
from iot_twin.models import SyncStatus
from iot_twin.twin import DeviceTwin

log = logging.getLogger("iot_twin.test.revoke")


def test_revoke_returns_to_last_confirmed_values():
    twin = DeviceTwin("dev-1")
    twin.connect(0)
    # v1: led=on 被设备确认生效
    twin.update_desired({"led": "on"}, None, 0)
    twin.apply_reported({"led": "on"}, 1, 10, 10, ack_desired_version=1)
    assert twin.state.sync_status is SyncStatus.IN_SYNC

    # v2: 想改成 blink，设备尚未确认 -> 撤销，回到 v1 的 on
    twin.update_desired({"led": "blink"}, 1, 20)
    assert twin.state.desired.version == 2
    twin.revoke_desired(now_ms=30)
    log.info("输入: v2=blink 未确认即撤销; 判定依据: 回到最后确认基线 v1; 结果 v=%s values=%s",
             twin.state.desired.version, twin.state.desired.values)
    assert twin.state.desired.values == {"led": "on"}
    assert twin.state.desired.version == 3  # 版本仍单调递增，绝不回退到 1
    assert twin.state.desired.revoke_of_version == 2
    assert twin.state.sync_status is SyncStatus.PENDING

    # 设备确认撤销版（执行回 on）-> IN_SYNC
    _, status, _ = twin.apply_reported(
        {"led": "on"}, 2, 40, 40, ack_desired_version=3
    )
    assert status is SyncStatus.IN_SYNC
    log.info("输入: 设备 ack 撤销版 v3 且 led=on; 最终结论=%s", status.value)


def test_revoke_is_idempotent():
    twin = DeviceTwin("dev-1")
    twin.connect(0)
    twin.update_desired({"a": 1}, None, 0)
    twin.apply_reported({"a": 1}, 1, 5, 5, ack_desired_version=1)
    twin.update_desired({"a": 2}, 1, 10)

    first = twin.revoke_desired(20)
    version_after_first = first.desired.version
    second = twin.revoke_desired(30)
    third = twin.revoke_desired(40)
    log.info("输入: 连续撤销三次; 判定依据: 撤销版幂等; 版本序列=%s",
             [s.desired.version for s in (first, second, third)])
    assert second.desired.version == version_after_first
    assert third.desired.version == version_after_first
    assert third.desired.revoke_of_version == 2


def test_confirmed_desired_cannot_be_revoked_must_override():
    twin = DeviceTwin("dev-1")
    twin.connect(0)
    twin.update_desired({"led": "on"}, None, 0)
    twin.apply_reported({"led": "on"}, 1, 5, 5, ack_desired_version=1)
    assert twin.state.sync_status is SyncStatus.IN_SYNC

    with pytest.raises(DomainError) as exc:
        twin.revoke_desired(10)
    log.info("输入: 撤销已确认的 v1; 判定依据: 已生效不可撤销; 错误=%s", exc.value.code)
    assert exc.value.code == ErrorCode.ALREADY_CONFIRMED

    # 只能走新一次下发覆盖
    snap = twin.update_desired({"led": "off"}, 1, 20)
    assert snap.desired.version == 2 and snap.desired.values == {"led": "off"}


def test_revoke_first_ever_unconfirmed_dispatch_clears_desired():
    """从未确认过任何期望时撤销 -> 清空，之后可正常重新下发。"""
    twin = DeviceTwin("dev-1")
    twin.connect(0)
    twin.update_desired({"led": "on"}, None, 0)
    twin.revoke_desired(10)
    assert twin.state.desired is None
    assert twin.state.sync_status is SyncStatus.PENDING
    snap = twin.update_desired({"led": "blink"}, None, 20)
    # 撤销清空也消费了 v2，版本单调不重用 -> 重新下发为 v3
    assert snap.desired.version == 3 and snap.desired.values == {"led": "blink"}
    log.info("输入: 撤销首次未确认下发后重新下发; 最终版本=v%s", snap.desired.version)


def test_revoke_concurrent_with_new_dispatch_no_lost_update():
    """撤销与新下发并发：乐观锁保证一个成功、另一个 VERSION_CONFLICT，无版本错乱。"""
    twin = DeviceTwin("dev-1")
    twin.connect(0)
    twin.update_desired({"k": 1}, None, 0)
    twin.apply_reported({"k": 1}, 1, 5, 5, ack_desired_version=1)
    twin.update_desired({"k": 2}, 1, 10)  # v2 未确认

    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def revoke() -> None:
        barrier.wait()
        try:
            twin.revoke_desired(20, expected_version=2)
            outcomes.append("revoke-ok")
        except DomainError as exc:
            outcomes.append(exc.code)

    def dispatch() -> None:
        barrier.wait()
        try:
            twin.update_desired({"k": 3}, 2, 20)
            outcomes.append("dispatch-ok")
        except DomainError as exc:
            outcomes.append(exc.code)

    t1 = threading.Thread(target=revoke)
    t2 = threading.Thread(target=dispatch)
    t1.start(); t2.start(); t1.join(); t2.join()

    log.info("输入: 撤销与下发并发, 均期望 v2; 判定依据: 乐观锁串行; 结果=%s 最终版本=v%s",
             sorted(outcomes), twin.state.desired.version)
    assert ErrorCode.VERSION_CONFLICT in outcomes
    assert twin.state.desired.version == 3  # 恰好消费一个新版本，不乱跳


def test_device_ack_racing_revoke_does_not_regress_version():
    """设备确认与撤销并发：串行化后版本单调，取值不无故回退。"""
    twin = DeviceTwin("dev-1")
    twin.connect(0)
    twin.update_desired({"k": 1}, None, 0)
    twin.apply_reported({"k": 1}, 1, 5, 5, ack_desired_version=1)
    twin.update_desired({"k": 2}, 1, 10)  # v2 未确认

    errors: list[str] = []

    def ack() -> None:
        try:
            twin.apply_reported({"k": 2}, 2, 15, 15, ack_desired_version=2)
        except DomainError as exc:  # 理论上不抛，保险记录
            errors.append(exc.code)

    def revoke() -> None:
        try:
            twin.revoke_desired(20)
        except DomainError as exc:
            errors.append(exc.code)

    t1 = threading.Thread(target=ack)
    t2 = threading.Thread(target=revoke)
    t1.start(); t2.start(); t1.join(); t2.join()

    log.info("输入: 设备确认 v2 与撤销并发; 撤销结果=%s 最终 desired v=%s values=%s 同步=%s",
             errors, twin.state.desired.version, twin.state.desired.values,
             twin.state.sync_status.value)
    # 串行化后只有两种确定结局，任一都不允许版本回退/跳号：
    if errors == ["ALREADY_CONFIRMED"]:
        # 设备确认先赢：v2 已生效，撤销被拒，状态停在 v2/IN_SYNC
        assert twin.state.desired.version == 2
        assert twin.state.desired.values == {"k": 2}
        assert twin.state.sync_status is SyncStatus.IN_SYNC
    else:
        # 撤销先赢：产生撤销版 v3 回到基线 k=1；版本不重用、取值不无故回退
        assert errors == []
        assert twin.state.desired.version == 3
        assert twin.state.desired.revoke_of_version == 2
        assert twin.state.desired.values == {"k": 1}
