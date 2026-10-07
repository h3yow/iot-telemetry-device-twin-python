"""撤销未确认期望：回退到上一次已确认取值、幂等、并发不乱。"""
from __future__ import annotations

import logging
import threading

import pytest

from iot_twin.errors import DomainError, ErrorCode
from iot_twin.models import Principal, Role, SyncStatus
from iot_twin.twin import DeviceTwin

log = logging.getLogger("iot_twin.test.rollback")


def test_rollback_restores_last_confirmed_values():
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.update_desired({"led": "on", "pow": 1}, None, 10)  # v1
    twin.apply_reported({"led": "on", "pow": 1}, 1, 11, 11, ack_desired_version=1)
    assert twin.state.sync_status is SyncStatus.IN_SYNC
    twin.update_desired({"led": "off"}, 1, 20)  # v2 未确认
    log.info("输入: v1 已确认后下发 v2(led=off)未确认; 判定依据: v2 仍 PENDING")

    outcome = twin.rollback_desired(21, version=2)
    log.info("输入: 撤销 v2; 判定依据: %s; 最终期望=%s",
             outcome.detail,
             (twin.state.desired.version, twin.state.desired.values))
    assert outcome.rolled_back and not outcome.idempotent
    # 回退另起一版（不回收 v2），取值为上一次已确认的 v1 全量取值
    assert twin.state.desired.version == 3
    assert twin.state.desired.values == {"led": "on", "pow": 1}
    # 设备上报仍是 on/1 -> 回退后重新对账为 IN_SYNC
    assert twin.state.sync_status is SyncStatus.IN_SYNC


def test_rollback_is_idempotent():
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.update_desired({"led": "on"}, None, 10)
    twin.apply_reported({"led": "on"}, 1, 11, 11, ack_desired_version=1)
    twin.update_desired({"led": "off"}, 1, 20)  # v2 pending

    first = twin.rollback_desired(21, version=2)
    second = twin.rollback_desired(22, version=2)
    third = twin.rollback_desired(23, version=2)
    log.info("输入: 对 v2 连续撤销3次; 判定依据: 已撤销版本表; 结果版本=%s 幂等标记=%s/%s",
             twin.state.desired.version, second.idempotent, third.idempotent)
    assert first.rolled_back and not first.idempotent
    assert second.idempotent and third.idempotent
    # 不多出一版、不重复回退
    assert twin.state.desired.version == 3
    assert twin.state.desired.values == {"led": "on"}


def test_confirmed_desired_cannot_be_rolled_back():
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.update_desired({"led": "on"}, None, 10)
    twin.apply_reported({"led": "on"}, 1, 11, 11, ack_desired_version=1)
    log.info("输入: 撤销已确认生效的 v1; 判定依据: IN_SYNC 不允许撤销")
    with pytest.raises(DomainError) as exc:
        twin.rollback_desired(12, version=1)
    assert exc.value.code == ErrorCode.ALREADY_CONFIRMED
    assert twin.state.desired.version == 1


def test_rollback_without_any_confirmed_history_clears_desired():
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.update_desired({"a": 1}, None, 10)  # v1 从未被确认
    outcome = twin.rollback_desired(11)
    assert outcome.rolled_back and twin.state.desired is None
    log.info("输入: 撤销从未确认过的 v1; 判定依据: 无已确认快照; 最终期望=None")

    # 再次撤销（已无挂起期望）-> 幂等的空操作，不报错也不加版本
    again = twin.rollback_desired(12)
    assert not again.rolled_back
    assert twin.state.desired is None


def test_rollback_superseded_version_conflicts():
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.update_desired({"a": 1}, None, 10)  # v1
    twin.update_desired({"a": 2}, 1, 11)     # v2 取代 v1
    log.info("输入: 撤销已被 v2 取代的 v1; 判定依据: 当前挂起版本不匹配")
    with pytest.raises(DomainError) as exc:
        twin.rollback_desired(12, version=1)
    assert exc.value.code == ErrorCode.VERSION_CONFLICT
    assert twin.state.desired.version == 2
    assert twin.state.desired.values == {"a": 2}


def test_rollback_concurrent_with_new_dispatch_no_lost_version():
    """撤销与新下发并发：两者都在孪生锁内串行，版本必须连续不乱跳。

    两种合法交错：
    * 撤销先到：v2 -> 回退产生 v3，新下发乐观版本失败后重读重试 -> v4；
    * 新下发先到：v2 -> v3，撤销 v2 因"已不再挂起"被 VERSION_CONFLICT 拒绝。
    无论哪种，都不允许丢失更新、版本错乱或值无故回退。
    """
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.update_desired({"led": "on"}, None, 0)       # v1
    twin.apply_reported({"led": "on"}, 1, 1, 1, ack_desired_version=1)
    twin.update_desired({"led": "off"}, 1, 2)         # v2 未确认

    notes: list[str] = []
    barrier = threading.Barrier(2)

    def do_rollback() -> None:
        barrier.wait()
        try:
            twin.rollback_desired(3, version=2)
            notes.append("rollback-ok")
        except DomainError as exc:
            # 新下发先到时撤销被拒绝：合法交错
            assert exc.code == ErrorCode.VERSION_CONFLICT
            notes.append("rollback-rejected")

    def do_dispatch() -> None:
        barrier.wait()
        for attempt in range(10):
            cur = twin.state.desired.version if twin.state.desired else 0
            try:
                twin.update_desired({"led": "blink"}, expected_version=cur, now_ms=4)
                notes.append(f"dispatch-ok-v{cur + 1}")
                return
            except DomainError as exc:
                assert exc.code == ErrorCode.VERSION_CONFLICT
        notes.append("dispatch-failed")

    t1 = threading.Thread(target=do_rollback)
    t2 = threading.Thread(target=do_dispatch)
    t1.start(); t2.start(); t1.join(); t2.join()

    final = twin.state.desired
    log.info(
        "输入: 撤销 v2 与新下发并发; 判定依据: 孪生锁串行+乐观锁; 过程=%s 最终=v%s %s",
        notes, final.version, final.values,
    )
    assert "dispatch-failed" not in notes
    # 新下发必然生效（或在撤销之后重试生效），最终值不丢、版本连续
    assert final.values == {"led": "blink"}
    assert final.version in (3, 4)
    if "rollback-ok" in notes:
        assert final.version == 4  # 回退 v3 + 新下发 v4
    else:
        assert final.version == 3  # 新下发 v3，撤销被拒绝


def test_rollback_concurrent_with_device_ack_keeps_value_stable():
    """撤销与设备确认并发：任一顺序都不允许取值无故回退或版本错乱。"""
    outcomes = []
    for order in ("rollback_first", "ack_first"):
        twin = DeviceTwin("dev-1")
        twin.connect(now_ms=0)
        twin.update_desired({"led": "on"}, None, 0)
        twin.apply_reported({"led": "on"}, 1, 1, 1, ack_desired_version=1)  # v1 confirmed
        twin.update_desired({"led": "off"}, 1, 2)  # v2 pending

        if order == "rollback_first":
            twin.rollback_desired(3, version=2)
            # 撤销后设备迟到确认 v2（off）：版本已落后，不能把回退值冲掉
            _, status, _ = twin.apply_reported(
                {"led": "off"}, 2, 4, 4, ack_desired_version=2
            )
            assert twin.state.desired.values == {"led": "on"}
            assert twin.state.desired.version == 3
        else:
            # 设备先确认 v2 -> 已确认生效，撤销必须被拒绝，值不能无故回退
            _, status, _ = twin.apply_reported(
                {"led": "off"}, 2, 3, 3, ack_desired_version=2
            )
            assert status is SyncStatus.IN_SYNC
            with pytest.raises(DomainError) as exc:
                twin.rollback_desired(4, version=2)
            assert exc.value.code == ErrorCode.ALREADY_CONFIRMED
            assert twin.state.desired.values == {"led": "off"}
            assert twin.state.desired.version == 2
        outcomes.append((order, twin.state.desired.version, twin.state.desired.values))

    log.info("输入: 撤销与设备确认两种到达顺序; 判定依据: 锁内串行; 结果=%s", outcomes)


def test_rollback_clears_ack_timer():
    twin = DeviceTwin("dev-1")
    twin.connect(now_ms=0)
    twin.update_desired({"led": "on"}, None, 10, ack_timeout_ms=100)
    assert twin.ack_deadline() == 110
    twin.rollback_desired(20)  # 无已确认历史 -> 清空期望
    assert twin.ack_deadline() is None
    assert twin.state.desired is None
    assert twin.evaluate_ack_deadline(10_000) is SyncStatus.PENDING
    log.info("输入: 撤销带时限的未确认期望; 判定依据: 撤销同时停表; 最终无超时结论")
