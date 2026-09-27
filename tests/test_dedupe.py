"""去重窗口测试。"""
from __future__ import annotations

import logging

from iot_twin.dedupe import DeduplicationWindow

log = logging.getLogger("iot_twin.test.dedupe")


def test_first_seen_accepted_duplicate_rejected():
    window = DeduplicationWindow(max_seqs_per_device=3)
    for seq in (1, 2, 3):
        assert window.check_and_remember("dev-1", seq) is True
    log.info("输入: seq=1,2,3; 判定依据: 均首次出现; 最终状态: 全部接受")

    assert window.check_and_remember("dev-1", 2) is False
    assert window.seen("dev-1", 2) is True
    log.info("输入: seq=2 重放; 判定依据: 已在窗口内; 最终状态: 拒绝为重复")


def test_devices_are_isolated_and_window_evicts_oldest():
    window = DeduplicationWindow(max_seqs_per_device=2)
    assert window.check_and_remember("dev-1", 1) is True
    assert window.check_and_remember("dev-2", 1) is True
    log.info("输入: 两台设备各自 seq=1; 判定依据: 设备间隔离; 最终状态: 均接受")

    # 容量 2：写入 seq=2,3 后 seq=1 被淘汰
    window.remember("dev-1", 2)
    window.remember("dev-1", 3)
    assert window.seen("dev-1", 1) is False
    assert window.check_and_remember("dev-1", 1) is True
    log.info("输入: seq=1 在窗口淘汰后重放; 判定依据: FIFO 淘汰; 最终状态: 视为新样本")
