"""遥测幂等去重。

同一设备短时间窗口内相同 ``seq`` 的重复上报被判定为重复，不重复计入。
每个设备维护一个有界 FIFO 集合（默认 1024 个序号），超出后最旧序号
被淘汰，避免无界增长。
"""
from __future__ import annotations

import threading
from collections import defaultdict, deque


class DeduplicationWindow:
    def __init__(self, max_seqs_per_device: int = 1024) -> None:
        if max_seqs_per_device <= 0:
            raise ValueError("max_seqs_per_device 必须为正数")
        self._capacity = max_seqs_per_device
        self._seen: dict[str, deque[int]] = defaultdict(deque)
        self._sets: dict[str, set[int]] = defaultdict(set)
        self._lock = threading.RLock()

    def seen(self, device_id: str, seq: int) -> bool:
        with self._lock:
            return seq in self._sets[device_id]

    def remember(self, device_id: str, seq: int) -> None:
        with self._lock:
            if seq in self._sets[device_id]:
                return
            bucket = self._seen[device_id]
            bucket.append(seq)
            self._sets[device_id].add(seq)
            while len(bucket) > self._capacity:
                oldest = bucket.popleft()
                self._sets[device_id].discard(oldest)

    def check_and_remember(self, device_id: str, seq: int) -> bool:
        """返回 True 表示这是新样本（并已记录），False 表示重复样本。"""
        with self._lock:
            if seq in self._sets[device_id]:
                return False
            self.remember(device_id, seq)
            return True

    def reset(self, device_id: str | None = None) -> None:
        with self._lock:
            if device_id is None:
                self._seen.clear()
                self._sets.clear()
            else:
                self._seen.pop(device_id, None)
                self._sets.pop(device_id, None)
