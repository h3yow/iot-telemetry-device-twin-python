"""遥测汇聚。

并入规则（同时满足才会计入，任一不满足返回 False）：

1. ``ts_ms`` 不得早于已计入样本的最大时间戳（防乱序倒退）；相同时间戳
   再比较 ``seq``，``seq`` 也不更新则视为迟到重复；
2. 计入后按指标做 latest-wins：新样本携带的指标覆盖旧值，未携带的指标
   保留。重连后的批量补传按样本自身的 ``(ts_ms, seq)`` 排序后逐条并入，
   因此离线补传不会造成状态倒退。
"""
from __future__ import annotations

import threading

from .models import TelemetrySample


class TelemetryStore:
    def __init__(self) -> None:
        self._samples: list[TelemetrySample] = []
        self._metrics: dict = {}
        self._watermark_ms: int | None = None
        self._last_seq: int | None = None
        self._lock = threading.RLock()

    @property
    def watermark_ms(self) -> int:
        with self._lock:
            return self._watermark_ms if self._watermark_ms is not None else -1

    @property
    def last_seq(self) -> int:
        with self._lock:
            return self._last_seq if self._last_seq is not None else -1

    def is_acceptable(self, ts_ms: int, seq: int) -> bool:
        """该样本是否不会造成倒退（重复/过期返回 False）。"""
        with self._lock:
            if self._watermark_ms is None:
                return True
            if ts_ms > self._watermark_ms:
                return True
            if ts_ms == self._watermark_ms and seq > (self._last_seq or -1):
                return True
            return False

    def add(self, sample: TelemetrySample) -> bool:
        with self._lock:
            if not self.is_acceptable(sample.ts_ms, sample.seq):
                return False
            self._samples.append(sample)
            self._metrics.update(sample.metrics)
            self._watermark_ms = sample.ts_ms
            self._last_seq = sample.seq
            return True

    def add_sorted(self, samples: list[TelemetrySample]) -> list[bool]:
        """批量补传：按 (ts_ms, seq) 排序后逐条并入，返回各自是否计入。"""
        ordered = sorted(samples, key=lambda s: (s.ts_ms, s.seq))
        results: list[bool] = []
        for sample in ordered:
            results.append(self.add(sample))
        return results

    def latest_metrics(self) -> dict:
        with self._lock:
            return dict(self._metrics)

    def samples(self) -> list[TelemetrySample]:
        with self._lock:
            return sorted(self._samples, key=lambda s: (s.ts_ms, s.seq))
