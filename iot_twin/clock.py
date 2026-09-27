"""可注入时钟，保证测试结果确定。"""
from __future__ import annotations

import time


class Clock:
    def now_ms(self) -> int:
        raise NotImplementedError


class SystemClock(Clock):
    def now_ms(self) -> int:
        return int(time.time() * 1000)


class FakeClock(Clock):
    def __init__(self, start_ms: int = 0):
        self._now = start_ms

    def now_ms(self) -> int:
        return self._now

    def advance(self, delta_ms: int) -> int:
        self._now += delta_ms
        return self._now

    def set(self, value_ms: int) -> None:
        self._now = value_ms
