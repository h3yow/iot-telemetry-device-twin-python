"""pytest 公共夹具与日志：单测中打印输入、判定依据与最终状态。"""
from __future__ import annotations

import logging
import sys

import pytest

from iot_twin.clock import FakeClock
from iot_twin.service import IoTService


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock(start_ms=1_000_000)


@pytest.fixture
def service(fake_clock: FakeClock) -> IoTService:
    return IoTService(clock=fake_clock)


def pytest_configure(config: pytest.Config) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    root = logging.getLogger("iot_twin")
    root.setLevel(logging.DEBUG)
    root.handlers[:] = [handler]
    root.propagate = False
