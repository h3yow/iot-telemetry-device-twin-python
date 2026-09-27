import logging

import pytest

from iot_service import IoTService

logger = logging.getLogger("iot.test")


@pytest.fixture
def service() -> IoTService:
    return IoTService(dedup_window_sec=300.0)


def log_case(title: str, inputs: dict, basis: str, final: dict) -> None:
    """单测统一日志：打印输入、判定依据与最终状态。"""
    logger.info("用例=%s | 输入=%s | 判定依据=%s | 最终状态=%s", title, inputs, basis, final)
