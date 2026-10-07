"""可区分的失败原因（错误码）与领域异常。"""
from __future__ import annotations


class ErrorCode:
    DEVICE_NOT_REGISTERED = "DEVICE_NOT_REGISTERED"
    INVALID_CREDENTIAL = "INVALID_CREDENTIAL"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    DEVICE_ALREADY_REGISTERED = "DEVICE_ALREADY_REGISTERED"
    DUPLICATE_SAMPLE = "DUPLICATE_SAMPLE"
    STALE_SAMPLE = "STALE_SAMPLE"
    BAD_TELEMETRY = "BAD_TELEMETRY"
    RULE_NOT_FOUND = "RULE_NOT_FOUND"
    RULE_ALREADY_EXISTS = "RULE_ALREADY_EXISTS"
    STATE_CONFLICT = "STATE_CONFLICT"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    RULE_EVALUATION_ERROR = "RULE_EVALUATION_ERROR"
    # 撤销一个已经被设备确认生效的期望版本
    ALREADY_CONFIRMED = "ALREADY_CONFIRMED"
    # 批量请求本身不合法（空批次 / 批次内重复设备）
    INVALID_REQUEST = "INVALID_REQUEST"
    # 批次不存在
    BATCH_NOT_FOUND = "BATCH_NOT_FOUND"


class DomainError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message
