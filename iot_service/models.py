"""核心数据模型与枚举定义（骨架）。"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RejectReason(str, Enum):
    """遥测/操作被拒绝的可区分原因。"""

    DEVICE_NOT_REGISTERED = "DEVICE_NOT_REGISTERED"
    INVALID_CREDENTIALS = "INVALID_CREDENTIALS"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    DUPLICATE_MESSAGE = "DUPLICATE_MESSAGE"
    STALE_MESSAGE = "STALE_MESSAGE"
    VERSION_CONFLICT = "VERSION_CONFLICT"


class ConnectionState(str, Enum):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"


class AlertStatus(str, Enum):
    ACTIVE = "ACTIVE"
    RESOLVED = "RESOLVED"


@dataclass
class Device:
    device_id: str
    token: str
    scopes: frozenset[str] = field(default_factory=lambda: frozenset({"telemetry:write"}))
    state: ConnectionState = ConnectionState.OFFLINE


@dataclass
class TelemetryMessage:
    device_id: str
    payload: dict[str, Any]
    message_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    seq: int = 0  # 设备侧单调序号
    ts: float = 0.0  # 设备侧采集时间戳（秒）


@dataclass
class IngestResult:
    accepted: bool
    reason: RejectReason | None = None
    applied_seq: int = 0
    alerts: list["Alert"] = field(default_factory=list)


@dataclass
class TwinState:
    """设备孪生：期望状态与上报状态分离维护。"""

    desired: dict[str, Any] = field(default_factory=dict)
    reported: dict[str, Any] = field(default_factory=dict)
    desired_version: int = 0
    reported_version: int = 0


@dataclass
class TwinUpdateResult:
    applied: bool
    reason: RejectReason | None = None
    conflict: bool = False
    detail: str = ""
    version: int = 0


@dataclass
class Alert:
    alert_id: str
    device_id: str
    rule_id: str
    status: AlertStatus
    message: str
    raised_at: float = field(default_factory=time.time)
    resolved_at: float | None = None
    source_seq: int = 0


@dataclass
class RuleError:
    """规则执行异常归一化后的记录。"""

    rule_id: str
    device_id: str
    error_type: str
    detail: str
