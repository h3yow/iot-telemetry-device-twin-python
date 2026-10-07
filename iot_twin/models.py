"""领域模型与枚举。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ConnectionState(str, Enum):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"


class Role(str, Enum):
    DEVICE = "DEVICE"
    ADMIN = "ADMIN"


class SyncStatus(str, Enum):
    PENDING = "PENDING"
    IN_SYNC = "IN_SYNC"
    CONFLICTED = "CONFLICTED"
    # 已下发并开始计时，但超过确认时限仍未收到设备确认
    ACK_TIMEOUT = "ACK_TIMEOUT"


class AlertState(str, Enum):
    FIRING = "FIRING"
    RESOLVED = "RESOLVED"
    ERROR = "ERROR"


class RejectReason(str, Enum):
    DEVICE_NOT_REGISTERED = "DEVICE_NOT_REGISTERED"
    INVALID_CREDENTIAL = "INVALID_CREDENTIAL"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    DUPLICATE_SAMPLE = "DUPLICATE_SAMPLE"
    STALE_SAMPLE = "STALE_SAMPLE"
    BAD_TELEMETRY = "BAD_TELEMETRY"


@dataclass(frozen=True)
class Principal:
    device_id: str | None
    role: Role


@dataclass
class Device:
    device_id: str
    credential: str
    registered_at_ms: int = 0


@dataclass
class TelemetrySample:
    device_id: str
    seq: int
    ts_ms: int
    metrics: dict = field(default_factory=dict)
    received_at_ms: int = 0


@dataclass
class IngestResult:
    accepted: bool
    device_id: str
    seq: int
    reject_reason: str | None = None
    detail: str = ""


@dataclass
class DesiredState:
    version: int
    values: dict
    updated_at_ms: int
    expected_version: int | None = None


@dataclass
class ReportedState:
    version: int
    values: dict
    updated_at_ms: int
    last_seq: int = -1


@dataclass
class TwinState:
    device_id: str
    connection: ConnectionState = ConnectionState.OFFLINE
    desired: DesiredState | None = None
    reported: ReportedState | None = None
    sync_status: SyncStatus = SyncStatus.PENDING
    connected_since_ms: int | None = None
    last_event_ms: int = 0


@dataclass
class Alert:
    rule_id: str
    device_id: str
    state: AlertState
    changed_at_ms: int
    detail: str = ""
    trigger_value: float | None = None
    error_code: str | None = None


# 单台设备的期望下发结论（批量逐台结果复用同一组状态）
class ItemStatus(str, Enum):
    DELIVERED = "DELIVERED"          # 已下发（设备是否确认取决于后续对账）
    REJECTED = "REJECTED"            # 未下发（原因见 reject_reason）


@dataclass
class BatchDesiredItem:
    """批量下发中的一台设备：可带各自不同的键值。"""
    device_id: str
    values: dict = field(default_factory=dict)
    expected_version: int | None = None
    merge: bool = True


@dataclass
class BatchItemResult:
    device_id: str
    status: ItemStatus
    desired_version: int | None = None
    reject_reason: str | None = None
    detail: str = ""


@dataclass
class BatchOperation:
    batch_id: str
    created_at_ms: int
    results: list[BatchItemResult] = field(default_factory=list)


@dataclass
class RollbackOutcome:
    """撤销一次未确认期望变更的结论。"""
    device_id: str
    rolled_back: bool
    idempotent: bool = False
    desired_version: int | None = None
    detail: str = ""
