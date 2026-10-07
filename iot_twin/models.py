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
    # 已下发但超过确认时限仍未收到设备确认：与 PENDING（尚在等待）、
    # CONFLICTED（设备按旧版本执行或键值对不上）明确区分。
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
    # 确认时限（毫秒）：None 表示该版本不设时限（兼容旧调用）。
    ack_timeout_ms: int | None = None
    # 当前版本的确认截止时刻；离线期间下发时为 None，重连时刻才被激活。
    deadline_ms: int | None = None
    # 本版为撤销版时，记录被撤销的版本号；None 表示普通下发。
    revoke_of_version: int | None = None
    # 该版本是否已经被设备正确确认（确认后不允许撤销，只能重新下发覆盖）。
    confirmed: bool = False


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
    # 最近一次已被设备确认的期望取值（撤销回退的基线）；从未确认过为 None。
    last_confirmed_desired: DesiredState | None = None


@dataclass
class Alert:
    rule_id: str
    device_id: str
    state: AlertState
    changed_at_ms: int
    detail: str = ""
    trigger_value: float | None = None
    error_code: str | None = None


class BatchItemStatus(str, Enum):
    DISPATCHED = "DISPATCHED"   # 下发成功
    REJECTED = "REJECTED"       # 被拒（原因见 reject_reason/detail）


@dataclass(frozen=True)
class BatchDesiredItem:
    """批量下发中的单台请求：每台可带各自不同的键值。"""
    device_id: str
    values: dict
    expected_version: int | None = None
    merge: bool = True
    ack_timeout_ms: int | None = None


@dataclass
class BatchItemResult:
    """单台结论；查询批次时附带该设备当前的逐台对账结论。"""
    device_id: str
    status: BatchItemStatus
    desired_version: int | None = None
    reject_reason: str | None = None
    detail: str = ""
    # 批次查询时回填：该设备当前同步状态（如 PENDING/ACK_TIMEOUT/IN_SYNC/CONFLICTED）
    current_sync_status: str | None = None


@dataclass
class BatchRecord:
    batch_id: str
    items: list[BatchItemResult]
    created_at_ms: int
    idempotency_key: str | None = None

    def result_for(self, device_id: str) -> BatchItemResult | None:
        for item in self.items:
            if item.device_id == device_id:
                return item
        return None
