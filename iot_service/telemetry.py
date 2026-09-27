"""遥测接入：去重、乱序与离线补传合并。

规则：
- 以 message_id 在去重窗口内去重，重复上报返回 DUPLICATE_MESSAGE，不重复计入；
- 以设备侧单调序号 seq 判定新旧：seq <= 已应用最大序号 的消息为过期/乱序补传，
  返回 STALE_MESSAGE，不并入最新状态，保证状态不倒退；
- 离线期间的批量补传在重连后逐条 ingest，按 seq 自然并入，乱序的过期条目被安全丢弃。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

from iot_service.models import IngestResult, RejectReason, TelemetryMessage

DEFAULT_DEDUP_WINDOW_SEC = 300.0


@dataclass
class _DeviceStream:
    last_seq: int = 0
    latest: dict[str, Any] = field(default_factory=dict)
    seen_ids: dict[str, float] = field(default_factory=dict)  # message_id -> 首次见到的时间


class TelemetryStore:
    def __init__(self, dedup_window_sec: float = DEFAULT_DEDUP_WINDOW_SEC,
                 clock: Any = time.monotonic) -> None:
        self._streams: dict[str, _DeviceStream] = {}
        self._lock = threading.Lock()
        self._dedup_window = dedup_window_sec
        self._clock = clock

    def _stream(self, device_id: str) -> _DeviceStream:
        return self._streams.setdefault(device_id, _DeviceStream())

    def _prune(self, stream: _DeviceStream, now: float) -> None:
        expired = [mid for mid, t in stream.seen_ids.items() if now - t > self._dedup_window]
        for mid in expired:
            del stream.seen_ids[mid]

    def ingest(self, msg: TelemetryMessage) -> IngestResult:
        with self._lock:
            stream = self._stream(msg.device_id)
            now = self._clock()
            self._prune(stream, now)
            if msg.message_id in stream.seen_ids:
                return IngestResult(accepted=False, reason=RejectReason.DUPLICATE_MESSAGE,
                                    applied_seq=stream.last_seq)
            stream.seen_ids[msg.message_id] = now
            if msg.seq <= stream.last_seq:
                return IngestResult(accepted=False, reason=RejectReason.STALE_MESSAGE,
                                    applied_seq=stream.last_seq)
            stream.latest.update(msg.payload)
            stream.last_seq = msg.seq
            return IngestResult(accepted=True, applied_seq=stream.last_seq)

    def latest(self, device_id: str) -> dict[str, Any]:
        with self._lock:
            return dict(self._stream(device_id).latest)

    def last_seq(self, device_id: str) -> int:
        with self._lock:
            return self._stream(device_id).last_seq
