"""批量期望下发编排：逐台隔离、调用幂等、结论可查。

* 一次请求可带几十台设备，每台各自的键值/乐观版本/合并方式；
* 一台失败（未注册、版本冲突、载荷非法）不影响其它设备；
* 同一 ``batch_id`` 重复提交：
    - 请求内容一致 -> 不重复下发、不造成版本乱跳，直接返回首拍结论；
    - 同一 ``batch_id`` 换了内容 -> 拒绝（``IDEMPOTENCY_KEY_REUSED``），
      调用方可以明确感知幂等键被挪用，而不是拿到与请求不符的旧结果。

判定逐台执行，下发动作通过 ``dispatch_one`` 回调注入，本模块不直接
依赖孪生/注册实现，方便单测与替换。
"""
from __future__ import annotations

import threading
from typing import Callable

from .errors import DomainError, ErrorCode
from .models import BatchDesiredItem, BatchItemResult, BatchOperation, ItemStatus


def _fingerprint(items: list[BatchDesiredItem]) -> str:
    """请求内容的稳定指纹：设备顺序、键值、乐观版本、合并方式全部纳入。"""
    parts = []
    for item in items:
        values = ",".join(f"{k}={item.values[k]!r}" for k in sorted(item.values))
        parts.append(
            f"{item.device_id}|merge={int(item.merge)}"
            f"|ev={item.expected_version}|{values}"
        )
    return ";".join(parts)


class BatchManager:
    def __init__(self) -> None:
        self._batches: dict[str, BatchOperation] = {}
        self._fingerprints: dict[str, str] = {}
        self._lock = threading.RLock()

    def get(self, batch_id: str) -> BatchOperation | None:
        with self._lock:
            op = self._batches.get(batch_id)
            if op is None:
                return None
            return BatchOperation(
                batch_id=op.batch_id,
                created_at_ms=op.created_at_ms,
                results=list(op.results),
            )

    def get_or_dispatch(
        self,
        batch_id: str,
        items: list[BatchDesiredItem],
        now_ms: int,
        dispatch_one: Callable[[BatchDesiredItem], BatchItemResult],
    ) -> BatchOperation:
        """幂等地执行一次批量下发；重复 batch_id 返回首拍结果。

        逐台回调执行，单台抛出的异常在本方法内归一化为 REJECTED 结果，
        因此一台失败不会中断同一批内其它设备。
        """
        if not batch_id:
            raise DomainError(ErrorCode.BAD_TELEMETRY, "batch_id 不能为空")
        if not items:
            raise DomainError(ErrorCode.EMPTY_BATCH, "批量下发至少要包含一台设备")

        fingerprint = _fingerprint(items)
        with self._lock:
            existing = self._batches.get(batch_id)
            if existing is not None:
                if self._fingerprints[batch_id] != fingerprint:
                    raise DomainError(
                        ErrorCode.IDEMPOTENCY_KEY_REUSED,
                        f"batch_id={batch_id} 已用于另一次请求，不能更换内容复用",
                    )
                # 完全相同的重试：直接返回首拍结论，绝不重复下发
                return self.get(batch_id)  # type: ignore[return-value]

            results: list[BatchItemResult] = []
            for item in items:
                try:
                    results.append(dispatch_one(item))
                except DomainError as exc:
                    results.append(
                        BatchItemResult(
                            device_id=item.device_id,
                            status=ItemStatus.REJECTED,
                            reject_reason=exc.code,
                            detail=exc.message,
                        )
                    )
                except Exception as exc:  # 防御：单台任何意外都不波及同批其它设备
                    results.append(
                        BatchItemResult(
                            device_id=item.device_id,
                            status=ItemStatus.REJECTED,
                            reject_reason="INTERNAL_ERROR",
                            detail=f"{type(exc).__name__}: {exc}",
                        )
                    )
            op = BatchOperation(
                batch_id=batch_id,
                created_at_ms=now_ms,
                results=results,
            )
            self._batches[batch_id] = op
            self._fingerprints[batch_id] = fingerprint
            return self.get(batch_id)  # type: ignore[return-value]
