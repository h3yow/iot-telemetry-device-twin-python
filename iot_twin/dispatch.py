"""批量下发编排：逐台独立下发、逐台记录结论、按幂等键去重。

设计要点
--------
* 一批几十台设备逐台调用单台下发，**单台失败不影响其它设备**；
* 每台的成功/拒绝及原因都保留在 :class:`BatchRecord` 中，调用方可随时
  按 ``batch_id`` 查询最终逐台结论；
* 同一 ``idempotency_key`` 重复提交直接返回首次的批次记录，不会重复
  下发、也不会导致期望版本乱跳。
"""
from __future__ import annotations

import threading
from typing import Callable

from .errors import DomainError, ErrorCode
from .models import (
    BatchDesiredItem,
    BatchItemResult,
    BatchItemStatus,
    BatchRecord,
)

DispatchOne = Callable[[BatchDesiredItem], BatchItemResult]


class BatchDispatcher:
    def __init__(self) -> None:
        self._records: dict[str, BatchRecord] = {}
        self._by_idem_key: dict[str, str] = {}
        self._lock = threading.RLock()

    def submit(
        self,
        batch_id: str,
        items: list[BatchDesiredItem],
        created_at_ms: int,
        idempotency_key: str | None,
        dispatch_one: DispatchOne,
    ) -> BatchRecord:
        """提交一批下发；逐台独立执行，单台失败不影响其它设备。

        相同 ``idempotency_key`` 的重复提交直接返回首次记录，不会重复下发。
        """
        with self._lock:
            if idempotency_key is not None:
                existing_id = self._by_idem_key.get(idempotency_key)
                if existing_id is not None:
                    return self._records[existing_id]
            if batch_id in self._records:
                return self._records[batch_id]

            self._validate(items)

            results: list[BatchItemResult] = []
            for item in items:
                results.append(self._safe_dispatch(item, dispatch_one))

            record = BatchRecord(
                batch_id=batch_id,
                items=results,
                created_at_ms=created_at_ms,
                idempotency_key=idempotency_key,
            )
            self._records[batch_id] = record
            if idempotency_key is not None:
                self._by_idem_key[idempotency_key] = batch_id
            return record

    @staticmethod
    def _validate(items: list[BatchDesiredItem]) -> None:
        if not items:
            raise DomainError(ErrorCode.INVALID_REQUEST, "批量下发不能为空")
        seen: set[str] = set()
        for item in items:
            if item.device_id in seen:
                raise DomainError(
                    ErrorCode.INVALID_REQUEST,
                    f"批次内设备 {item.device_id} 重复出现",
                )
            seen.add(item.device_id)

    @staticmethod
    def _safe_dispatch(
        item: BatchDesiredItem, dispatch_one: DispatchOne
    ) -> BatchItemResult:
        try:
            return dispatch_one(item)
        except DomainError as exc:
            return BatchItemResult(
                device_id=item.device_id,
                status=BatchItemStatus.REJECTED,
                reject_reason=exc.code,
                detail=exc.message,
            )

    def get(self, batch_id: str) -> BatchRecord:
        with self._lock:
            record = self._records.get(batch_id)
            if record is None:
                raise DomainError(
                    ErrorCode.BATCH_NOT_FOUND,
                    f"批次 {batch_id} 不存在",
                )
            # 返回带逐台当前结论回填的副本，避免调用方修改内部记录
            return BatchRecord(
                batch_id=record.batch_id,
                items=[
                    BatchItemResult(
                        device_id=r.device_id,
                        status=r.status,
                        desired_version=r.desired_version,
                        reject_reason=r.reject_reason,
                        detail=r.detail,
                        current_sync_status=r.current_sync_status,
                    )
                    for r in record.items
                ],
                created_at_ms=record.created_at_ms,
                idempotency_key=record.idempotency_key,
            )
