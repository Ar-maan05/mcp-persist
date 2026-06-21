"""Write-behind batching wrapper for high-throughput event stores."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventStore,
    StreamId,
)
from mcp.types import JSONRPCMessage

from mcp_persist.metrics import NoOpMetricsCollector, safe_call

if TYPE_CHECKING:
    from mcp_persist.metrics import MetricsCollector

logger = logging.getLogger(__name__)


@dataclass
class _PendingWrite:
    stream_id: StreamId
    message: JSONRPCMessage | None
    event_id: EventId


class BatchingEventStore(EventStore):
    """Buffer ``store_event`` writes and flush on size or latency thresholds.

    Returns an ``event_id`` immediately by pre-allocating ID blocks from the
    inner store; durability is deferred until the flush window (default 50 ms).
    Acceptable for resumability: worst case the client replays from one event
    earlier if the process crashes before flush.

    The inner store must expose ``_allocate_event_ids(n) -> list[EventId]`` and
    ``_store_event_with_id(stream_id, message, event_id)``, provided by the Redis
    and Postgres backends. SQLite is intentionally unsupported: its own
    write-behind (``commit_interval`` / ``commit_max_pending``) already batches
    the fsync that dominates its write cost, so wrapping it would only add a layer.

    Args:
        inner:                 The wrapped event store.
        flush_max_events:      Flush when this many events are buffered (default 64).
        flush_max_latency_ms:  Flush after this many milliseconds (default 50).
        metrics:               Optional metrics collector.
    """

    def __init__(
        self,
        inner: EventStore,
        *,
        flush_max_events: int = 64,
        flush_max_latency_ms: float = 50.0,
        metrics: MetricsCollector | None = None,
    ) -> None:
        if flush_max_events < 1:
            raise ValueError(f"flush_max_events must be a positive integer, got {flush_max_events!r}")
        if flush_max_latency_ms <= 0:
            raise ValueError(f"flush_max_latency_ms must be positive, got {flush_max_latency_ms!r}")
        if not callable(getattr(inner, "_allocate_event_ids", None)):
            raise TypeError(
                f"{type(inner).__name__} does not support batched ID pre-allocation. BatchingEventStore "
                "wraps backends whose per-event round trip is the bottleneck (RedisEventStore, "
                "PostgresEventStore). SQLite already batches the dominant fsync cost via its own "
                "write-behind (commit_interval / commit_max_pending), so wrap one of those instead."
            )

        self._inner = inner
        self._flush_max_events = flush_max_events
        self._flush_max_latency_ms = flush_max_latency_ms
        self._metrics: MetricsCollector = metrics if metrics is not None else NoOpMetricsCollector()
        self._pending: list[_PendingWrite] = []
        self._id_block: list[EventId] = []
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task[None] | None = None
        self._next_flush_at: float | None = None

    async def store_event(
        self,
        stream_id: StreamId,
        message: JSONRPCMessage | None,
    ) -> EventId:
        if type(self._metrics) is NoOpMetricsCollector:
            return await self._store_event_impl(stream_id, message)
        start = time.monotonic()
        try:
            event_id = await self._store_event_impl(stream_id, message)
        except Exception as exc:
            safe_call(self._metrics.on_error, "store_event", exc)
            raise
        safe_call(self._metrics.on_store_event, stream_id, event_id, (time.monotonic() - start) * 1000.0)
        return event_id

    async def _store_event_impl(self, stream_id: StreamId, message: JSONRPCMessage | None) -> EventId:
        async with self._lock:
            if not self._id_block:
                self._id_block = await self._inner._allocate_event_ids(self._flush_max_events)  # type: ignore[attr-defined]
            event_id = self._id_block.pop(0)
            self._pending.append(_PendingWrite(stream_id, message, event_id))
            flush_now = len(self._pending) >= self._flush_max_events
            if self._next_flush_at is None:
                self._next_flush_at = time.monotonic() + (self._flush_max_latency_ms / 1000.0)
                self._ensure_flusher()
        if flush_now:
            await self.flush()
        return event_id

    def _ensure_flusher(self) -> None:
        if self._flush_task is not None and not self._flush_task.done():
            return
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def _flush_loop(self) -> None:
        while True:
            async with self._lock:
                deadline = self._next_flush_at
            if deadline is None:
                return
            delay = max(0.0, deadline - time.monotonic())
            await asyncio.sleep(delay)
            await self.flush()

    async def flush(self) -> None:
        async with self._lock:
            if not self._pending:
                return
            batch = self._pending
            self._pending = []

        store_with_id: Any = getattr(self._inner, "_store_event_with_id", None)
        if store_with_id is None:
            raise TypeError(f"{type(self._inner).__name__} has no _store_event_with_id()")
        for item in batch:
            await store_with_id(item.stream_id, item.message, item.event_id)

        async with self._lock:
            if self._pending:
                self._next_flush_at = time.monotonic() + (self._flush_max_latency_ms / 1000.0)
                self._ensure_flusher()
            else:
                self._next_flush_at = None

    async def aclose(self) -> None:
        task = self._flush_task
        self._flush_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await self.flush()
        closer: Any = getattr(self._inner, "aclose", None)
        if closer is not None:
            await closer()

    async def replay_events_after(
        self,
        last_event_id: EventId,
        send_callback: EventCallback,
    ) -> StreamId | None:
        await self.flush()
        return await self._inner.replay_events_after(last_event_id, send_callback)
