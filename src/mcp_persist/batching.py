"""Write-behind batching wrapper for high-throughput event stores."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mcp.server.streamable_http import (
    EventCallback,
    EventId,
    EventStore,
    StreamId,
)
from mcp.types import JSONRPCMessage

from mcp_persist._debug import debug_log
from mcp_persist.health import HealthReport
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
        self._id_block: deque[EventId] = deque()
        self._lock = asyncio.Lock()
        self._flush_lock = asyncio.Lock()
        self._flush_task: asyncio.Task[None] | None = None
        self._next_flush_at: float | None = None
        self._closed = False

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
            if self._closed:
                raise RuntimeError("BatchingEventStore is closed")
            if not self._id_block:
                self._id_block.extend(
                    await self._inner._allocate_event_ids(self._flush_max_events)  # type: ignore[attr-defined]
                )
            event_id = self._id_block.popleft()
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
            try:
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - retain the batch and retry after the latency window
                logger.exception("batched event flush failed; retrying after the latency window")

    async def flush(self) -> None:
        async with self._flush_lock:
            async with self._lock:
                if not self._pending:
                    return
                batch = self._pending
                self._pending = []

            store_with_id: Any = getattr(self._inner, "_store_event_with_id", None)
            if store_with_id is None:
                raise TypeError(f"{type(self._inner).__name__} has no _store_event_with_id()")

            written = 0
            try:
                store_batch: Any = getattr(self._inner, "_store_events_with_ids", None)
                if store_batch is not None:
                    await store_batch([(item.stream_id, item.message, item.event_id) for item in batch])
                    written = len(batch)
                else:
                    for item in batch:
                        await store_with_id(item.stream_id, item.message, item.event_id)
                        written += 1
            except BaseException:
                # Completed writes are idempotent and need no retry. Put the
                # failed item and untouched tail back ahead of writes accepted
                # concurrently, preserving global event-ID order.
                async with self._lock:
                    self._pending = batch[written:] + self._pending
                    self._next_flush_at = time.monotonic() + (self._flush_max_latency_ms / 1000.0)
                raise

            debug_log("FLUSH events=%d", len(batch))

            async with self._lock:
                if self._pending:
                    self._next_flush_at = time.monotonic() + (self._flush_max_latency_ms / 1000.0)
                    self._ensure_flusher()
                else:
                    self._next_flush_at = None

    async def aclose(self) -> None:
        async with self._lock:
            self._closed = True
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
        stream_id: StreamId | None = None,
    ) -> StreamId | None:
        await self.flush()
        return await self._inner.replay_events_after(last_event_id, send_callback, stream_id)  # pyright: ignore[reportCallIssue]

    async def fork_stream(
        self,
        parent_stream_id: StreamId,
        fork_event_id: EventId,
        new_stream_id: StreamId,
    ) -> None:
        """Branch a session at a specific event ID."""
        await self.flush()
        fork_method = getattr(self._inner, "fork_stream", None)
        if fork_method is not None:
            await fork_method(parent_stream_id, fork_event_id, new_stream_id)

    # ── operational surface ─────────────────────────────────────────────────

    # Batching is selected by event_store_from_env(), so it must preserve the
    # optional operational APIs exposed by the concrete store rather than turn a
    # configured production store into an EventStore-only object. Reads flush
    # first: callers should never observe a partial view merely because writes
    # happen to be in the short batching window.

    def __getattr__(self, name: str) -> Any:
        """Delegate backend metadata and future optional APIs to ``inner``.

        Read/write operations that need flush-before-observe semantics are
        defined explicitly below. This fallback keeps non-mutating metadata such
        as ``table_name`` and ``_ttl`` available to existing integrations.
        """
        return getattr(self._inner, name)

    async def ping(self) -> bool:
        ping: Any = getattr(self._inner, "ping", None)
        if ping is None:
            raise AttributeError(f"{type(self._inner).__name__} has no ping()")
        return await ping()

    async def health(self) -> HealthReport:
        health: Any = getattr(self._inner, "health", None)
        if health is None:
            raise AttributeError(f"{type(self._inner).__name__} has no health()")
        report: HealthReport = await health()
        async with self._lock:
            pending = len(self._pending)
        detail = dict(report.detail)
        detail["inner_backend"] = report.backend
        detail["pending_writes"] = pending
        return HealthReport(
            healthy=report.healthy,
            backend="batching",
            latency_ms=report.latency_ms,
            detail=detail,
        )

    @property
    def backend_name(self) -> str:
        """Expose the wrapped backend to integrations that select by storage type."""
        return str(getattr(self._inner, "backend_name", type(self._inner).__name__.lower()))

    async def list_streams(self) -> AsyncIterator[StreamId]:
        await self.flush()
        list_streams: Any = getattr(self._inner, "list_streams", None)
        if list_streams is None:
            raise AttributeError(f"{type(self._inner).__name__} has no list_streams()")
        async for stream_id in list_streams():
            yield stream_id

    async def _iter_stream_events(self, stream_id: StreamId) -> AsyncIterator[tuple[EventId, JSONRPCMessage | None]]:
        await self.flush()
        iterator: Any = getattr(self._inner, "_iter_stream_events", None)
        if iterator is None:
            raise AttributeError(f"{type(self._inner).__name__} has no _iter_stream_events()")
        async for item in iterator(stream_id):
            yield item

    async def subscribe(self, stream_id: StreamId, **kwargs: Any) -> AsyncIterator[tuple[EventId, JSONRPCMessage]]:
        await self.flush()
        subscribe: Any = getattr(self._inner, "subscribe", None)
        if subscribe is None:
            raise AttributeError(f"{type(self._inner).__name__} has no subscribe()")
        async for item in subscribe(stream_id, **kwargs):
            yield item

    async def purge_expired(self, **kwargs: Any) -> int:
        await self.flush()
        purge: Any = getattr(self._inner, "purge_expired", None)
        if purge is None:
            raise AttributeError(f"{type(self._inner).__name__} has no purge_expired()")
        return await purge(**kwargs)

    async def count_expired(self, **kwargs: Any) -> int:
        await self.flush()
        count: Any = getattr(self._inner, "count_expired", None)
        if count is None:
            raise AttributeError(f"{type(self._inner).__name__} has no count_expired()")
        return await count(**kwargs)

    async def select_expired(self, **kwargs: Any) -> AsyncIterator[Any]:
        await self.flush()
        select: Any = getattr(self._inner, "select_expired", None)
        if select is None:
            raise AttributeError(f"{type(self._inner).__name__} has no select_expired()")
        async for item in select(**kwargs):
            yield item

    async def delete_events(self, events: Any) -> int:
        await self.flush()
        delete: Any = getattr(self._inner, "delete_events", None)
        if delete is None:
            raise AttributeError(f"{type(self._inner).__name__} has no delete_events()")
        return await delete(events)

    async def _store_event_raw(self, *args: Any, **kwargs: Any) -> None:
        await self.flush()
        store_raw: Any = getattr(self._inner, "_store_event_raw", None)
        if store_raw is None:
            raise AttributeError(f"{type(self._inner).__name__} has no _store_event_raw()")
        await store_raw(*args, **kwargs)

    async def _event_exists(self, event_id: EventId) -> bool:
        await self.flush()
        exists: Any = getattr(self._inner, "_event_exists", None)
        if exists is None:
            raise AttributeError(f"{type(self._inner).__name__} has no _event_exists()")
        return await exists(event_id)

    async def _stream_id_for_event(self, event_id: EventId) -> StreamId | None:
        await self.flush()
        lookup: Any = getattr(self._inner, "_stream_id_for_event", None)
        if lookup is None:
            raise AttributeError(f"{type(self._inner).__name__} has no _stream_id_for_event()")
        return await lookup(event_id)

    async def distinct_tenants(self) -> list[str | None]:
        await self.flush()
        tenants: Any = getattr(self._inner, "distinct_tenants", None)
        if tenants is None:
            raise AttributeError(f"{type(self._inner).__name__} has no distinct_tenants()")
        return await tenants()

    async def purge_tenant(self, tenant_id: str | None, **kwargs: Any) -> int:
        await self.flush()
        purge: Any = getattr(self._inner, "purge_tenant", None)
        if purge is None:
            raise AttributeError(f"{type(self._inner).__name__} has no purge_tenant()")
        return await purge(tenant_id, **kwargs)
