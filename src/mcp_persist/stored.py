"""Stored event records and tiered-storage archive helpers."""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from mcp.server.streamable_http import EventId, StreamId

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class StoredEvent:
    """A raw event row as read from a backend (payload may be compressed)."""

    stream_id: StreamId
    event_id: EventId
    payload: str
    created_at: float


class _ArchivableHot(Protocol):
    _ttl: int | None

    def select_expired(self, *, cutoff: float, batch_size: int) -> AsyncIterator[StoredEvent]: ...

    async def delete_events(self, events: Sequence[StoredEvent]) -> int: ...


class _ArchivableCold(Protocol):
    async def _store_event_raw(
        self,
        stream_id: StreamId,
        event_id: EventId,
        payload: str,
        created_at: float,
    ) -> None: ...


async def archive_expired_batch(
    hot_store: Any,
    cold_store: Any,
    *,
    batch_size: int = 500,
) -> int:
    """Move one batch of expired events from ``hot_store`` into ``cold_store``.

    Reads up to ``batch_size`` rows past ``hot_store``'s ttl cutoff, writes each
    to ``cold_store`` with ID-preserving :meth:`_store_event_raw`, then deletes
    that exact batch from ``hot_store``. Order is archive-then-delete so a crash
    mid-cycle leaves duplicates in cold (idempotent upsert) rather than data loss.

    Returns the number of events archived (``0`` when nothing was expired).
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be a positive integer, got {batch_size!r}")

    ttl = getattr(hot_store, "_ttl", None)
    if ttl is None:
        return 0

    select_expired: Any = getattr(hot_store, "select_expired", None)
    delete_events: Any = getattr(hot_store, "delete_events", None)
    store_raw: Any = getattr(cold_store, "_store_event_raw", None)
    if select_expired is None or delete_events is None or store_raw is None:
        raise TypeError(
            "archive_expired_batch requires a hot store with select_expired/delete_events "
            "and a cold store with _store_event_raw"
        )

    cutoff = time.time() - ttl
    batch: list[StoredEvent] = []
    async for event in select_expired(cutoff=cutoff, batch_size=batch_size):
        batch.append(event)

    if not batch:
        return 0

    for event in batch:
        await store_raw(event.stream_id, event.event_id, event.payload, event.created_at)
    await delete_events(batch)
    return len(batch)


async def count_expired(hot_store: Any) -> int:
    """Count events past ``hot_store``'s ttl cutoff without deleting them."""
    counter: Any = getattr(hot_store, "count_expired", None)
    if counter is not None:
        return await counter()

    ttl = getattr(hot_store, "_ttl", None)
    if ttl is None:
        return 0

    select_expired: Any = getattr(hot_store, "select_expired", None)
    if select_expired is None:
        raise TypeError(f"{type(hot_store).__name__} has no count_expired() or select_expired()")

    cutoff = time.time() - ttl
    total = 0
    async for _ in select_expired(cutoff=cutoff, batch_size=10_000):
        total += 1
    return total
