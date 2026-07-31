"""Cross-backend event migration.

:func:`migrate` copies events from one event store to another — for example from
SQLite to Postgres when a single-node deployment grows into a multi-node one, or
from Redis to Postgres to gain durability. It streams events from the source and
re-stores them on the destination, preserving per-stream ordering.

Important caveats — read before migrating a production deployment:

* **Event IDs are not preserved.** The destination assigns its own fresh,
  monotonic IDs via ``store_event``. Ordering and payloads are preserved; the
  numeric IDs are not.
* **Timestamps are reset.** Re-stored events get a new ``created_at`` of "now",
  so any ``ttl`` expiry clock restarts on the destination.
* **Resumability tokens are invalidated.** Because IDs change, a client holding
  a ``Last-Event-ID`` from the source store cannot resume against the
  destination after cutover. Migrate during a maintenance window and drain or
  reconnect clients afterwards.
* **Not consistent under concurrent writes.** ``migrate`` is a point-in-time
  copy. Events written to the source while it runs may or may not be picked up.
  Stop writes to the source (or treat the source as read-only) for a complete,
  consistent copy.

Usage::

    from mcp_persist import migrate

    result = await migrate(sqlite_store, postgres_store)
    print(result.events_migrated, result.failed_streams)

    # Single stream, custom batch size, progress logging:
    await migrate(
        sqlite_store,
        postgres_store,
        stream_id="session-abc",
        batch_size=100,
        on_progress=lambda sid, n: print(f"{sid}: {n}"),
    )
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Protocol

    from mcp.server.streamable_http import EventId, StreamId
    from mcp.types import JSONRPCMessage

    class _MigrationSource(Protocol):
        def list_streams(self) -> AsyncIterator[StreamId]: ...

        def _iter_stream_events(self, stream_id: StreamId) -> AsyncIterator[tuple[EventId, JSONRPCMessage | None]]: ...

    class _MigrationDest(Protocol):
        async def store_event(self, stream_id: StreamId, message: JSONRPCMessage | None) -> EventId: ...

    ProgressCallback = Callable[[StreamId, int], None]

logger = logging.getLogger(__name__)


@dataclass
class MigrationResult:
    """Summary of a :func:`migrate` run.

    Attributes:
        streams_migrated: Number of streams copied without error.
        events_migrated:  Total events written to the destination.
        failed_streams:   Stream IDs whose migration raised; these were logged
                          and skipped so the rest of the migration could finish.
                          A failed stream may have been copied partially.
        skipped_events:   Events the source could read from storage but could not
                          decode into a message, so they were not copied. The
                          usual cause is a payload the source store cannot
                          decrypt (no keyring configured, or the wrong key), and
                          the rest is genuine corruption. These do not raise, so
                          without this count a migration that copied nothing
                          would look like a migration of an empty store. Zero
                          when the source does not track the count.
    """

    streams_migrated: int = 0
    events_migrated: int = 0
    failed_streams: list[str] = field(default_factory=list)
    skipped_events: int = 0


async def migrate(
    source: _MigrationSource,
    dest: _MigrationDest,
    *,
    batch_size: int = 500,
    stream_id: StreamId | None = None,
    on_progress: ProgressCallback | None = None,
) -> MigrationResult:
    """Copy events from ``source`` to ``dest``, preserving per-stream ordering.

    Reads every event from ``source`` (oldest first) and re-stores it on ``dest``
    with ``store_event``. See the module docstring for the important caveats: IDs
    and timestamps are not preserved, resumability tokens are invalidated, and
    the copy is not consistent under concurrent writes to the source.

    Args:
        source:      The store to read from (any backend in this package).
        dest:        The store to write to (any backend in this package).
        batch_size:  How many events to copy for a stream between ``on_progress``
                     calls. Must be a positive integer. Does not change what is
                     migrated, only the progress-reporting cadence.
        stream_id:   If given, migrate only this stream. If ``None`` (default),
                     migrate every stream returned by ``source.list_streams()``.
        on_progress: Optional callback invoked as ``on_progress(stream_id,
                     events_migrated_so_far)`` every ``batch_size`` events and
                     once when a stream finishes. Use it to drive a progress bar
                     or log.

    Returns:
        A :class:`MigrationResult`. Each stream is migrated independently: if one
        stream raises it is logged, recorded in ``failed_streams``, and migration
        continues with the next stream rather than aborting the whole run.
        Events the source cannot decode (undecryptable or corrupt payloads) are
        skipped without raising and counted in ``skipped_events``.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be a positive integer, got {batch_size!r}")

    result = MigrationResult()
    # The stores skip an undecodable event mid-iteration rather than raising, so
    # the only way to see the loss is to sample their counter across the run.
    # getattr keeps a source that predates the counter (or a custom one) working.
    unreadable_before = getattr(source, "unreadable_events", 0)

    async def migrate_one(sid: StreamId) -> int:
        migrated = 0
        last_reported = 0
        async for _event_id, message in source._iter_stream_events(sid):
            await dest.store_event(sid, message)
            migrated += 1
            if on_progress is not None and migrated - last_reported >= batch_size:
                on_progress(sid, migrated)
                last_reported = migrated
        if on_progress is not None and migrated != last_reported:
            on_progress(sid, migrated)
        return migrated

    async def run(sid: StreamId) -> None:
        try:
            migrated = await migrate_one(sid)
        except Exception:  # noqa: BLE001 - one bad stream must not abort the whole migration
            logger.exception("Migration of stream %s failed; skipping and continuing", sid)
            result.failed_streams.append(sid)
            return
        result.streams_migrated += 1
        result.events_migrated += migrated

    if stream_id is not None:
        await run(stream_id)
    else:
        async for sid in source.list_streams():
            await run(sid)

    result.skipped_events = getattr(source, "unreadable_events", 0) - unreadable_before
    if result.skipped_events:
        logger.warning(
            "Migration skipped %d event(s) the source could not decode; "
            "they were not copied to the destination. If the source is encrypted, "
            "check that the migration is configured with its keyring.",
            result.skipped_events,
        )
    return result
