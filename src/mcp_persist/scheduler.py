"""Background scheduler for periodic ``purge_expired()`` calls.

SQLite and Postgres have no automatic row expiry: ``ttl`` only *hides* expired
events on replay, and :meth:`~mcp_persist.SQLiteEventStore.purge_expired` must be
called periodically to reclaim space (see ``docs/production.md``).
:class:`PurgeScheduler` is a small, batteries-included wrapper around that purge
loop so callers don't have to hand-roll one::

    from mcp_persist import PostgresEventStore, PurgeScheduler

    store = PostgresEventStore(pool, ttl=3600)
    async with PurgeScheduler(store, interval=300):
        async with manager.run():
            yield  # purge runs every 300s for the life of the block

Or manage the lifecycle explicitly with :meth:`start` / :meth:`aclose`.

It is deliberately rejected for stores without a ``purge_expired`` method
(``RedisEventStore``, which relies on native key TTL) so a misconfiguration
surfaces at construction rather than as a silently useless loop.
"""

from __future__ import annotations

import asyncio
import logging
import random
from types import TracebackType
from typing import TYPE_CHECKING, Any

from mcp_persist.retention import DeletionAuditEntry
from mcp_persist.stored import archive_expired_batch

if TYPE_CHECKING:
    from typing import Protocol

    from mcp_persist.retention import AuditSink, RetentionPolicy

    class _Purgeable(Protocol):
        async def purge_expired(self, *, batch_size: int | None = ...) -> int: ...


class PurgeScheduler:
    """Periodically call ``store.purge_expired()`` on a background task.

    Args:
        store:       A store exposing ``purge_expired`` (SQLite or Postgres).
                     Passing a store without it (e.g. ``RedisEventStore``) raises
                     ``TypeError`` — Redis expires keys natively, so a scheduler
                     would do nothing.
        interval:    Seconds between purges. Must be positive.
        jitter:      Maximum extra seconds added to each sleep, drawn uniformly
                     from ``[0, jitter]`` and re-rolled every cycle. Must be
                     non-negative; ``0.0`` (the default) keeps the loop exactly
                     periodic. Use it to de-synchronise replicas that start
                     together so they don't all purge a shared backend in the
                     same instant (a "thundering herd"). A good rule of thumb is
                     10–20% of ``interval`` — e.g. ``interval=300, jitter=30``
                     spreads replicas across a 30s window.
        batch_size:  Forwarded to ``purge_expired(batch_size=...)`` when set, so a
                     large purge deletes in bounded chunks instead of one long
                     locking ``DELETE``. ``None`` (the default) uses the store's
                     single-statement purge.
        log:         Logger for the one-line "purged N events" message (at
                     ``INFO``) and for errors (at ``ERROR``). Defaults to the
                     ``mcp_persist.scheduler`` logger.

    A purge that raises is logged and swallowed so the loop survives a transient
    backend blip; the loop only stops on :meth:`aclose` (or context exit).
    """

    def __init__(
        self,
        store: Any,  # _Purgeable at runtime
        interval: float,
        *,
        jitter: float = 0.0,
        batch_size: int | None = None,
        log: logging.Logger | None = None,
    ) -> None:
        if not callable(getattr(store, "purge_expired", None)):
            raise TypeError(
                f"{type(store).__name__} has no purge_expired(); PurgeScheduler only applies to backends "
                "that need manual expiry (SQLite, Postgres). Redis expires keys natively via ttl."
            )
        if interval <= 0:
            raise ValueError(f"interval must be a positive number of seconds, got {interval!r}")
        if jitter < 0:
            raise ValueError(f"jitter must be a non-negative number of seconds, got {jitter!r}")
        if batch_size is not None and batch_size < 1:
            raise ValueError(f"batch_size must be a positive integer or None, got {batch_size!r}")

        self._store = store
        self._interval = interval
        self._jitter = jitter
        self._batch_size = batch_size
        self._log = log if log is not None else logging.getLogger("mcp_persist.scheduler")
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the background purge loop. Raises if already running."""
        if self._task is not None and not self._task.done():
            raise RuntimeError("PurgeScheduler is already running")
        self._task = asyncio.create_task(self._run())

    async def aclose(self) -> None:
        """Stop the background purge loop, awaiting its cancellation."""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def _run(self) -> None:
        while True:
            delay = self._interval
            if self._jitter:
                delay += random.uniform(0, self._jitter)
            await asyncio.sleep(delay)
            try:
                if self._batch_size is None:
                    removed = await self._store.purge_expired()
                else:
                    removed = await self._store.purge_expired(batch_size=self._batch_size)
                if removed:
                    self._log.info("purged %d expired events", removed)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep the loop alive across transient backend errors
                self._log.exception("purge_expired failed; the scheduler will retry next interval")

    async def __aenter__(self) -> PurgeScheduler:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()


class ArchiveScheduler:
    """Periodically archive expired events from a hot store into a cold store.

    Each cycle moves one batch (up to ``batch_size`` events) from ``hot_store``
    to ``cold_store`` via :func:`~mcp_persist.stored.archive_expired_batch`:
    read expired rows, ID-preserving insert into cold, then delete from hot.
    A crash mid-cycle may duplicate rows in cold (upsert-safe) but never loses
    data from hot before it is archived.

    Args:
        hot_store:  A store with ``select_expired``, ``delete_events``, and a
                    positive ``ttl`` (SQLite or Postgres). Redis hot stores expire
                    keys natively and are not supported here.
        cold_store: A store with ``_store_event_raw``. Use ``ttl=None`` on Redis
                    cold stores so archived events are not re-expired.
        interval:   Seconds between archive cycles. Must be positive.
        batch_size: Maximum events moved per cycle (default ``500``).
        jitter:     Extra sleep jitter, same semantics as :class:`PurgeScheduler`.
        log:        Logger for archive progress and errors.
    """

    def __init__(
        self,
        hot_store: Any,
        cold_store: Any,
        interval: float,
        *,
        batch_size: int = 500,
        jitter: float = 0.0,
        log: logging.Logger | None = None,
    ) -> None:
        if getattr(hot_store, "_ttl", None) is None:
            raise TypeError(
                f"{type(hot_store).__name__} has ttl=None; ArchiveScheduler requires a hot store "
                "with a positive ttl so expired events can be selected"
            )
        if not callable(getattr(hot_store, "select_expired", None)):
            raise TypeError(
                f"{type(hot_store).__name__} has no select_expired(); ArchiveScheduler only applies "
                "to backends that track created_at (SQLite, Postgres)"
            )
        if not callable(getattr(cold_store, "_store_event_raw", None)):
            raise TypeError(
                f"{type(cold_store).__name__} has no _store_event_raw(); cold store must support ID-preserving inserts"
            )
        if interval <= 0:
            raise ValueError(f"interval must be a positive number of seconds, got {interval!r}")
        if jitter < 0:
            raise ValueError(f"jitter must be a non-negative number of seconds, got {jitter!r}")
        if batch_size < 1:
            raise ValueError(f"batch_size must be a positive integer, got {batch_size!r}")

        self._hot = hot_store
        self._cold = cold_store
        self._interval = interval
        self._batch_size = batch_size
        self._jitter = jitter
        self._log = log if log is not None else logging.getLogger("mcp_persist.scheduler")
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            raise RuntimeError("ArchiveScheduler is already running")
        self._task = asyncio.create_task(self._run())

    async def aclose(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def _run(self) -> None:
        while True:
            delay = self._interval
            if self._jitter:
                delay += random.uniform(0, self._jitter)
            await asyncio.sleep(delay)
            try:
                # Drain the whole expired backlog this cycle, not just one batch:
                # a store accumulating more than batch_size expired events per
                # interval would otherwise never catch up. Each batch is its own
                # archive-then-delete unit, so a short batch means we're caught up.
                total = 0
                while True:
                    archived = await archive_expired_batch(
                        self._hot,
                        self._cold,
                        batch_size=self._batch_size,
                    )
                    total += archived
                    if archived < self._batch_size:
                        break
                if total:
                    self._log.info("archived %d expired events to cold store", total)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                self._log.exception("archive_expired_batch failed; the scheduler will retry next interval")

    async def __aenter__(self) -> ArchiveScheduler:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()


class RetentionScheduler:
    """Background scheduler applying per-tenant retention policies with audit logging.

    Applies retention windows specified in a RetentionPolicy, deletes expired rows,
    and captures audit entries written to an AuditSink.
    """

    def __init__(
        self,
        store: Any,
        policy: RetentionPolicy,
        audit_sink: AuditSink,
        interval: float,
        *,
        jitter: float = 0.0,
        batch_size: int | None = None,
        strict_audit: bool = True,
        audit_empty: bool = False,
        log: logging.Logger | None = None,
    ) -> None:
        if not callable(getattr(store, "purge_tenant", None)) or not callable(getattr(store, "distinct_tenants", None)):
            raise TypeError(
                f"{type(store).__name__} lacks purge_tenant or distinct_tenants; "
                "RetentionScheduler does not support this store (e.g. RedisEventStore is unsupported)"
            )
        if interval <= 0:
            raise ValueError(f"interval must be a positive number of seconds, got {interval!r}")
        if jitter < 0:
            raise ValueError(f"jitter must be a non-negative number of seconds, got {jitter!r}")
        if batch_size is not None and batch_size < 1:
            raise ValueError(f"batch_size must be a positive integer or None, got {batch_size!r}")
        if policy is None or not hasattr(policy, "window_for"):
            raise TypeError("policy must be a RetentionPolicy instance")
        if audit_sink is None:
            raise TypeError("audit_sink cannot be None")

        self._store = store
        self._policy = policy
        self._audit_sink = audit_sink
        self._interval = interval
        self._jitter = jitter
        self._batch_size = batch_size
        self._strict_audit = strict_audit
        self._audit_empty = audit_empty
        self._log = log if log is not None else logging.getLogger("mcp_persist.scheduler")
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            raise RuntimeError("RetentionScheduler is already running")
        self._task = asyncio.create_task(self._run())

    async def aclose(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def _run(self) -> None:
        import time

        while True:
            delay = self._interval
            if self._jitter:
                delay += random.uniform(0, self._jitter)
            await asyncio.sleep(delay)
            try:
                now = time.time()
                tenants = await self._store.distinct_tenants()
                for tenant in tenants:
                    window = self._policy.window_for(tenant)
                    if window is None:
                        continue
                    cutoff = now - window
                    deleted = await self._store.purge_tenant(tenant, window_seconds=window, batch_size=self._batch_size)
                    if deleted == 0 and not self._audit_empty:
                        continue
                    entry = DeletionAuditEntry(
                        timestamp=now,
                        tenant_id=tenant,
                        window_seconds=window,
                        cutoff=cutoff,
                        deleted_count=deleted,
                        backend=self._store.backend_name,
                        source_table=self._store.table_name,
                        default_applied=(tenant not in self._policy.windows),
                    )
                    try:
                        await self._audit_sink.record(entry)
                    except Exception:
                        self._log.exception("audit sink failed to record deletion for tenant %r", tenant)
                        if self._strict_audit:
                            raise
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                self._log.exception("RetentionScheduler cycle failed; the scheduler will retry next interval")

    async def __aenter__(self) -> RetentionScheduler:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()
