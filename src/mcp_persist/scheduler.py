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

if TYPE_CHECKING:
    from typing import Protocol

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
