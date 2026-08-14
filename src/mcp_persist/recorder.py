"""Off-the-hot-path writer for records.

A recorder sits in front of every request the server handles, so writing a
record must never delay a response and must never fail one. That rules out
awaiting the database in the request path: a slow or unreachable backend would
become a slow or failing MCP server, which is a far worse outcome than losing
some observability.

:class:`RecordFlusher` is the seam that makes that safe. Submitting is a
synchronous, non-blocking put onto a bounded queue, and a background task owned
by the application lifespan drains it in batches.

Two consequences follow, and both are deliberate:

* **Records can be dropped.** A full queue drops rather than blocks. Dropping is
  the only correct answer once the alternative is stalling a request, but a drop
  that nobody can see would just be a quieter version of the silent-bypass bug
  this whole surface exists to fix, so drops are counted and surfaced through
  :meth:`RecordFlusher.stats`, the metrics collector, and ``health()``.
* **The flusher task must outlive any request.** It is started from the lifespan
  and never from inside a request, because the modern transport cancels a
  request's whole task group on client disconnect: a flusher parented there
  would be cancelled mid-drain and take its queued records with it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from mcp_persist.metrics import safe_call

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mcp_persist.metrics import MetricsCollector
    from mcp_persist.records import Record, RecordStore

logger = logging.getLogger(__name__)

DEFAULT_MAX_QUEUE = 10_000
DEFAULT_BATCH_SIZE = 100
DEFAULT_DRAIN_TIMEOUT = 2.0


class RecordFlusher:
    """Queue records from the request path and write them in the background.

    Args:
        store: Where records land. Opened by the caller.
        max_queue: How many records may wait to be written. Beyond this,
            submissions are dropped and counted.
        batch_size: Upper bound on records per write.
        drain_timeout: How long :meth:`aclose` waits for the backlog to be
            written before giving up. Anything still queued at that point is
            counted as dropped, because hanging a process shutdown to finish
            writing observability data is the wrong trade.
        metrics: Optional collector. ``on_record_write`` and ``on_record_drop``
            are called when present, so an existing collector that predates
            records keeps working untouched.
    """

    def __init__(
        self,
        store: RecordStore,
        *,
        max_queue: int = DEFAULT_MAX_QUEUE,
        batch_size: int = DEFAULT_BATCH_SIZE,
        drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
        metrics: MetricsCollector | None = None,
    ) -> None:
        if max_queue <= 0:
            raise ValueError("max_queue must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self._store = store
        self._queue: asyncio.Queue[Record] = asyncio.Queue(maxsize=max_queue)
        self._batch_size = batch_size
        self._drain_timeout = drain_timeout
        self._metrics = metrics
        self._task: asyncio.Task[None] | None = None
        self._written = 0
        self._dropped = 0
        self._failed = 0
        self._inflight = 0
        self._accounted_at_shutdown = False
        self._closing = False

    async def start(self) -> None:
        """Begin draining. Call from the application lifespan, never a request.

        The task is created on the running loop rather than inside any task
        group, so a cancelled request cannot cancel the writer.
        """
        if self._task is not None or self._closing:
            return
        # Clear the shutdown accounting guard: it suppresses the mid-write drop
        # count for the batch a timed-out aclose already charged, and leaving it
        # set would silently suppress a legitimate count on this new run.
        self._accounted_at_shutdown = False
        await self._store.initialize()
        self._task = asyncio.create_task(self._run(), name="mcp-persist-record-flusher")

    def submit(self, record: Record) -> bool:
        """Queue ``record`` without blocking. Returns False if it was dropped.

        This is the only method the request path calls, and it never awaits,
        never raises, and never takes a lock. A caller that ignores the return
        value still behaves correctly; the drop is counted either way.
        """
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull:
            self._dropped += 1
            if self._dropped == 1 or self._dropped % 1000 == 0:
                self._log(
                    logging.WARNING,
                    "record queue full; dropped %d record(s) so far. The record backend cannot keep up: "
                    "raise max_queue, or check backend health with `mcp-persist doctor`.",
                    self._dropped,
                )
            self._emit("on_record_drop", 1)
            return False
        return True

    def _emit(self, hook_name: str, *args: object) -> None:
        """Call an optional metrics hook, if the collector has one.

        Absent hooks are skipped rather than dispatched, so neither a missing
        collector nor one written before records existed produces a spurious
        "collector hook raised" traceback on every drop.

        The whole dispatch is guarded, not just the call: the collector is
        user-supplied, so even the attribute lookup can run a property that
        raises, and ``safe_call`` logs, which is itself user-controlled code.
        This runs on the request path, where nothing is allowed to raise.
        """
        try:
            hook = getattr(self._metrics, hook_name, None)
            if hook is not None:
                safe_call(hook, *args)
        except Exception:
            pass

    def _log(self, level: int, message: str, *args: object) -> None:
        """Log without ever letting a handler break the caller.

        Used on the request path and inside the writer loop: a raising handler
        must not fail a request, and must not kill the writer and strand every
        record queued after it.
        """
        with contextlib.suppress(Exception):
            logger.log(level, message, *args)

    async def aclose(self) -> None:
        """Stop draining, writing what is already queued within the budget."""
        task = self._task
        if task is None:
            return
        # The task reference is cleared only once the writer is actually stopped.
        # Clearing it up front would let a concurrent start() launch a second
        # writer that this close would never cancel.
        self._closing = True
        try:
            await asyncio.wait_for(self._queue.join(), timeout=self._drain_timeout)
        except (TimeoutError, asyncio.TimeoutError):
            # Records already pulled into the batch being written are no longer
            # in the queue, so qsize() alone would undercount what is about to
            # be lost when the task is cancelled.
            remaining = self._queue.qsize() + self._inflight
            self._dropped += remaining
            self._emit("on_record_drop", remaining)
            self._log(
                logging.WARNING,
                "record flusher shutdown exceeded %.1fs with %d record(s) unwritten; they are counted as dropped",
                self._drain_timeout,
                remaining,
            )
            # The cancel below reaches the in-flight batch, which would otherwise
            # count itself a second time on the way out.
            self._accounted_at_shutdown = True
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        self._task = None
        self._closing = False

    async def _run(self) -> None:
        while True:
            batch = await self._collect()
            self._inflight = len(batch)
            try:
                stored = await self._store.store_records(batch)
            except asyncio.CancelledError:
                # Cancelled mid-write: this batch is already out of the queue,
                # so nothing else will account for it. Count it before the
                # cancellation propagates, or the loss is silent. The exception
                # is a shutdown that already counted the backlog, which would
                # otherwise be charged twice.
                if not self._accounted_at_shutdown:
                    self._dropped += len(batch)
                    self._emit("on_record_drop", len(batch))
                    self._log(logging.WARNING, "record flusher cancelled mid-write; %d record(s) lost", len(batch))
                raise
            except Exception as exc:
                # A failing backend must not kill the writer: the next batch may
                # well succeed, and an exception here would silently end
                # recording for the life of the process.
                self._failed += len(batch)
                self._log(logging.WARNING, "failed to write %d record(s): %s", len(batch), exc)
                self._emit("on_error", "store_records", exc)
            else:
                # Trust the store's own count over the batch length: a partial
                # write must not be reported as a complete one.
                written = len(batch) if stored is None else min(int(stored), len(batch))
                self._written += written
                if written:
                    self._emit("on_record_write", written)
                if written < len(batch):
                    missing = len(batch) - written
                    self._failed += missing
                    self._log(
                        logging.WARNING,
                        "record store accepted only %d of %d record(s) in a batch",
                        written,
                        len(batch),
                    )
                    self._emit("on_record_drop", missing)
            finally:
                self._inflight = 0
                for _ in batch:
                    self._queue.task_done()

    async def _collect(self) -> list[Record]:
        """Wait for one record, then take whatever else is already waiting."""
        batch = [await self._queue.get()]
        while len(batch) < self._batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return batch

    @property
    def dropped(self) -> int:
        """Records discarded because the queue was full or shutdown timed out."""
        return self._dropped

    @property
    def written(self) -> int:
        """Records successfully written."""
        return self._written

    @property
    def failed(self) -> int:
        """Records the backend refused. Distinct from dropped."""
        return self._failed

    @property
    def pending(self) -> int:
        """Records queued but not yet written."""
        return self._queue.qsize()

    def stats(self) -> dict[str, Any]:
        """A snapshot for ``health()``, the dashboard and the CLI."""
        return {
            "written": self._written,
            "dropped": self._dropped,
            "failed": self._failed,
            "pending": self.pending,
            "running": self._task is not None and not self._task.done(),
        }

    async def __aenter__(self) -> RecordFlusher:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


async def drain_records(flusher: RecordFlusher, records: Sequence[Record]) -> int:
    """Submit ``records`` and report how many were accepted. For tests."""
    return sum(1 for record in records if flusher.submit(record))
