# pyright: reportPrivateUsage=false
"""The flusher keeps record writing off the request path.

The properties that matter are all about what happens when things go wrong: a
full queue, a dead backend, a shutdown with a backlog. In every case a request
must be unaffected and the loss must be counted rather than silent.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from mcp_persist.recorder import RecordFlusher
from mcp_persist.records import Record, RecordStore

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class FakeRecordStore(RecordStore):
    """In-memory record store that can be told to fail."""

    def __init__(self) -> None:
        self.records: list[Record] = []
        self.fail_times = 0
        self.initialized = False
        self.write_calls = 0

    async def initialize(self) -> None:
        self.initialized = True

    async def store_record(self, record: Record) -> None:
        await self.store_records([record])

    async def store_records(self, records) -> int:
        self.write_calls += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("backend down")
        self.records.extend(records)
        return len(records)

    async def list_records(self, **kwargs) -> list[Record]:
        return list(self.records)

    async def count(self) -> int:
        return len(self.records)

    async def purge(self, *, older_than: float) -> int:
        return 0


def _record(method: str = "tools/call") -> Record:
    return Record(protocol_version="2026-07-28", method=method)


async def _settle() -> None:
    """Let the flusher task run."""
    for _ in range(10):
        await asyncio.sleep(0)


async def test_submitted_records_are_written() -> None:
    store = FakeRecordStore()
    async with RecordFlusher(store) as flusher:
        assert flusher.submit(_record("a")) is True
        assert flusher.submit(_record("b")) is True
        await _settle()

    assert [r.method for r in store.records] == ["a", "b"]
    assert flusher.written == 2
    assert flusher.dropped == 0


async def test_start_initializes_the_store() -> None:
    store = FakeRecordStore()
    async with RecordFlusher(store):
        assert store.initialized is True


async def test_full_queue_drops_and_counts_without_raising() -> None:
    store = FakeRecordStore()
    flusher = RecordFlusher(store, max_queue=2)
    # Deliberately not started: nothing drains, so the queue fills.

    assert flusher.submit(_record()) is True
    assert flusher.submit(_record()) is True
    assert flusher.submit(_record()) is False
    assert flusher.submit(_record()) is False

    assert flusher.dropped == 2
    assert flusher.pending == 2


async def test_submit_never_raises_when_full() -> None:
    flusher = RecordFlusher(FakeRecordStore(), max_queue=1)
    flusher.submit(_record())

    for _ in range(100):
        flusher.submit(_record())  # must not raise

    assert flusher.dropped == 100


async def test_backend_failure_is_counted_and_the_flusher_survives() -> None:
    store = FakeRecordStore()
    store.fail_times = 1
    async with RecordFlusher(store) as flusher:
        flusher.submit(_record("doomed"))
        await _settle()
        assert flusher.failed == 1
        assert store.records == []

        # The writer is still alive and the next batch lands.
        flusher.submit(_record("survivor"))
        await _settle()

    assert [r.method for r in store.records] == ["survivor"]
    assert flusher.written == 1


async def test_aclose_drains_the_backlog() -> None:
    store = FakeRecordStore()
    flusher = RecordFlusher(store)
    await flusher.start()
    for i in range(50):
        flusher.submit(_record(f"m{i}"))

    await flusher.aclose()

    assert len(store.records) == 50
    assert flusher.dropped == 0


async def test_aclose_is_idempotent() -> None:
    flusher = RecordFlusher(FakeRecordStore())
    await flusher.start()
    await flusher.aclose()
    await flusher.aclose()


async def test_aclose_without_start_is_safe() -> None:
    await RecordFlusher(FakeRecordStore()).aclose()


async def test_shutdown_timeout_counts_the_backlog_as_dropped() -> None:
    """Hanging process shutdown to finish writing records is the wrong trade."""

    class StalledStore(FakeRecordStore):
        async def store_records(self, records) -> int:
            await asyncio.sleep(30)
            return 0

    store = StalledStore()
    flusher = RecordFlusher(store, drain_timeout=0.05)
    await flusher.start()
    flusher.submit(_record())
    flusher.submit(_record())

    await flusher.aclose()

    assert flusher.dropped >= 1
    assert store.records == []


async def test_records_are_batched_rather_than_written_one_by_one() -> None:
    store = FakeRecordStore()
    flusher = RecordFlusher(store)
    await flusher.start()
    for i in range(20):
        flusher.submit(_record(f"m{i}"))

    await flusher.aclose()

    assert len(store.records) == 20
    assert store.write_calls < 20


async def test_batch_size_is_respected() -> None:
    store = FakeRecordStore()
    flusher = RecordFlusher(store, batch_size=5)
    await flusher.start()
    for i in range(20):
        flusher.submit(_record(f"m{i}"))

    await flusher.aclose()

    assert len(store.records) == 20
    assert store.write_calls >= 4


async def test_a_cancelled_caller_does_not_cancel_the_flusher() -> None:
    """The lifetime property: a disconnecting request must not kill the writer.

    The modern transport cancels a request's whole task group on disconnect, so
    a flusher parented there would be cancelled mid-drain and take its queued
    records with it. Starting it on the loop is what prevents that.
    """
    store = FakeRecordStore()
    flusher = RecordFlusher(store)
    await flusher.start()

    async def request_like() -> None:
        flusher.submit(_record("from-cancelled-request"))
        await asyncio.sleep(30)  # still in flight when cancelled

    task = asyncio.create_task(request_like())
    await _settle()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await _settle()
    assert [r.method for r in store.records] == ["from-cancelled-request"]

    # And the writer is still usable afterwards.
    flusher.submit(_record("after"))
    await flusher.aclose()
    assert [r.method for r in store.records] == ["from-cancelled-request", "after"]


async def test_stats_reports_the_loss_counters() -> None:
    store = FakeRecordStore()
    flusher = RecordFlusher(store, max_queue=1)
    await flusher.start()
    flusher.submit(_record())
    await _settle()

    stats = flusher.stats()

    assert stats["written"] == 1
    assert stats["dropped"] == 0
    assert stats["failed"] == 0
    assert stats["running"] is True
    await flusher.aclose()
    assert flusher.stats()["running"] is False


async def test_drops_reach_the_metrics_collector() -> None:
    class Collector:
        def __init__(self) -> None:
            self.dropped = 0
            self.written = 0

        def on_record_drop(self, count: int) -> None:
            self.dropped += count

        def on_record_write(self, count: int) -> None:
            self.written += count

    metrics = Collector()
    flusher = RecordFlusher(FakeRecordStore(), max_queue=1, metrics=metrics)  # type: ignore[arg-type]
    flusher.submit(_record())
    flusher.submit(_record())

    assert metrics.dropped == 1

    await flusher.start()
    await flusher.aclose()
    assert metrics.written == 1


async def test_a_collector_without_record_hooks_still_works() -> None:
    """Collectors written before records existed must keep working."""

    class OldCollector:
        def on_store_event(self, *a, **k) -> None: ...
        def on_replay(self, *a, **k) -> None: ...
        def on_error(self, *a, **k) -> None: ...

    flusher = RecordFlusher(FakeRecordStore(), max_queue=1, metrics=OldCollector())  # type: ignore[arg-type]
    flusher.submit(_record())
    flusher.submit(_record())  # dropped, must not explode

    async with flusher:
        await _settle()


async def test_invalid_configuration_is_rejected() -> None:
    with pytest.raises(ValueError):
        RecordFlusher(FakeRecordStore(), max_queue=0)
    with pytest.raises(ValueError):
        RecordFlusher(FakeRecordStore(), batch_size=0)


# ── Loss accounting: every path that loses a record must count it ─────────────


async def test_partial_write_is_not_reported_as_success() -> None:
    """A store that accepts fewer than submitted must not inflate `written`."""

    class HalfStore(FakeRecordStore):
        async def store_records(self, records) -> int:
            self.records.extend(records[:1])
            return 1  # accepted one of them

    store = HalfStore()
    flusher = RecordFlusher(store)
    await flusher.start()
    flusher.submit(_record("a"))
    flusher.submit(_record("b"))
    await flusher.aclose()

    assert flusher.written == 1
    assert flusher.failed == 1


async def test_a_store_returning_none_is_trusted_for_the_whole_batch() -> None:
    """Older stores may return None; that is not a partial write."""

    class NoneStore(FakeRecordStore):
        async def store_records(self, records):
            self.records.extend(records)
            return None

    store = NoneStore()
    async with RecordFlusher(store) as flusher:
        flusher.submit(_record())
        await _settle()

    assert flusher.written == 1
    assert flusher.failed == 0


async def test_cancelling_the_writer_mid_write_counts_the_lost_batch() -> None:
    """An externally cancelled flusher must not lose its in-flight batch silently."""
    started = asyncio.Event()

    class StalledStore(FakeRecordStore):
        async def store_records(self, records) -> int:
            started.set()
            await asyncio.sleep(30)
            return 0

    flusher = RecordFlusher(StalledStore())
    await flusher.start()
    flusher.submit(_record())
    flusher.submit(_record())
    await asyncio.wait_for(started.wait(), timeout=5)

    task = flusher._task
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert flusher.dropped >= 1
    assert flusher.written == 0


async def test_a_raising_log_handler_cannot_break_submit() -> None:
    """submit() runs on the request path, so logging must never raise out of it."""

    class Exploding(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            raise RuntimeError("logging is broken")

    handler = Exploding()
    log = logging.getLogger("mcp_persist.recorder")
    log.addHandler(handler)
    try:
        flusher = RecordFlusher(FakeRecordStore(), max_queue=1)
        flusher.submit(_record())
        assert flusher.submit(_record()) is False  # must not raise
    finally:
        log.removeHandler(handler)

    assert flusher.dropped == 1


async def test_shutdown_timeout_does_not_double_count_the_in_flight_batch() -> None:
    """The timeout path counts the backlog; the cancel must not charge it again."""

    class StalledStore(FakeRecordStore):
        async def store_records(self, records) -> int:
            await asyncio.sleep(30)
            return 0

    flusher = RecordFlusher(StalledStore(), drain_timeout=0.05)
    await flusher.start()
    flusher.submit(_record())
    flusher.submit(_record())
    await _settle()

    await flusher.aclose()

    assert flusher.dropped == 2


async def test_a_restarted_flusher_still_counts_a_cancelled_batch() -> None:
    """The shutdown double-count guard must not persist across a restart.

    A timed-out aclose() sets the guard so the cancel it issues does not charge
    the same batch twice. If that flag survived into a later run, the next
    mid-write cancellation would be silently uncounted, which is the exact bug
    the guard exists to avoid on the other side.
    """

    class StalledStore(FakeRecordStore):
        async def store_records(self, records) -> int:
            await asyncio.sleep(30)
            return 0

    flusher = RecordFlusher(StalledStore(), drain_timeout=0.05)
    await flusher.start()
    flusher.submit(_record())
    flusher.submit(_record())
    await _settle()
    await flusher.aclose()
    assert flusher.dropped == 2

    # Same flusher, new run, a fresh mid-write cancellation.
    await flusher.start()
    flusher.submit(_record())
    await _settle()
    task = flusher._task
    assert task is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert flusher.dropped == 3


async def test_start_during_a_close_does_not_leak_a_second_writer() -> None:
    """aclose() must not hand a window to start() that leaves an orphan writer."""

    class SlowStore(FakeRecordStore):
        async def store_records(self, records) -> int:
            await asyncio.sleep(0.2)
            return await super().store_records(records)

    flusher = RecordFlusher(SlowStore(), drain_timeout=5)
    await flusher.start()
    first = flusher._task
    flusher.submit(_record())

    closing = asyncio.create_task(flusher.aclose())
    await asyncio.sleep(0)
    await flusher.start()  # concurrent with the close
    assert flusher._task is first or flusher._task is None

    await closing
    assert flusher._task is None
    assert first is not None and first.done()


async def test_a_raising_metrics_collector_cannot_break_submit() -> None:
    """The collector is user code, including its attribute lookup."""

    class Hostile:
        @property
        def on_record_drop(self):
            raise RuntimeError("exploding property")

    flusher = RecordFlusher(FakeRecordStore(), max_queue=1, metrics=Hostile())  # type: ignore[arg-type]
    flusher.submit(_record())

    assert flusher.submit(_record()) is False  # must not raise
    assert flusher.dropped == 1


async def test_a_raising_log_handler_cannot_kill_the_writer() -> None:
    """A dead writer would strand every record queued after it."""

    class Exploding(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            raise RuntimeError("logging is broken")

    store = FakeRecordStore()
    store.fail_times = 1  # forces the writer down its logging path
    handler = Exploding()
    log = logging.getLogger("mcp_persist.recorder")
    log.addHandler(handler)
    try:
        flusher = RecordFlusher(store)
        await flusher.start()
        flusher.submit(_record("doomed"))
        await _settle()
        flusher.submit(_record("survivor"))
        await flusher.aclose()
    finally:
        log.removeHandler(handler)

    assert [r.method for r in store.records] == ["survivor"]
