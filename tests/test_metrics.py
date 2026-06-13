# pyright: reportUnknownParameterType=false
# pyright: reportMissingParameterType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
# pyright: reportPrivateUsage=false
"""Tests for the optional MetricsCollector observability hooks.

The collector wiring is identical across all three backends, so the store-level
behaviour is exercised here against SQLite (in-memory) and Redis (fakeredis),
which both run without an external service. The Postgres path is covered for
parity in test_postgres_event_store.py (CI only). The collector classes
themselves are unit-tested directly.
"""

from __future__ import annotations

import logging

import aiosqlite
import fakeredis.aioredis as fakeredis
import pytest
from mcp.server.streamable_http import EventMessage
from mcp.types import JSONRPCRequest

from mcp_persist import (
    LoggingMetricsCollector,
    NoOpMetricsCollector,
    RedisEventStore,
    SQLiteEventStore,
)
from mcp_persist.metrics import dispatch_proxy_replay, safe_call

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


class RecordingCollector:
    """Captures every hook call so tests can assert on the arguments."""

    def __init__(self) -> None:
        self.store_calls: list[tuple] = []
        self.replay_calls: list[tuple] = []
        self.errors: list[tuple] = []

    def on_store_event(self, stream_id, event_id, duration_ms) -> None:
        self.store_calls.append((stream_id, event_id, duration_ms))

    def on_replay(self, stream_id, events_replayed, duration_ms) -> None:
        self.replay_calls.append((stream_id, events_replayed, duration_ms))

    def on_error(self, operation, error) -> None:
        self.errors.append((operation, error))


class RaisingCollector:
    """A misbehaving collector; every hook raises."""

    def on_store_event(self, *args) -> None:
        raise RuntimeError("metrics boom")

    def on_replay(self, *args) -> None:
        raise RuntimeError("metrics boom")

    def on_error(self, *args) -> None:
        raise RuntimeError("metrics boom")


# ── Collector classes ─────────────────────────────────────────────────────────


def test_noop_collector_returns_none():
    collector = NoOpMetricsCollector()
    assert collector.on_store_event("s", "1", 1.0) is None
    assert collector.on_replay("s", 3, 1.0) is None
    assert collector.on_error("store_event", RuntimeError()) is None


def test_logging_collector_logs_each_op_at_debug(caplog):
    collector = LoggingMetricsCollector()
    with caplog.at_level(logging.DEBUG, logger="mcp_persist.metrics"):
        collector.on_store_event("stream-A", "5", 1.23)
        collector.on_replay("stream-A", 2, 4.56)
        collector.on_error("store_event", RuntimeError("nope"))
    messages = [r.getMessage() for r in caplog.records]
    assert any("store_event" in m and "stream-A" in m for m in messages)
    assert any("replay" in m and "events=2" in m for m in messages)
    assert any("error in store_event" in m for m in messages)


def test_logging_collector_accepts_custom_logger(caplog):
    custom = logging.getLogger("my.custom.metrics")
    collector = LoggingMetricsCollector(custom)
    with caplog.at_level(logging.DEBUG, logger="my.custom.metrics"):
        collector.on_store_event("s", "1", 0.5)
    assert any(r.name == "my.custom.metrics" for r in caplog.records)


def test_safe_call_swallows_and_logs_exceptions(caplog):
    def boom():
        raise RuntimeError("hook failed")

    with caplog.at_level(logging.ERROR, logger="mcp_persist.metrics"):
        safe_call(boom)  # must not raise
    assert any("hook raised" in r.getMessage() for r in caplog.records)


def test_safe_call_warns_on_async_collector_hook(caplog, recwarn):
    async def async_hook(*args):
        return None

    with caplog.at_level(logging.WARNING, logger="mcp_persist.metrics"):
        safe_call(async_hook, "stream-A", "1", 1.0)  # must not raise

    assert any("coroutine" in r.getMessage() for r in caplog.records)
    # The coroutine was closed, so no "coroutine was never awaited" RuntimeWarning.
    assert not [w for w in recwarn.list if issubclass(w.category, RuntimeWarning)]


# ── SQLite wiring ─────────────────────────────────────────────────────────────


@pytest.fixture
async def sqlite_store_factory():
    conns: list = []

    async def make(metrics):
        conn = await aiosqlite.connect(":memory:")
        conns.append(conn)
        store = SQLiteEventStore(conn, table_name="ev", ttl=None, metrics=metrics)
        await store.initialize()
        return store

    try:
        yield make
    finally:
        for conn in conns:
            await conn.close()


@pytest.mark.anyio
async def test_sqlite_store_event_fires_metric(sqlite_store_factory):
    collector = RecordingCollector()
    store = await sqlite_store_factory(collector)

    event_id = await store.store_event("stream-A", SAMPLE_MSG)

    assert len(collector.store_calls) == 1
    stream_id, recorded_id, duration_ms = collector.store_calls[0]
    assert stream_id == "stream-A"
    assert recorded_id == event_id
    assert isinstance(recorded_id, str)
    assert isinstance(duration_ms, float)
    assert duration_ms >= 0.0
    assert collector.errors == []


@pytest.mark.anyio
async def test_sqlite_replay_fires_metric_with_count(sqlite_store_factory):
    collector = RecordingCollector()
    store = await sqlite_store_factory(collector)

    anchor = await store.store_event("stream-A", None)  # priming anchor
    await store.store_event("stream-A", SAMPLE_MSG)
    await store.store_event("stream-A", SAMPLE_MSG)

    captured: list[EventMessage] = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    stream_id = await store.replay_events_after(anchor, cb)

    assert stream_id == "stream-A"
    assert len(captured) == 2
    assert len(collector.replay_calls) == 1
    recorded_stream, count, duration_ms = collector.replay_calls[0]
    assert recorded_stream == "stream-A"
    assert count == 2  # priming anchor excluded; two real events replayed
    assert isinstance(duration_ms, float)


@pytest.mark.anyio
async def test_sqlite_on_error_fires_and_reraises(sqlite_store_factory, monkeypatch):
    collector = RecordingCollector()
    store = await sqlite_store_factory(collector)

    async def boom(stream_id, message):
        raise RuntimeError("db down")

    monkeypatch.setattr(store, "_store_event_impl", boom)

    with pytest.raises(RuntimeError, match="db down"):
        await store.store_event("stream-A", SAMPLE_MSG)

    assert len(collector.errors) == 1
    operation, error = collector.errors[0]
    assert operation == "store_event"
    assert isinstance(error, RuntimeError)
    assert collector.store_calls == []  # success hook not fired on failure


@pytest.mark.anyio
async def test_sqlite_raising_collector_does_not_break_store(sqlite_store_factory):
    store = await sqlite_store_factory(RaisingCollector())

    # store_event and replay must succeed even though every hook raises.
    anchor = await store.store_event("stream-A", None)
    event_id = await store.store_event("stream-A", SAMPLE_MSG)
    assert isinstance(event_id, str)

    captured: list[EventMessage] = []

    async def cb(event: EventMessage) -> None:
        captured.append(event)

    stream_id = await store.replay_events_after(anchor, cb)
    assert stream_id == "stream-A"
    assert len(captured) == 1


@pytest.mark.anyio
async def test_sqlite_default_collector_is_noop(sqlite_store_factory):
    store = await sqlite_store_factory(None)
    assert type(store._metrics) is NoOpMetricsCollector
    # Operations still work on the no-op fast path.
    event_id = await store.store_event("stream-A", SAMPLE_MSG)
    assert isinstance(event_id, str)


# ── Redis wiring ──────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_redis_store_and_replay_fire_metrics():
    client = fakeredis.FakeRedis()
    collector = RecordingCollector()
    store = RedisEventStore(client, key_prefix="test:", ttl=None, metrics=collector)
    try:
        anchor = await store.store_event("stream-A", None)
        await store.store_event("stream-A", SAMPLE_MSG)

        captured: list[EventMessage] = []

        async def cb(event: EventMessage) -> None:
            captured.append(event)

        stream_id = await store.replay_events_after(anchor, cb)

        assert stream_id == "stream-A"
        assert len(collector.store_calls) == 2
        assert len(collector.replay_calls) == 1
        assert collector.replay_calls[0][0] == "stream-A"
        assert collector.replay_calls[0][1] == 1  # one real event replayed
    finally:
        try:
            await client.aclose()
        except AttributeError:
            await client.close()


# ── Proxy replay hook (optional, feature-detected) ─────────────────────────────


def test_dispatch_proxy_replay_calls_hook_when_present():
    calls: list[tuple] = []

    class WithHook:
        def on_proxy_replay(self, stream_id, session_id, events_replayed, blocked, duration_ms) -> None:
            calls.append((stream_id, session_id, events_replayed, blocked, duration_ms))

    dispatch_proxy_replay(WithHook(), "s:1", "s", 3, False, 1.5)
    assert calls == [("s:1", "s", 3, False, 1.5)]


def test_dispatch_proxy_replay_noops_on_three_method_collector():
    # A collector that predates the proxy hook (only the three Protocol methods)
    # must not break: feature detection finds no on_proxy_replay and does nothing.
    collector = RecordingCollector()
    assert not hasattr(collector, "on_proxy_replay")
    dispatch_proxy_replay(collector, "s:1", "s", 1, False, 0.1)  # no raise


def test_dispatch_proxy_replay_noops_on_none():
    dispatch_proxy_replay(None, "s:1", "s", 1, False, 0.1)  # no raise


def test_dispatch_proxy_replay_swallows_raising_hook():
    class Boom:
        def on_proxy_replay(self, *args) -> None:
            raise RuntimeError("metrics boom")

    dispatch_proxy_replay(Boom(), "s:1", "s", 1, False, 0.1)  # logged + ignored, no raise


def test_builtin_collectors_implement_proxy_replay(caplog):
    NoOpMetricsCollector().on_proxy_replay("s:1", "s", 2, False, 0.5)  # no raise
    with caplog.at_level(logging.DEBUG, logger="mcp_persist.metrics"):
        LoggingMetricsCollector().on_proxy_replay("s:1", "s", 2, True, 0.5)
    assert "proxy_replay" in caplog.text
    assert "blocked=True" in caplog.text
