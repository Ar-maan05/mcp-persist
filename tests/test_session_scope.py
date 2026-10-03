"""SessionScopedEventStore: one session's view of an event store shared by all."""

from __future__ import annotations

from typing import Any

import pytest
from mcp.server.streamable_http import EventMessage, EventStore
from mcp_types import JSONRPCRequest

from mcp_persist import (
    BatchingEventStore,
    RedisEventStore,
    SessionScopedEventStore,
    SessionScopedSessionManager,
    SQLiteEventStore,
)
from mcp_persist.session_scope import _scope_transport

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _message(tag: str) -> JSONRPCRequest:
    return JSONRPCRequest(jsonrpc="2.0", id=tag, method="tools/call")


async def _replay(view: SessionScopedEventStore, last_event_id: str) -> tuple[str | None, list[str]]:
    sent: list[EventMessage] = []

    async def collect(event: EventMessage) -> None:
        sent.append(event)

    stream_id = await view.replay_events_after(last_event_id, collect)
    return stream_id, [str(getattr(e.message, "id", "")) for e in sent]


async def test_a_session_resumes_its_own_stream() -> None:
    async with SQLiteEventStore.create(":memory:") as shared:
        alice = SessionScopedEventStore(shared, "alice")
        first = await alice.store_event("7", _message("a1"))
        await alice.store_event("7", _message("a2"))

        stream_id, sent = await _replay(alice, first)

    # The transport matches the stream against its own request ids, unprefixed.
    assert stream_id == "7"
    assert sent == ["a2"]


async def test_streams_are_named_after_their_session() -> None:
    async with SQLiteEventStore.create(":memory:") as shared:
        await SessionScopedEventStore(shared, "alice").store_event("7", _message("a1"))
        await SessionScopedEventStore(shared, "bob").store_event("7", _message("b1"))
        streams = sorted([s async for s in shared.list_streams()])

    assert streams == ["alice:7", "bob:7"]


async def test_the_same_request_id_in_two_sessions_does_not_mix_their_events() -> None:
    async with SQLiteEventStore.create(":memory:") as shared:
        alice = SessionScopedEventStore(shared, "alice")
        mallory = SessionScopedEventStore(shared, "mallory")
        planted = await mallory.store_event("7", _message("m1"))
        await alice.store_event("7", _message("alice-secret"))
        await mallory.store_event("7", _message("m2"))

        stream_id, sent = await _replay(mallory, planted)

    assert stream_id == "7"
    assert sent == ["m2"]


async def test_another_sessions_event_id_replays_nothing() -> None:
    async with SQLiteEventStore.create(":memory:") as shared:
        alice = SessionScopedEventStore(shared, "alice")
        first = await alice.store_event("7", _message("a1"))
        await alice.store_event("7", _message("alice-secret"))

        stream_id, sent = await _replay(SessionScopedEventStore(shared, "mallory"), first)

    assert stream_id is None
    assert sent == []


async def test_a_session_id_that_prefixes_another_does_not_match_it() -> None:
    # "ab" must not own "abc:7": the separator ends the session id.
    async with SQLiteEventStore.create(":memory:") as shared:
        first = await SessionScopedEventStore(shared, "abc").store_event("7", _message("x1"))
        await SessionScopedEventStore(shared, "abc").store_event("7", _message("x2"))

        stream_id, sent = await _replay(SessionScopedEventStore(shared, "ab"), first)

    assert (stream_id, sent) == (None, [])


async def test_an_unknown_event_id_replays_nothing() -> None:
    async with SQLiteEventStore.create(":memory:") as shared:
        stream_id, sent = await _replay(SessionScopedEventStore(shared, "alice"), "12345")

    assert (stream_id, sent) == (None, [])


def test_scoping_fails_closed_on_a_transport_without_an_event_store_attribute() -> None:
    class _UnknownTransport:
        pass

    with pytest.raises(RuntimeError, match="cannot isolate sessions"):
        _scope_transport(_UnknownTransport(), "alice", object())  # type: ignore[arg-type]


def test_the_manager_scopes_each_transport_it_registers() -> None:
    shared: Any = object()

    class _Transport:
        def __init__(self) -> None:
            self._event_store: Any = shared

    manager = SessionScopedSessionManager(app=object(), event_store=shared)  # type: ignore[arg-type]
    transport = _Transport()
    manager._server_instances["alice"] = transport  # type: ignore[assignment]

    assert isinstance(transport._event_store, SessionScopedEventStore)
    assert transport._event_store.session_id == "alice"
    assert transport._event_store.store is shared


def test_a_manager_without_an_event_store_leaves_transports_alone() -> None:
    class _Transport:
        _event_store = None

    manager = SessionScopedSessionManager(app=object())  # type: ignore[arg-type]
    transport = _Transport()
    manager._server_instances["alice"] = transport  # type: ignore[assignment]

    assert transport._event_store is None


class _NoLookupStore(EventStore):
    """A minimal third-party store: replays, but cannot say whose an event is up front."""

    def __init__(self, inner: SQLiteEventStore) -> None:
        self._inner = inner

    async def store_event(self, stream_id: str, message: Any) -> str:
        return await self._inner.store_event(stream_id, message)

    async def replay_events_after(self, last_event_id: str, send_callback: Any) -> str | None:
        return await self._inner.replay_events_after(last_event_id, send_callback)


async def test_a_foreign_id_is_refused_without_reading_the_stream() -> None:
    # With a store that can name an event's stream, a foreign Last-Event-ID is
    # refused before the replay runs, so the other session's events are never
    # read, decrypted or held in memory.
    async with SQLiteEventStore.create(":memory:") as shared:
        first = await SessionScopedEventStore(shared, "alice").store_event("7", _message("a1"))
        await SessionScopedEventStore(shared, "alice").store_event("7", _message("alice-secret"))

        calls = 0
        original = shared.replay_events_after

        async def counting(*args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            calls += 1
            return await original(*args, **kwargs)

        shared.replay_events_after = counting  # type: ignore[method-assign]
        stream_id, sent = await _replay(SessionScopedEventStore(shared, "mallory"), first)

    assert (stream_id, sent) == (None, [])
    assert calls == 0


async def test_an_owned_replay_is_streamed_through_not_buffered() -> None:
    # Each event reaches the transport while the store is still replaying, so a
    # long replay keeps the transport's backpressure instead of being held in
    # memory first.
    async with SQLiteEventStore.create(":memory:") as shared:
        alice = SessionScopedEventStore(shared, "alice")
        first = await alice.store_event("7", _message("a1"))
        for tag in ("a2", "a3"):
            await alice.store_event("7", _message(tag))

        store_replaying = False
        original = shared.replay_events_after

        async def tracking(*args: Any, **kwargs: Any) -> Any:
            nonlocal store_replaying
            store_replaying = True
            try:
                return await original(*args, **kwargs)
            finally:
                store_replaying = False

        shared.replay_events_after = tracking  # type: ignore[method-assign]
        delivered_mid_replay: list[bool] = []

        async def callback(event: EventMessage) -> None:
            delivered_mid_replay.append(store_replaying)

        stream_id = await alice.replay_events_after(first, callback)

    assert stream_id == "7"
    assert delivered_mid_replay == [True, True]


@pytest.mark.parametrize("owner", ["alice", "mallory"])
async def test_a_store_without_a_lookup_is_still_isolated(owner: str) -> None:
    async with SQLiteEventStore.create(":memory:") as inner:
        shared = _NoLookupStore(inner)
        first = await SessionScopedEventStore(shared, "alice").store_event("7", _message("a1"))
        await SessionScopedEventStore(shared, "alice").store_event("7", _message("alice-secret"))

        stream_id, sent = await _replay(SessionScopedEventStore(shared, owner), first)

    if owner == "alice":
        assert (stream_id, sent) == ("7", ["alice-secret"])
    else:
        assert (stream_id, sent) == (None, [])


async def test_batching_store_lookups_pass_through() -> None:
    # BatchingEventStore wraps Redis or Postgres and hands the lookup through.
    import fakeredis.aioredis as fakeredis

    client = fakeredis.FakeRedis()
    shared = BatchingEventStore(RedisEventStore(client, ttl=3600), flush_max_events=100)
    try:
        alice = SessionScopedEventStore(shared, "alice")
        first = await alice.store_event("7", _message("a1"))
        await alice.store_event("7", _message("a2"))

        own = await _replay(alice, first)
        foreign = await _replay(SessionScopedEventStore(shared, "mallory"), first)
    finally:
        await shared.aclose()
        try:
            await client.aclose()
        except AttributeError:  # redis-py < 5.0
            await client.close(close_connection_pool=True)

    assert own == ("7", ["a2"])
    assert foreign == (None, [])
