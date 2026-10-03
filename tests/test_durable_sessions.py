"""Durable sessions: a session id outlives the process that created it.

The point of these tests is the thing a persistent event store alone does not
buy you. Upstream, the session registry is an in-process dict, so after a
restart the client's ``Mcp-Session-Id`` is unknown and every request 404s: the
events are still on disk but unreachable. With ``durable_sessions=True`` a
second process recognizes the id and carries on.

The servers are run under uvicorn on an ephemeral port and driven with raw HTTP,
because what is being asserted is exactly the status code and headers of a
reconnect, not client-library behaviour.
"""

from __future__ import annotations

import contextlib
import json
import socket
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import anyio
import httpx2 as httpx
import pytest
import uvicorn
from mcp.server.mcpserver import MCPServer
from starlette.applications import Starlette

from mcp_persist import SQLiteEventStore, with_persistence
from mcp_persist.sessions import SessionRecord, _owner_matches, session_registry_for

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _one_process_many_servers() -> Iterator[None]:
    """Let several uvicorn servers run one after another in this process.

    sse-starlette watches for a uvicorn shutdown and, once it sees one, ends
    every SSE response in the process for good. A real restart is a new
    process, but these tests "restart" by starting a second server in this one,
    which would then have every stream cut off. Stop it from watching.
    """
    from sse_starlette.sse import AppStatus

    saved = AppStatus.enable_automatic_graceful_drain
    AppStatus.enable_automatic_graceful_drain = False
    AppStatus.should_exit = False
    try:
        yield
    finally:
        AppStatus.enable_automatic_graceful_drain = saved
        AppStatus.should_exit = False


_INIT_BODY: dict[str, Any] = {
    "jsonrpc": "2.0",
    "id": "init-1",
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "durable-session-test", "version": "1.0"},
    },
}
_HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _make_mcp() -> MCPServer:
    mcp = MCPServer(name="DurableSessionServer")

    @mcp.tool()
    def echo(message: str) -> dict[str, str]:
        return {"echo": message}

    return mcp


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextlib.asynccontextmanager
async def _serve(app: Starlette, port: int) -> AsyncIterator[str]:
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    async with anyio.create_task_group() as tg:
        tg.start_soon(server.serve)
        while not server.started:
            await anyio.sleep(0.02)
        try:
            yield f"http://127.0.0.1:{port}/mcp"
        finally:
            # Leaving the task group waits for serve() to return, so the port is
            # free before the next server in a restart test binds it.
            server.should_exit = True


async def _initialize(url: str) -> str:
    """POST initialize and return the server-assigned session id."""
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        response = await client.post(url, json=_INIT_BODY, headers=_HEADERS)
        assert response.status_code == 200, response.text
        session_id = response.headers.get("mcp-session-id")
        assert session_id, f"no session id in {dict(response.headers)}"
        return session_id


async def _reconnect_status(url: str, session_id: str) -> int:
    """Status code for a GET that resumes ``session_id`` (the reconnect path)."""
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        headers = {**_HEADERS, "Mcp-Session-Id": session_id}
        # A resuming client opens the server-to-client stream; read only the
        # status line and hang up, since the stream itself stays open.
        async with client.stream("GET", url, headers=headers) as response:
            return response.status_code


async def test_session_survives_a_restart(tmp_path: Path) -> None:
    """The headline case: same store, new process, session still recognized."""
    db = str(tmp_path / "events.db")
    port = _free_port()

    app1 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app1, port) as url:
        session_id = await _initialize(url)

    # First server is gone. Nothing is shared but the SQLite file.
    app2 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app2, port) as url:
        assert await _reconnect_status(url, session_id) == 200


async def test_without_durable_sessions_a_restart_loses_the_session(tmp_path: Path) -> None:
    """The behaviour being fixed, pinned so the feature cannot silently regress.

    Same durable event store, but the in-process session registry means the new
    process has never heard of the id.
    """
    db = str(tmp_path / "events.db")
    port = _free_port()

    app1 = with_persistence(_make_mcp(), backend="sqlite", url=db)
    async with _serve(app1, port) as url:
        session_id = await _initialize(url)

    app2 = with_persistence(_make_mcp(), backend="sqlite", url=db)
    async with _serve(app2, port) as url:
        assert await _reconnect_status(url, session_id) == 404


async def test_a_second_worker_adopts_the_session(tmp_path: Path) -> None:
    """No sticky routing needed: a peer that shares the store answers too.

    Both servers run at once on different ports, which is the load-balanced
    deployment rather than the restart.
    """
    db = str(tmp_path / "events.db")
    worker_a, worker_b = _free_port(), _free_port()

    app_a = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    app_b = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app_a, worker_a) as url_a, _serve(app_b, worker_b) as url_b:
        session_id = await _initialize(url_a)
        assert await _reconnect_status(url_b, session_id) == 200


async def test_unknown_session_id_still_404s(tmp_path: Path) -> None:
    """Adoption must not turn into "accept any id the client invents"."""
    db = str(tmp_path / "events.db")
    port = _free_port()

    app = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app, port) as url:
        await _initialize(url)
        assert await _reconnect_status(url, "0" * 32) == 404


async def test_terminated_session_is_not_adopted(tmp_path: Path) -> None:
    """A session ended by DELETE stays ended, including after a restart."""
    db = str(tmp_path / "events.db")
    port = _free_port()

    app1 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app1, port) as url:
        session_id = await _initialize(url)
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            deleted = await client.delete(url, headers={**_HEADERS, "Mcp-Session-Id": session_id})
        assert deleted.status_code in (200, 204, 405)

    if deleted.status_code == 405:  # server does not implement DELETE; nothing to assert
        pytest.skip("transport does not support session termination via DELETE")

    app2 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app2, port) as url:
        assert await _reconnect_status(url, session_id) == 404


async def test_registry_records_and_lists_sessions(tmp_path: Path) -> None:
    db = str(tmp_path / "events.db")
    port = _free_port()

    app = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app, port) as url:
        first = await _initialize(url)
        second = await _initialize(url)

    async with SQLiteEventStore.create(db) as store:
        registry = session_registry_for(store)
        await registry.initialize()
        listed = await registry.list_sessions()
        ids = {r.session_id for r in listed}
        assert {first, second} <= ids

        record = await registry.get(first)
        assert record is not None
        assert record.terminated is False
        assert record.owner is None  # unauthenticated server
        assert record.created_at <= record.last_seen_at

        await registry.terminate(first)
        after = await registry.get(first)
        assert after is not None and after.terminated is True
        assert first not in {r.session_id for r in await registry.list_sessions()}
        assert first in {r.session_id for r in await registry.list_sessions(include_terminated=True)}


async def test_registry_purges_by_age(tmp_path: Path) -> None:
    async with SQLiteEventStore.create(str(tmp_path / "events.db")) as store:
        registry = session_registry_for(store)
        await registry.initialize()
        await registry.register("old-session")
        # Nothing is old yet, so a large window deletes nothing.
        assert await registry.purge(older_than=3600) == 0
        # A window of zero makes everything already stale.
        assert await registry.purge(older_than=0) == 1
        assert await registry.get("old-session") is None


async def test_register_does_not_revive_a_terminated_session(tmp_path: Path) -> None:
    """Re-registering an id must not clear the terminated flag."""
    async with SQLiteEventStore.create(str(tmp_path / "events.db")) as store:
        registry = session_registry_for(store)
        await registry.initialize()
        await registry.register("s1")
        await registry.terminate("s1")
        await registry.register("s1")
        record = await registry.get("s1")
        assert record is not None and record.terminated is True


async def test_registry_is_tenant_scoped(tmp_path: Path) -> None:
    """One tenant must not see, or be able to adopt, another tenant's session."""
    db = str(tmp_path / "events.db")
    async with SQLiteEventStore.create(db, tenant_id="alpha") as alpha:
        alpha_registry = session_registry_for(alpha)
        await alpha_registry.initialize()
        await alpha_registry.register("shared-id", owner={"client_id": "a"})

    async with SQLiteEventStore.create(db, tenant_id="beta") as beta:
        beta_registry = session_registry_for(beta)
        await beta_registry.initialize()
        assert await beta_registry.get("shared-id") is None
        assert await beta_registry.list_sessions() == []


def test_owner_matching_rules() -> None:
    """Adoption must not become a way around the SDK's credential binding."""
    ctx = {"client_id": "c1", "issuer": "https://idp", "subject": "u1"}
    assert _owner_matches(ctx, dict(ctx))
    assert _owner_matches(None, None)
    # An unauthenticated request may not pick up an authenticated session.
    assert not _owner_matches(ctx, None)
    assert not _owner_matches(None, ctx)
    assert not _owner_matches(ctx, {**ctx, "subject": "u2"})
    assert not _owner_matches(ctx, {**ctx, "client_id": "c2"})
    assert not _owner_matches(ctx, {**ctx, "issuer": "https://evil"})


def test_session_record_as_dict_is_json_shaped() -> None:
    record = SessionRecord(session_id="s", created_at=1.0, last_seen_at=2.0)
    assert record.as_dict() == {
        "session_id": "s",
        "created_at": 1.0,
        "last_seen_at": 2.0,
        "terminated": False,
        "owner": None,
        "metadata": {},
        "handshake": None,
    }


async def test_a_new_session_is_recorded_before_the_client_learns_its_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The session was registered after the initialize response had gone out, so
    # a client that went straight to another worker could beat the write and get
    # a 404. A slow registry write makes that window wide enough to hit reliably.
    from mcp_persist.sessions import SQLiteSessionRegistry

    real_register = SQLiteSessionRegistry.register

    async def slow_register(self, session_id, **kwargs):  # type: ignore[no-untyped-def]
        await anyio.sleep(0.5)
        await real_register(self, session_id, **kwargs)

    monkeypatch.setattr(SQLiteSessionRegistry, "register", slow_register)

    db = str(tmp_path / "events.db")
    worker_a, worker_b = _free_port(), _free_port()
    app_a = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    app_b = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app_a, worker_a) as url_a, _serve(app_b, worker_b) as url_b:
        session_id = await _initialize(url_a)
        assert await _reconnect_status(url_b, session_id) == 200


async def _call(url: str, session_id: str, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """One JSON-RPC request on ``session_id``; returns the response message."""
    body: dict[str, Any] = {"jsonrpc": "2.0", "id": uuid.uuid4().hex, "method": method}
    if params is not None:
        body["params"] = params
    headers = {**_HEADERS, "Mcp-Session-Id": session_id, "Mcp-Protocol-Version": "2025-06-18"}
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        response = await client.post(url, json=body, headers=headers)
    assert response.status_code == 200, response.text
    if response.headers["content-type"].startswith("application/json"):
        return response.json()
    data = [line[len("data:") :].strip() for line in response.text.splitlines() if line.startswith("data:")]
    return json.loads(next(d for d in data if d))


async def test_an_adopted_session_serves_ordinary_requests_after_a_restart(tmp_path: Path) -> None:
    # The client finished `initialize` with the old process and will not send it
    # again, and a fresh connection that never saw it answered every method but
    # `ping` with -32602. The handshake is now recorded with the session and
    # restored by the process that adopts it.
    db = str(tmp_path / "events.db")
    port = _free_port()

    app1 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app1, port) as url:
        session_id = await _initialize(url)

    app2 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app2, port) as url:
        listed = await _call(url, session_id, "tools/list")
        assert [tool["name"] for tool in listed["result"]["tools"]] == ["echo"]
        await anyio.sleep(0.6)  # later requests too, not just a burst right after adoption
        called = await _call(url, session_id, "tools/call", {"name": "echo", "arguments": {"message": "hi"}})
        assert called["result"]["structuredContent"] == {"echo": "hi"}


async def test_an_open_adopted_stream_does_not_block_other_sessions(tmp_path: Path) -> None:
    # Adoption served the request while holding the manager's session creation
    # lock, and a standalone GET stream stays open as long as its client does.
    # After a restart, where every client reconnects at once, the first one to
    # resume held the lock for good: no other session could be adopted or created
    # on that worker.
    db = str(tmp_path / "events.db")
    port = _free_port()

    app1 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app1, port) as url:
        first = await _initialize(url)
        second = await _initialize(url)

    app2 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app2, port) as url, httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        headers = {**_HEADERS, "Mcp-Session-Id": first}
        async with client.stream("GET", url, headers=headers) as held_open:
            assert held_open.status_code == 200
            with anyio.fail_after(5):
                assert await _reconnect_status(url, second) == 200
                assert await _initialize(url)


async def test_the_handshake_is_recorded_with_the_session(tmp_path: Path) -> None:
    db = str(tmp_path / "events.db")
    port = _free_port()
    app = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app, port) as url:
        session_id = await _initialize(url)

    async with SQLiteEventStore.create(db) as store:
        registry = session_registry_for(store)
        await registry.initialize()
        record = await registry.get(session_id)
    assert record is not None
    assert record.handshake == _INIT_BODY["params"]


async def test_a_registry_without_the_handshake_keyword_still_works(tmp_path: Path) -> None:
    # SessionRegistry is public, and one written against 2.1 has no `handshake`
    # parameter. Sessions are still recorded and adopted; they just are not
    # restored as initialized.
    from mcp_persist.sessions import SQLiteSessionRegistry

    real_register = SQLiteSessionRegistry.register

    async def register_2_1(self, session_id, *, owner=None):  # type: ignore[no-untyped-def]
        await real_register(self, session_id, owner=owner)

    original = SQLiteSessionRegistry.register
    SQLiteSessionRegistry.register = register_2_1  # type: ignore[method-assign]
    try:
        db = str(tmp_path / "events.db")
        port = _free_port()
        app1 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
        async with _serve(app1, port) as url:
            session_id = await _initialize(url)
        app2 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
        async with _serve(app2, port) as url:
            assert (await _call(url, session_id, "ping"))["result"] == {}
    finally:
        SQLiteSessionRegistry.register = original  # type: ignore[method-assign]


async def test_a_registry_outage_after_lookup_does_not_fail_the_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Lookups and registration already tolerated a registry outage, but the
    # bookkeeping writes did not: a failing `touch` during adoption raised after
    # the transport was built and before it saw the request, so the client got a
    # 500 for a session this worker was ready to serve.
    from mcp_persist.sessions import SQLiteSessionRegistry

    db = str(tmp_path / "events.db")
    port = _free_port()
    app1 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app1, port) as url:
        session_id = await _initialize(url)

    async def registry_down(self, session_id):  # type: ignore[no-untyped-def]
        raise RuntimeError("registry unavailable")

    monkeypatch.setattr(SQLiteSessionRegistry, "touch", registry_down)
    monkeypatch.setattr(SQLiteSessionRegistry, "terminate", registry_down)

    app2 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app2, port) as url:
        assert (await _call(url, session_id, "ping"))["result"] == {}
        listed = await _call(url, session_id, "tools/list")
        assert [tool["name"] for tool in listed["result"]["tools"]] == ["echo"]


async def test_a_request_cancelled_after_creating_a_session_still_records_its_end() -> None:
    # `_reconcile` is where a new session's termination hook is installed, and it
    # ran only if the upstream handler returned. A request cancelled once the
    # session existed (a client that hung up, a timeout around the handler) left
    # it unhooked, so when the session later idled out the registry kept listing
    # it as live and any worker would adopt a session that had ended.
    import aiosqlite

    from mcp_persist.session_manager import ResumableSessionManager

    conn = await aiosqlite.connect(":memory:")
    store = SQLiteEventStore(conn, table_name="events", ttl=None)
    await store.initialize()
    registry = session_registry_for(store)
    await registry.initialize()
    manager = ResumableSessionManager(
        app=_make_mcp()._lowlevel_server, event_store=store, registry=registry, session_idle_timeout=0.3
    )
    body = json.dumps(_INIT_BODY).encode()
    headers = [(k.lower().encode(), v.encode()) for k, v in _HEADERS.items()]
    headers.append((b"content-length", str(len(body)).encode()))
    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "raw_path": b"/mcp",
        "query_string": b"",
        "headers": headers,
        "server": ("127.0.0.1", 80),
        "client": ("127.0.0.1", 1234),
        "scheme": "http",
        "http_version": "1.1",
    }
    delivered = False

    async def receive() -> dict[str, Any]:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": body, "more_body": False}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")

    cancel_scope = anyio.CancelScope()

    async def send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            # The session exists and has been recorded; now the request goes away.
            cancel_scope.cancel()
        await anyio.lowlevel.checkpoint()

    try:
        async with manager.run():
            with cancel_scope:
                await manager.handle_request(scope, receive, send)
            [recorded] = await registry.list_sessions(include_terminated=True)
            session_id = recorded.session_id
            # mcp 2.0 keeps the session and it idles out (0.3s) with nothing in
            # flight; from 2.2 the SDK discards it as soon as the request that
            # opened it is cancelled. Either way it has ended.
            with anyio.fail_after(5):
                while session_id in manager._server_instances:
                    await anyio.sleep(0.05)
            await anyio.sleep(0.1)
            record = await registry.get(session_id)
            assert record is not None and record.terminated
    finally:
        await conn.close()


def _user(client_id: str) -> Any:
    from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
    from mcp.server.auth.provider import AccessToken

    return AuthenticatedUser(AccessToken(token="t", client_id=client_id, scopes=[], subject=client_id))


async def _asgi_post(
    manager: Any, body: dict[str, Any], session_id: str | None, user: Any
) -> tuple[int, bytes, str | None]:
    """Drive one POST through ``manager`` without a server; returns (status, body, new session id)."""
    raw = json.dumps(body).encode()
    headers = [(k.lower().encode(), v.encode()) for k, v in _HEADERS.items()]
    headers.append((b"content-length", str(len(raw)).encode()))
    if session_id is not None:
        headers += [(b"mcp-session-id", session_id.encode()), (b"mcp-protocol-version", b"2025-06-18")]
    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "raw_path": b"/mcp",
        "query_string": b"",
        "headers": headers,
        "server": ("127.0.0.1", 80),
        "client": ("127.0.0.1", 1234),
        "scheme": "http",
        "http_version": "1.1",
    }
    if user is not None:
        scope["user"] = user
    sent = False
    status = 0
    chunks: list[bytes] = []
    new_id: str | None = None

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": raw, "more_body": False}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")

    async def send(message: dict[str, Any]) -> None:
        nonlocal status, new_id
        if message["type"] == "http.response.start":
            status = message["status"]
            for name, value in message["headers"]:
                if name.lower() == b"mcp-session-id":
                    new_id = value.decode()
        elif message["type"] == "http.response.body":
            chunks.append(message.get("body", b""))

    with anyio.fail_after(10):
        await manager.handle_request(scope, receive, send)
    return status, b"".join(chunks), new_id


@pytest.mark.parametrize("creator", ["alice", None])
async def test_a_refused_request_does_not_end_the_session_for_its_owner(creator: str | None) -> None:
    # A worker that does not hold a session, and declines to adopt it (wrong
    # credential), answers 404. That 404 said nothing about the session, but
    # `_reconcile` read "not held here" as "ended" and marked it terminated in
    # the shared registry, so anyone who knew a session id could end it for
    # everyone by sending one request with the wrong credential.
    import aiosqlite

    from mcp_persist.session_manager import ResumableSessionManager

    conn = await aiosqlite.connect(":memory:")
    store = SQLiteEventStore(conn, table_name="events", ttl=None)
    await store.initialize()
    registry = session_registry_for(store)
    await registry.initialize()

    def make() -> ResumableSessionManager:
        return ResumableSessionManager(app=_make_mcp()._lowlevel_server, event_store=store, registry=registry)

    owner = _user(creator) if creator else None
    intruder = _user("mallory")
    try:
        async with contextlib.AsyncExitStack() as stack:
            worker_a, worker_b, worker_c = make(), make(), make()
            for worker in (worker_a, worker_b, worker_c):
                await stack.enter_async_context(worker.run())
            status, _, session_id = await _asgi_post(worker_a, _INIT_BODY, None, owner)
            assert status == 200 and session_id

            refused, _, _ = await _asgi_post(
                worker_b, {"jsonrpc": "2.0", "id": 1, "method": "ping"}, session_id, intruder
            )
            assert refused == 404
            record = await registry.get(session_id)
            assert record is not None and not record.terminated

            # The rightful owner can still reach it, on a worker that never held it.
            status, body, _ = await _asgi_post(
                worker_c, {"jsonrpc": "2.0", "id": 2, "method": "ping"}, session_id, owner
            )
            assert status == 200, body
    finally:
        await conn.close()


async def test_a_refused_request_does_not_refresh_the_session() -> None:
    # On the worker that holds the session, the SDK refuses a request with the
    # wrong credential (404), but the bookkeeping afterwards still touched the
    # registry record, so anyone who knew a session id could keep it looking
    # active, and out of an age-based purge, indefinitely.
    import aiosqlite

    from mcp_persist.session_manager import ResumableSessionManager

    conn = await aiosqlite.connect(":memory:")
    store = SQLiteEventStore(conn, table_name="events", ttl=None)
    await store.initialize()
    registry = session_registry_for(store)
    await registry.initialize()
    try:
        manager = ResumableSessionManager(app=_make_mcp()._lowlevel_server, event_store=store, registry=registry)
        async with manager.run():
            status, _, session_id = await _asgi_post(manager, _INIT_BODY, None, _user("alice"))
            assert status == 200 and session_id
            before = await registry.get(session_id)
            assert before is not None

            await anyio.sleep(0.05)
            refused, _, _ = await _asgi_post(
                manager, {"jsonrpc": "2.0", "id": 1, "method": "ping"}, session_id, _user("mallory")
            )
            assert refused == 404
            after = await registry.get(session_id)
            assert after is not None and after.last_seen_at == before.last_seen_at

            # The owner's own requests still count as activity.
            await anyio.sleep(0.05)
            status, _, _ = await _asgi_post(
                manager, {"jsonrpc": "2.0", "id": 2, "method": "ping"}, session_id, _user("alice")
            )
            assert status == 200
            touched = await registry.get(session_id)
            assert touched is not None and touched.last_seen_at > before.last_seen_at
    finally:
        await conn.close()


async def _tool_call_event_ids(client: httpx.AsyncClient, url: str, session_id: str, message: str) -> list[str]:
    """Call ``echo`` with JSON-RPC id 7 on ``session_id``; return the SSE event ids it got."""
    body = {
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {"name": "echo", "arguments": {"message": message}},
    }
    headers = {**_HEADERS, "Mcp-Session-Id": session_id, "Mcp-Protocol-Version": "2025-06-18"}
    response = await client.post(url, json=body, headers=headers)
    assert response.status_code == 200, response.text
    assert message in response.text
    return [line[len("id:") :].strip() for line in response.text.splitlines() if line.startswith("id:")]


async def _resume(client: httpx.AsyncClient, url: str, session_id: str, last_event_id: str) -> str:
    """Everything a GET resuming ``session_id`` from ``last_event_id`` is sent within a second."""
    headers = {
        **_HEADERS,
        "Mcp-Session-Id": session_id,
        "Mcp-Protocol-Version": "2025-06-18",
        "Last-Event-ID": last_event_id,
    }
    received = ""
    with anyio.move_on_after(1.0):
        async with client.stream("GET", url, headers=headers) as response:
            assert response.status_code == 200
            async for chunk in response.aiter_text():
                received += chunk
    return received


@pytest.mark.parametrize("durable", [False, True], ids=["plain", "durable-after-restart"])
async def test_a_session_cannot_replay_another_sessions_events(tmp_path: Path, durable: bool) -> None:
    # The SDK names the streams it stores after JSON-RPC request ids, not
    # sessions, and every session shares the store. A request id another session
    # also used put both sessions' events in one stream, and resuming it replayed
    # them all: here the second session reads the first one's tool result by
    # resuming from an event id of its own.
    db = str(tmp_path / "events.db")
    port = _free_port()

    app1 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=durable)
    async with _serve(app1, port) as url:
        alice = await _initialize(url)
        mallory = await _initialize(url)
        if not durable:
            await _run_attack(url, alice, mallory)

    if durable:
        # After a restart both sessions are adopted, which builds their
        # transports outside the SDK; they must be isolated the same way.
        app2 = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
        async with _serve(app2, port) as url:
            await _run_attack(url, alice, mallory)


async def _run_attack(url: str, alice: str, mallory: str) -> None:
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        planted = await _tool_call_event_ids(client, url, mallory, "mallory-own")
        assert planted, "expected the tool call to be stored as an event"
        await _tool_call_event_ids(client, url, alice, "alice-secret")
        received = await _resume(client, url, mallory, planted[-1])
    assert "alice-secret" not in received


async def test_streams_are_stored_under_their_session(tmp_path: Path) -> None:
    db = str(tmp_path / "events.db")
    port = _free_port()
    app = with_persistence(_make_mcp(), backend="sqlite", url=db)
    async with _serve(app, port) as url:
        session_id = await _initialize(url)
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            await _tool_call_event_ids(client, url, session_id, "hello")

    async with SQLiteEventStore.create(db) as store:
        streams = [stream async for stream in store.list_streams()]
    assert streams and all(stream.startswith(f"{session_id}:") for stream in streams), streams
