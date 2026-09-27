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
import socket
from collections.abc import AsyncIterator
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
    }


async def test_a_new_session_is_recorded_before_the_client_learns_its_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The session was registered after the initialize response had gone out, so
    # a client that went straight to another worker could beat the write and get
    # a 404. A slow registry write makes that window wide enough to hit reliably.
    from mcp_persist.sessions import SQLiteSessionRegistry

    real_register = SQLiteSessionRegistry.register

    async def slow_register(self, session_id, *, owner=None):  # type: ignore[no-untyped-def]
        await anyio.sleep(0.5)
        await real_register(self, session_id, owner=owner)

    monkeypatch.setattr(SQLiteSessionRegistry, "register", slow_register)

    db = str(tmp_path / "events.db")
    worker_a, worker_b = _free_port(), _free_port()
    app_a = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    app_b = with_persistence(_make_mcp(), backend="sqlite", url=db, durable_sessions=True)
    async with _serve(app_a, worker_a) as url_a, _serve(app_b, worker_b) as url_b:
        session_id = await _initialize(url_a)
        assert await _reconnect_status(url_b, session_id) == 200
