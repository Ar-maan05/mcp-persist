"""Tests for the FastMCP plugin (mcp_persist.fastmcp.with_persistence).

The plugin is exercised end-to-end: the returned Starlette app is run under an
in-process uvicorn server on an ephemeral port and driven with the real MCP
streamable-http client, so the full path — lifespan, store, session manager,
SSE — is covered, not just construction.
"""

from __future__ import annotations

import contextlib
import json
import socket
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import uvicorn
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.server.fastmcp import FastMCP
from mcp.types import JSONRPCRequest
from starlette.applications import Starlette

from mcp_persist import SQLiteEventStore, with_persistence

_SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="probe", method="tools/list")


def _make_mcp() -> FastMCP:
    mcp = FastMCP(name="PluginTestServer")

    @mcp.tool()
    def shout(message: str) -> dict[str, str]:
        return {"shout": message.upper()}

    return mcp


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextlib.asynccontextmanager
async def _serve(app: Starlette) -> AsyncIterator[str]:
    """Run ``app`` under uvicorn in-process; yield the MCP endpoint URL."""
    import anyio

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    # Don't let uvicorn install process-wide signal handlers under pytest.
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]

    async with anyio.create_task_group() as tg:
        tg.start_soon(server.serve)
        while not server.started:  # wait for startup (lifespan complete) before connecting
            await anyio.sleep(0.02)
        try:
            yield f"http://127.0.0.1:{port}/mcp"
        finally:
            server.should_exit = True


async def _call_shout(url: str, message: str = "hi") -> dict[str, str]:
    """Run a full MCP session against ``url`` and return the shout tool result."""
    async with streamable_http_client(url) as (r, w, _):
        async with ClientSession(r, w) as session:
            await session.initialize()
            res = await session.call_tool("shout", {"message": message})
            return json.loads(res.content[0].text)  # type: ignore[attr-defined]


# ── End-to-end: each store-resolution pattern builds a working server ──────────


@pytest.mark.anyio
async def test_backend_kwargs_builds_working_app() -> None:
    """Pattern A: backend=/url= builds, runs and tears down a working app."""
    app = with_persistence(_make_mcp(), backend="sqlite", url=":memory:", ttl=3600)
    async with _serve(app) as url:
        result = await _call_shout(url, "hello")
    assert result == {"shout": "HELLO"}


@pytest.mark.anyio
async def test_env_selects_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pattern C: with neither store nor backend, MCP_PERSIST_* drives the store."""
    monkeypatch.setenv("MCP_PERSIST_BACKEND", "sqlite")
    monkeypatch.setenv("MCP_PERSIST_URL", ":memory:")
    monkeypatch.setenv("MCP_PERSIST_TTL", "3600")
    app = with_persistence(_make_mcp())
    async with _serve(app) as url:
        result = await _call_shout(url, "env")
    assert result == {"shout": "ENV"}


@pytest.mark.anyio
async def test_prebuilt_store_persists_and_is_not_closed(tmp_path: Path) -> None:
    """Pattern B: a passed-in store captures events, replays them, and is NOT closed.

    "Caller owns lifecycle" means the plugin must not tear down a store it didn't
    build — the case that matters for users sharing one store across several
    mounts/servers. We verify that directly by spying on ``aclose()`` and
    requiring zero calls across the app's full startup→shutdown (``ping()`` alone
    can't catch this: ``SQLiteEventStore.aclose()`` doesn't close the underlying
    connection, so an erroneous close would still ping fine). The store must also
    remain usable afterwards.
    """
    db = str(tmp_path / "events.db")
    async with SQLiteEventStore.create(db, ttl=3600) as store:
        aclose_calls = 0
        original_aclose = store.aclose

        async def counting_aclose() -> None:
            nonlocal aclose_calls
            aclose_calls += 1
            await original_aclose()

        store.aclose = counting_aclose  # type: ignore[method-assign]

        app = with_persistence(_make_mcp(), store=store)
        async with _serve(app) as url:
            assert await _call_shout(url, "persist") == {"shout": "PERSIST"}

        # App has fully started and shut down by here; the plugin must not have
        # closed our store at any point.
        assert aclose_calls == 0, "with_persistence closed a caller-owned store"

        # Still usable after shutdown: ping, plus a fresh write that succeeds.
        assert await store.ping() is True
        new_id = await store.store_event("post-shutdown", _SAMPLE_MSG)
        assert new_id.isdigit()

        # Events from the session also landed in our store via the plugin, and
        # replay correctly through the store's own API (resumability really wired).
        streams = [s async for s in store.list_streams()]
        assert streams, "expected the session manager to persist events into the passed-in store"
        assert await _replays(store, streams)

    # On exiting the create() block, aclose() runs exactly once — ours (the
    # owner's), never the plugin's.
    assert aclose_calls == 1


async def _replays(store: SQLiteEventStore, streams: list[str]) -> bool:
    """True if some persisted stream has an event whose successors replay back."""
    from mcp.server.streamable_http import EventMessage

    for stream_id in streams:
        ids = [eid async for eid, _msg in store._iter_stream_events(stream_id)]
        if len(ids) < 2:
            continue
        anchor = ids[0]
        replayed: list[str] = []

        async def cb(event: EventMessage, _replayed: list[str] = replayed) -> None:
            _replayed.append(event.event_id)

        returned_stream = await store.replay_events_after(anchor, cb)
        assert returned_stream == stream_id
        # Every replayed id is a real (non-priming) event strictly after the anchor.
        assert replayed
        assert all(int(eid) > int(anchor) for eid in replayed)
        return True
    return False


# ── Configuration validation (pure, no server) ────────────────────────────────


def test_store_and_backend_are_mutually_exclusive() -> None:
    store = object()  # not actually used; resolution fails before touching it
    with pytest.raises(ValueError, match="either store= or backend="):
        with_persistence(_make_mcp(), store=store, backend="sqlite", url=":memory:")  # type: ignore[arg-type]


def test_backend_requires_url() -> None:
    with pytest.raises(ValueError, match="backend= requires url="):
        with_persistence(_make_mcp(), backend="sqlite")


def test_unknown_backend_rejected() -> None:
    with pytest.raises(ValueError, match="backend must be one of"):
        with_persistence(_make_mcp(), backend="mongo", url="x")


def test_inapplicable_option_rejected() -> None:
    # key_prefix is redis-only; passing it to sqlite is a programming error.
    with pytest.raises(ValueError, match="key_prefix not supported by backend 'sqlite'"):
        with_persistence(_make_mcp(), backend="sqlite", url=":memory:", key_prefix="p")
    # table_name is not a redis option.
    with pytest.raises(ValueError, match="table_name not supported by backend 'redis'"):
        with_persistence(_make_mcp(), backend="redis", url="redis://x", table_name="t")


def test_config_kwargs_without_backend_rejected() -> None:
    # Env path (no store, no backend) can't honor inline config kwargs.
    with pytest.raises(ValueError, match="require backend="):
        with_persistence(_make_mcp(), ttl=3600)
