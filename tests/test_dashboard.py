"""Tests for the local dashboard (``mcp-persist dashboard``).

Run under uvicorn against a real SQLite store, because what matters is that the
routes answer over HTTP with the shapes the inline page reads, and that the page
itself is servable and self-contained.
"""

from __future__ import annotations

import contextlib
import re
import socket
from collections.abc import AsyncIterator
from pathlib import Path

import anyio
import httpx2 as httpx
import pytest
import uvicorn
from mcp_types import JSONRPCNotification, JSONRPCRequest, JSONRPCResponse
from starlette.applications import Starlette

from mcp_persist import SQLiteEventStore, session_registry_for
from mcp_persist._admin import StoreConfig, _is_loopback
from mcp_persist.dashboard import _describe_payload, create_dashboard

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def _seed(db: str) -> None:
    async with SQLiteEventStore.create(db, ttl=3600) as store:
        registry = session_registry_for(store)
        await registry.initialize()
        await registry.register("live-session")
        await registry.register("dead-session")
        await registry.terminate("dead-session")
        await store.store_event(
            "stream-a",
            JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/call", params={"name": "search"}),
        )
        await store.store_event("stream-a", JSONRPCResponse(jsonrpc="2.0", id="1", result={"ok": True}))
        await store.store_event(
            "stream-b",
            JSONRPCNotification(jsonrpc="2.0", method="notifications/progress", params={"p": 1}),
        )


@contextlib.asynccontextmanager
async def _serve(app: Starlette) -> AsyncIterator[str]:
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    async with anyio.create_task_group() as tg:
        tg.start_soon(server.serve)
        while not server.started:
            await anyio.sleep(0.02)
        try:
            yield f"http://127.0.0.1:{port}"
        finally:
            server.should_exit = True


@contextlib.asynccontextmanager
async def _dashboard(tmp_path: Path, *, redact: bool = False) -> AsyncIterator[str]:
    db = str(tmp_path / "events.db")
    await _seed(db)
    cfg = StoreConfig(backend="sqlite", url=db)
    async with _serve(create_dashboard(cfg, redact_payloads=redact)) as base:
        yield base


async def test_page_is_served_and_self_contained(tmp_path: Path) -> None:
    """No CDN, no external font, no remote script: it must work offline."""
    async with _dashboard(tmp_path) as base, httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(base + "/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        body = response.text
        assert "mcp-persist" in body
        # Any absolute URL in src/href would be an external asset.
        assert not re.search(r'(?:src|href)\s*=\s*["\']https?://', body)


async def test_overview_reports_the_store(tmp_path: Path) -> None:
    async with _dashboard(tmp_path) as base, httpx.AsyncClient(timeout=15.0) as client:
        data = (await client.get(base + "/api/overview")).json()

    assert data["backend"] == "sqlite"
    assert data["total_events"] == 3
    assert data["total_streams"] == 2
    assert data["health"]["healthy"] is True
    assert {s["stream_id"] for s in data["streams"]} == {"stream-a", "stream-b"}
    assert next(s for s in data["streams"] if s["stream_id"] == "stream-a")["events"] == 2


async def test_events_are_newest_first_and_labelled(tmp_path: Path) -> None:
    async with _dashboard(tmp_path) as base, httpx.AsyncClient(timeout=15.0) as client:
        data = (await client.get(base + "/api/streams/stream-a/events")).json()

    labels = [e["payload"]["label"] for e in data["events"]]
    assert labels == ["result", "tools/call"]  # newest first
    assert data["events"][1]["payload"]["kind"] == "request"
    assert "tools/call" in data["events"][1]["payload"]["json"]


async def test_unknown_stream_is_empty_not_an_error(tmp_path: Path) -> None:
    async with _dashboard(tmp_path) as base, httpx.AsyncClient(timeout=15.0) as client:
        response = await client.get(base + "/api/streams/nope/events")
    assert response.status_code == 200
    assert response.json()["events"] == []


async def test_sessions_include_terminated(tmp_path: Path) -> None:
    async with _dashboard(tmp_path) as base, httpx.AsyncClient(timeout=15.0) as client:
        data = (await client.get(base + "/api/sessions")).json()

    assert data["available"] is True
    states = {s["session_id"]: s["terminated"] for s in data["sessions"]}
    assert states == {"live-session": False, "dead-session": True}


async def test_redaction_hides_payloads_but_keeps_the_shape(tmp_path: Path) -> None:
    """The flag must remove message bodies, not just hide them in the page."""
    async with _dashboard(tmp_path, redact=True) as base, httpx.AsyncClient(timeout=15.0) as client:
        events = (await client.get(base + "/api/streams/stream-a/events")).json()["events"]
        overview = (await client.get(base + "/api/overview")).json()

    assert overview["redact_payloads"] is True
    assert overview["total_events"] == 3  # counts still work
    assert len(events) == 2
    for event in events:
        assert event["payload"]["json"] is None
        assert event["payload"]["kind"] == "redacted"
        assert event["event_id"] is not None


# ── Unit-level: payload labelling and the bind guard ──────────────────────────


def test_describe_payload_labels_each_message_kind() -> None:
    assert _describe_payload(None)["kind"] == "priming"
    assert _describe_payload({"method": "tools/list", "id": 1})["kind"] == "request"
    assert _describe_payload({"method": "notifications/progress"})["kind"] == "notification"
    assert _describe_payload({"result": {}})["kind"] == "result"

    error = _describe_payload({"error": {"code": -32601, "message": "nope"}})
    assert error["kind"] == "error"
    assert "-32601" in error["label"]


def test_describe_payload_truncates_a_huge_message() -> None:
    described = _describe_payload({"result": {"blob": "x" * 20000}})
    assert described["truncated"] is True
    assert "truncated" in described["json"]
    assert len(described["json"]) < 20000


def test_loopback_detection_governs_the_bind_guard() -> None:
    """A non-loopback bind exposes payloads, so anything unclear must be refused."""
    assert _is_loopback("127.0.0.1")
    assert _is_loopback("localhost")
    assert _is_loopback("::1")
    assert _is_loopback("127.5.5.5")
    assert not _is_loopback("0.0.0.0")
    assert not _is_loopback("192.168.1.10")
    assert not _is_loopback("example.com")  # unclassifiable resolves to "exposed"


def test_dashboard_refuses_a_public_bind(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from mcp_persist._admin import main

    monkeypatch.setenv("MCP_PERSIST_BACKEND", "sqlite")
    monkeypatch.setenv("MCP_PERSIST_URL", str(tmp_path / "e.db"))
    monkeypatch.setattr("sys.argv", ["mcp-persist", "dashboard", "--host", "0.0.0.0"])
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 2


async def test_requests_naming_a_foreign_host_are_refused(tmp_path: Path) -> None:
    # Bound to loopback, but a page on attacker.example that re-points its own
    # name at 127.0.0.1 (DNS rebinding) reaches the API as same-origin; the
    # Host header is what still names the attacker's domain.
    async with _dashboard(tmp_path) as base, httpx.AsyncClient(timeout=15.0) as client:
        port = base.rsplit(":", 1)[1]
        refused = await client.get(base + "/api/streams/stream-a/events", headers={"host": "attacker.example"})
        assert refused.status_code == 400
        for host in (f"localhost:{port}", f"127.0.0.1:{port}", f"[::1]:{port}"):
            response = await client.get(base + "/api/overview", headers={"host": host})
            assert response.status_code == 200, host


async def test_allowed_hosts_extend_the_host_check(tmp_path: Path) -> None:
    db = str(tmp_path / "events.db")
    await _seed(db)
    app = create_dashboard(StoreConfig(backend="sqlite", url=db), allowed_hosts=["dash.internal"])
    async with _serve(app) as base, httpx.AsyncClient(timeout=15.0) as client:
        assert (await client.get(base + "/api/overview", headers={"host": "dash.internal:80"})).status_code == 200
        assert (await client.get(base + "/api/overview", headers={"host": "localhost"})).status_code == 400


async def test_overview_never_shows_the_dsn_password(monkeypatch: pytest.MonkeyPatch) -> None:
    import mcp_persist._admin as admin
    from mcp_persist.dashboard import _snapshot

    async def fake_stats(*_args: object, **_kwargs: object) -> admin.StatsReport:
        return admin.StatsReport("postgres", [], 0, 0, None, 0.0)

    monkeypatch.setattr(admin, "gather_stats", fake_stats)
    cfg = StoreConfig(backend="postgres", url="postgresql://app:s3cret@db.internal/mcp")
    snapshot = await _snapshot(cfg, object())
    assert snapshot["url"] == "postgresql://app:***@db.internal/mcp"


async def test_events_truncated_only_when_more_than_the_limit(tmp_path: Path) -> None:
    async with _dashboard(tmp_path) as base, httpx.AsyncClient(timeout=15.0) as client:
        exact = (await client.get(base + "/api/streams/stream-a/events?limit=2")).json()
        clipped = (await client.get(base + "/api/streams/stream-a/events?limit=1")).json()
    assert exact["truncated"] is False and len(exact["events"]) == 2
    assert clipped["truncated"] is True
    assert [e["payload"]["label"] for e in clipped["events"]] == ["result"]  # the newest one
