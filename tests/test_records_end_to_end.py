"""Records work on both protocol eras, driven through a real server.

The claim 2.1 makes is that a record is written whatever protocol a client
negotiates, including the stateless 2026-07-28 transport where the event store
is never consulted. That claim is only worth anything if it is proven against
the real SDK routing rather than a stub, so these tests run the plugin's app
under uvicorn and speak both eras to it: the MCP client for the handshake era,
and a raw self-contained POST for the modern one.
"""

from __future__ import annotations

import contextlib
import json
import socket
from collections.abc import AsyncIterator
from typing import Any

import httpx2 as httpx
import pytest
import uvicorn
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.server.mcpserver import MCPServer
from starlette.applications import Starlette

from mcp_persist import PayloadPolicy, SQLiteEventStore, with_persistence

pytestmark = pytest.mark.anyio

MODERN = "2026-07-28"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _make_mcp() -> MCPServer:
    mcp = MCPServer(name="RecordTestServer")

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
    import anyio

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]

    async with anyio.create_task_group() as tg:
        tg.start_soon(server.serve)
        # Bounded: a lifespan that fails startup never sets `started`, and an
        # unbounded wait would hang the suite instead of reporting it.
        with anyio.fail_after(20):
            while not server.started:
                await anyio.sleep(0.02)
        try:
            yield f"http://127.0.0.1:{port}/mcp"
        finally:
            server.should_exit = True


async def _modern_post(url: str, method: str, params: dict[str, Any] | None = None) -> httpx.Response:
    """Send one self-contained 2026-07-28 request.

    The modern wire requires the protocol version, client info and client
    capabilities in the params ``_meta`` envelope, and an ``MCP-Method`` header
    matching the body. Building it by hand is the point: it proves the request
    really takes the era-routed path rather than the handshake one.
    """
    body: dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": {
            **(params or {}),
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": MODERN,
                "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
                "io.modelcontextprotocol/clientCapabilities": {},
            },
        },
    }
    headers = {
        "MCP-Protocol-Version": MODERN,
        "MCP-Method": method,
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }
    # SEP-2663 mirrors the request's name param into the Mcp-Name header, and
    # the server rejects a mismatch, so tools/call must carry it.
    name = (params or {}).get("name")
    if isinstance(name, str):
        headers["MCP-Name"] = name
    async with httpx.AsyncClient(follow_redirects=True) as client:
        return await client.post(url, json=body, headers=headers)


async def _call_shout(url: str, message: str = "hi") -> dict[str, str]:
    async with streamable_http_client(url) as (r, w):
        async with ClientSession(r, w) as session:
            await session.initialize()
            res = await session.call_tool("shout", {"message": message})
            return json.loads(res.content[0].text)  # type: ignore[attr-defined]


async def _streams(store: SQLiteEventStore) -> list[str]:
    return [s async for s in store.list_streams()]


async def _drained(app: Starlette) -> None:
    """Wait until the background writer has caught up.

    Records are written off the request path on purpose, so a read taken
    straight after a response can legitimately see nothing yet. Every assertion
    about stored records has to wait for the writer rather than assume it won.
    """
    import anyio

    flusher = app.state.record_flusher
    with anyio.fail_after(10):
        while flusher.pending > 0 or flusher.stats()["written"] == 0:
            await anyio.sleep(0.01)
    # `pending` counts the queue, so a batch can still be mid-write.
    await anyio.sleep(0.05)


async def _records(app: Starlette):
    await _drained(app)
    return await app.state.record_store.list_records()


async def _record_for(app: Starlette, method: str):
    """Return the single record for ``method``.

    Selecting by method rather than by position is deliberate: a modern
    ``tools/call`` resolves the tool through an internal listing that also runs
    through ``serve_one``, so more than one record exists and their timestamps
    can tie.
    """
    matching = [r for r in await _records(app) if r.method == method]
    assert len(matching) == 1, f"expected one {method} record, got {len(matching)}"
    return matching[0]


# ── The modern era: the event store is bypassed, records still land ───────────


async def test_modern_request_is_recorded_even_though_the_event_store_is_bypassed(tmp_path) -> None:
    mcp = _make_mcp()
    app = with_persistence(mcp, backend="sqlite", url=str(tmp_path / "e.db"), record=True)

    async with _serve(app) as url:
        response = await _modern_post(url, "tools/list")
        assert response.status_code == 200
        assert "shout" in response.text

        record = await _record_for(app, "tools/list")
        assert (record.protocol_version, record.outcome) == (MODERN, "ok")
        # The whole reason records exist: this era never reaches the event store.
        assert await _streams(app.state.event_store) == []


async def test_modern_tool_call_is_recorded_with_its_tool_name(tmp_path) -> None:
    mcp = _make_mcp()
    app = with_persistence(mcp, backend="sqlite", url=str(tmp_path / "e.db"), record=True)

    async with _serve(app) as url:
        response = await _modern_post(url, "tools/call", {"name": "shout", "arguments": {"message": "hi"}})
        assert response.status_code == 200

        record = await _record_for(app, "tools/call")
        assert record.tool_name == "shout"
        assert record.outcome == "ok"
        assert record.protocol_version == MODERN


async def test_modern_request_logs_the_bypass_once(tmp_path, caplog: pytest.LogCaptureFixture) -> None:
    mcp = _make_mcp()
    app = with_persistence(mcp, backend="sqlite", url=str(tmp_path / "e.db"), record=True)

    with caplog.at_level("WARNING", logger="mcp_persist.middleware"):
        async with _serve(app) as url:
            await _modern_post(url, "tools/list")
            await _modern_post(url, "tools/list")

    warnings = [r for r in caplog.records if "stateless single-exchange" in r.getMessage()]
    assert len(warnings) == 1


async def test_a_failing_modern_tool_call_records_a_tool_error(tmp_path) -> None:
    mcp = MCPServer(name="Boom")

    @mcp.tool()
    def explode() -> str:
        raise RuntimeError("kaboom")

    app = with_persistence(mcp, backend="sqlite", url=str(tmp_path / "e.db"), record=True)

    async with _serve(app) as url:
        await _modern_post(url, "tools/call", {"name": "explode", "arguments": {}})

        record = await _record_for(app, "tools/call")
        assert record.outcome in {"tool_error", "exception"}
        assert "kaboom" not in str(record.as_dict())


# ── The handshake era: events AND records ─────────────────────────────────────


async def test_handshake_era_records_alongside_the_events(tmp_path) -> None:
    mcp = _make_mcp()
    app = with_persistence(mcp, backend="sqlite", url=str(tmp_path / "e.db"), record=True)

    async with _serve(app) as url:
        assert await _call_shout(url) == {"shout": "HI"}

        records = await _records(app)
        methods = {r.method for r in records}
        assert "tools/call" in methods
        # Handshake era keeps doing what it always did.
        assert all(not r.protocol_version.startswith("2026") for r in records)
        assert await _streams(app.state.event_store) != []


async def test_one_store_spans_both_eras(tmp_path) -> None:
    """The LTS promise: a single store keeps recording across a migration."""
    mcp = _make_mcp()
    app = with_persistence(mcp, backend="sqlite", url=str(tmp_path / "e.db"), record=True)

    async with _serve(app) as url:
        await _call_shout(url)
        await _modern_post(url, "tools/list")

        eras = {r.protocol_version for r in await _records(app)}

    assert MODERN in eras
    assert len(eras) > 1


# ── Configuration ─────────────────────────────────────────────────────────────


async def test_recording_is_off_by_default(tmp_path) -> None:
    mcp = _make_mcp()
    app = with_persistence(mcp, backend="sqlite", url=str(tmp_path / "e.db"))

    async with _serve(app) as url:
        await _modern_post(url, "tools/list")
        assert app.state.record_store is None
        assert app.state.record_flusher is None


async def test_the_bypass_warning_still_fires_with_recording_off(tmp_path, caplog: pytest.LogCaptureFixture) -> None:
    """The silent bypass is worth a log line whether or not anything is recorded."""
    mcp = _make_mcp()
    app = with_persistence(mcp, backend="sqlite", url=str(tmp_path / "e.db"))

    with caplog.at_level("WARNING", logger="mcp_persist.middleware"):
        async with _serve(app) as url:
            await _modern_post(url, "tools/list")

    assert [r for r in caplog.records if "stateless single-exchange" in r.getMessage()]


async def test_warn_on_bypass_can_be_disabled(tmp_path, caplog: pytest.LogCaptureFixture) -> None:
    mcp = _make_mcp()
    app = with_persistence(mcp, backend="sqlite", url=str(tmp_path / "e.db"), warn_on_bypass=False)

    with caplog.at_level("WARNING", logger="mcp_persist.middleware"):
        async with _serve(app) as url:
            await _modern_post(url, "tools/list")

    assert [r for r in caplog.records if "stateless single-exchange" in r.getMessage()] == []


def test_payload_capture_needs_a_keyring(tmp_path) -> None:
    """Refused at construction, not at startup: a server that fails its lifespan
    is a far worse way to learn about a misconfiguration."""
    mcp = _make_mcp()

    with pytest.raises(ValueError, match="plaintext"):
        with_persistence(
            mcp,
            backend="sqlite",
            url=str(tmp_path / "e.db"),
            record=True,
            record_payload_policy=PayloadPolicy(tool_arguments={"shout": ["message"]}),
        )


async def test_payload_capture_records_only_allowed_arguments(tmp_path) -> None:
    mcp = _make_mcp()
    app = with_persistence(
        mcp,
        backend="sqlite",
        url=str(tmp_path / "e.db"),
        record=True,
        record_payload_policy=PayloadPolicy(tool_arguments={"shout": ["message"]}),
        record_allow_plaintext=True,
    )

    async with _serve(app) as url:
        await _modern_post(url, "tools/call", {"name": "shout", "arguments": {"message": "hi", "secret": "x"}})

        record = await _record_for(app, "tools/call")
        assert record.payload == {"message": "hi"}
        assert "secret" not in str(record.payload)


def test_record_options_require_record_enabled() -> None:
    mcp = _make_mcp()

    with pytest.raises(ValueError, match="require record=True"):
        with_persistence(mcp, backend="sqlite", url=":memory:", record_ttl=60)


def test_a_supplied_store_requires_an_explicit_record_store() -> None:
    """A record store is never inferred from another store's internals."""
    mcp = _make_mcp()
    conn = object()

    with pytest.raises(ValueError, match="record_store="):
        with_persistence(mcp, store=SQLiteEventStore(conn, table_name="e"), record=True)  # type: ignore[arg-type]


async def test_records_survive_the_lifespan_shutdown(tmp_path) -> None:
    """Whatever is queued at shutdown is written, not lost."""
    mcp = _make_mcp()
    db = str(tmp_path / "e.db")
    app = with_persistence(mcp, backend="sqlite", url=db, record=True)

    async with _serve(app) as url:
        for _ in range(5):
            await _modern_post(url, "tools/list")
        flusher = app.state.record_flusher

    # The lifespan has exited, taking the store's connection with it, so the
    # claim is checked on the flusher's own counters: everything queued was
    # written on the way out rather than dropped.
    assert flusher.dropped == 0
    assert flusher.failed == 0
    assert flusher.pending == 0
    assert flusher.written >= 5


def test_record_ttl_is_rejected_on_sql_backends() -> None:
    """A retention setting that looks configured and does nothing is worse than none."""
    mcp = _make_mcp()

    for backend, url in (("sqlite", ":memory:"), ("postgres", "postgresql://localhost/x")):
        with pytest.raises(ValueError, match="record_ttl applies to the redis backend only"):
            with_persistence(mcp, backend=backend, url=url, record=True, record_ttl=60)


def test_record_ttl_is_accepted_on_redis() -> None:
    mcp = _make_mcp()

    with_persistence(mcp, backend="redis", url="redis://localhost:6379", record=True, record_ttl=60)


async def test_record_metrics_receive_writes(tmp_path) -> None:
    """The advertised metrics integration must be reachable from with_persistence."""

    class Collector:
        def __init__(self) -> None:
            self.written = 0
            self.dropped = 0

        def on_record_write(self, count: int) -> None:
            self.written += count

        def on_record_drop(self, count: int) -> None:
            self.dropped += count

    metrics = Collector()
    mcp = _make_mcp()
    app = with_persistence(
        mcp,
        backend="sqlite",
        url=str(tmp_path / "e.db"),
        record=True,
        record_metrics=metrics,
    )

    async with _serve(app) as url:
        await _modern_post(url, "tools/list")
        await _drained(app)

    assert metrics.written >= 1
    assert metrics.dropped == 0


async def test_restarting_the_app_does_not_accumulate_middleware(tmp_path) -> None:
    """A second lifespan must not leave the first run's recorder in the chain.

    A stale recorder points at a closed writer whose queue nothing drains, so
    every record it accepts is lost silently. Restart is only possible with a
    caller-owned store (a `create()` context manager cannot be re-entered), so
    that is the path exercised here.
    """
    from mcp_persist.records import record_store_for

    mcp = _make_mcp()
    # Not zero: MCPServer installs middleware of its own at construction.
    baseline = len(mcp.middleware)

    async with SQLiteEventStore.create(str(tmp_path / "e.db"), ttl=None) as store:
        records = record_store_for(store)
        app = with_persistence(mcp, store=store, record=True, record_store=records)

        async with _serve(app) as url:
            await _modern_post(url, "tools/list")
            await _drained(app)
        after_first = len(mcp.middleware)

        async with _serve(app) as url:
            await _modern_post(url, "tools/list")
            await _drained(app)
        after_second = len(mcp.middleware)

    assert after_first == baseline
    assert after_second == baseline


async def test_a_restarted_app_keeps_recording(tmp_path) -> None:
    """The second run must write through a live writer, not a stale one."""
    from mcp_persist.records import record_store_for

    mcp = _make_mcp()
    async with SQLiteEventStore.create(str(tmp_path / "e.db"), ttl=None) as store:
        records = record_store_for(store)
        app = with_persistence(mcp, store=store, record=True, record_store=records)

        async with _serve(app) as url:
            await _modern_post(url, "tools/list")
            await _drained(app)
        first_count = await records.count()

        async with _serve(app) as url:
            await _modern_post(url, "tools/list")
            await _drained(app)
        second_count = await records.count()

    assert second_count > first_count


def test_record_ttl_is_rejected_for_an_env_resolved_backend(monkeypatch) -> None:
    """An unknown backend is not assumed to be redis."""
    monkeypatch.setenv("MCP_PERSIST_BACKEND", "sqlite")
    monkeypatch.setenv("MCP_PERSIST_URL", ":memory:")
    mcp = _make_mcp()

    with pytest.raises(ValueError, match="record_ttl"):
        with_persistence(mcp, record=True, record_ttl=60)


def test_record_ttl_is_rejected_with_a_caller_supplied_record_store() -> None:
    """The caller already configured that store's retention."""
    from mcp_persist.records import SQLiteRecordStore

    mcp = _make_mcp()
    fake = SQLiteRecordStore.__new__(SQLiteRecordStore)

    with pytest.raises(ValueError, match="record_ttl"):
        with_persistence(mcp, backend="redis", url="redis://x", record=True, record_store=fake, record_ttl=60)
