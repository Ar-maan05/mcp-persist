"""
examples/resume_demo.py
=======================
A self-contained, recordable terminal demo of mcp-persist resumability.

It runs a *real* MCP server (FastMCP + SQLiteEventStore, the same wiring as
``sqlite_server.py``) in a background thread, then drives a *real* HTTP client
against it that:

  1. starts a streaming tool call (the server reports progress step by step),
  2. yanks the connection mid-stream — simulating a client/network crash,
  3. waits offline while the server keeps working and persists every event,
  4. reconnects with ``Last-Event-ID`` and watches the server replay exactly
     the events that were missed, finishing with the tool's result.

Nothing is mocked: the events round-trip through SQLite (``resume_demo.db``),
and the client speaks the Streamable HTTP wire protocol with httpx, parsing the
SSE stream with mcp-persist's own ``SSEParser``.

Run it (one command, made to be screen-recorded):

    python examples/resume_demo.py

Only the [sqlite] extra is needed (uvicorn + httpx ship with mcp):

    pip install "mcp-persist[sqlite]"
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sqlite3
import threading
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import aiosqlite
import httpx
import uvicorn
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import LATEST_PROTOCOL_VERSION
from starlette.applications import Starlette
from starlette.routing import Mount

from mcp_persist import SQLiteEventStore
from mcp_persist._sse_parser import SSEParser

# --------------------------------------------------------------------------- #
# Demo configuration
# --------------------------------------------------------------------------- #

HOST = "127.0.0.1"
PORT = 8765
URL = f"http://{HOST}:{PORT}/mcp"
DB_PATH = Path("resume_demo.db")

STEPS = 8  # how many progress events the tool emits
STEP_DELAY = 0.4  # seconds between steps
DROP_AFTER = 3  # crash the client after receiving this many events

# --------------------------------------------------------------------------- #
# Pretty terminal output
# --------------------------------------------------------------------------- #

_COLOR = os.environ.get("NO_COLOR") is None


def _c(code: str, text: str) -> str:
    return text if not _COLOR else f"\033[{code}m{text}\033[0m"


def dim(t: str) -> str:
    return _c("2", t)


def bold(t: str) -> str:
    return _c("1", t)


def green(t: str) -> str:
    return _c("32", t)


def yellow(t: str) -> str:
    return _c("33", t)


def red(t: str) -> str:
    return _c("31", t)


def cyan(t: str) -> str:
    return _c("36", t)


def banner(text: str) -> None:
    print()
    print(bold(cyan(f"━━━ {text} ")) + cyan("━" * max(0, 60 - len(text))))


# --------------------------------------------------------------------------- #
# The server: a real MCP server backed by SQLiteEventStore
# --------------------------------------------------------------------------- #

mcp = FastMCP(name="ResumeDemo")


@mcp.tool()
async def slow_count(n: int, ctx: Context) -> dict[str, Any]:
    """Count to ``n``, reporting progress each step.

    Each ``report_progress`` becomes one SSE event that mcp-persist stores, so
    the stream has several replayable checkpoints before the final result.
    """
    for i in range(1, n + 1):
        await ctx.report_progress(i, n, f"step {i}/{n}")
        await asyncio.sleep(STEP_DELAY)
    return {"counted_to": n, "status": "complete"}


@contextlib.asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[None]:
    conn = await aiosqlite.connect(DB_PATH)
    try:
        store = SQLiteEventStore(conn, ttl=3600)
        await store.initialize()
        manager = StreamableHTTPSessionManager(
            app=mcp._mcp_server,
            event_store=store,
            session_idle_timeout=300,
        )
        app.state.session_manager = manager
        async with manager.run():
            yield
    finally:
        await conn.close()


async def handle_mcp(scope: Any, receive: Any, send: Any) -> None:
    await scope["app"].state.session_manager.handle_request(scope, receive, send)


app = Starlette(lifespan=lifespan, routes=[Mount("/mcp", app=handle_mcp)])


def start_server() -> uvicorn.Server:
    """Launch uvicorn in a background thread and wait until it is serving."""
    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(0.05)
    return server


# --------------------------------------------------------------------------- #
# The client: a real Streamable HTTP / SSE client
# --------------------------------------------------------------------------- #

JSON_SSE = "application/json, text/event-stream"


def _short(payload: dict[str, Any]) -> str:
    """One-line human summary of a JSON-RPC message for the demo log."""
    if payload.get("method") == "notifications/progress":
        p = payload["params"]
        return f"progress  {p['message']:<10}  ({int(p['progress'])}/{int(p['total'])})"
    if "result" in payload:
        return "RESULT    " + json.dumps(payload["result"].get("structuredContent", payload["result"]))
    return json.dumps(payload)


async def initialize(client: httpx.AsyncClient) -> tuple[str, str]:
    """Run the MCP handshake. Returns (session_id, negotiated_protocol_version)."""
    req = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": LATEST_PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "resume-demo", "version": "0"},
        },
    }
    headers = {"Content-Type": "application/json", "Accept": JSON_SSE}
    async with client.stream("POST", URL, headers=headers, json=req) as resp:
        resp.raise_for_status()
        session_id = resp.headers["mcp-session-id"]
        parser = SSEParser()
        result = None
        async for chunk in resp.aiter_text():
            for frame in parser.feed(chunk):
                if frame.data == "":
                    continue  # priming event (resumability checkpoint, no payload)
                result = json.loads(frame.data)
                break
            if result is not None:
                break
    assert result is not None
    version = result["result"]["protocolVersion"]

    # Complete the handshake.
    await client.post(
        URL,
        headers={**headers, "mcp-session-id": session_id, "MCP-Protocol-Version": version},
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
    )
    print(green("✓") + f" session initialized  {dim('id=' + session_id[:12] + '…  proto=' + version)}")
    return session_id, version


async def fire_tool(client: httpx.AsyncClient, auth: dict[str, str]) -> dict[str, Any] | None:
    """POST the streaming tool call and return its final result.

    Progress notifications travel on the standalone GET stream (that is the
    server→client channel resumability is built for); this POST stream carries
    only the tool's final result.
    """
    headers = {"Content-Type": "application/json", "Accept": JSON_SSE, **auth}
    call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {"name": "slow_count", "arguments": {"n": STEPS}, "_meta": {"progressToken": "demo"}},
    }
    async with client.stream("POST", URL, headers=headers, json=call) as resp:
        resp.raise_for_status()
        parser = SSEParser()
        async for chunk in resp.aiter_text():
            for frame in parser.feed(chunk):
                if frame.data == "":
                    continue
                msg = json.loads(frame.data)
                if "result" in msg and msg.get("id") == 2:
                    return msg["result"]
    return None


async def read_get(
    client: httpx.AsyncClient,
    auth: dict[str, str],
    *,
    label: str,
    paint: Any,
    seen: list[int],
    last_event_id: str | None = None,
    stop_after: int | None = None,
    stop_value: int | None = None,
    on_attached: Any = None,
) -> str | None:
    """Open the standalone GET stream and print progress events as they arrive.

    Returns the last event id seen. Stops after ``stop_after`` events (the
    simulated crash) or once a progress value reaches ``stop_value``. If
    ``on_attached`` is given it is launched once the stream is open — used to
    kick off the tool only after the GET channel is registered server-side.
    """
    headers = {"Accept": "text/event-stream", **auth}
    if last_event_id is not None:
        headers["Last-Event-ID"] = str(last_event_id)
    last_id = last_event_id
    got = 0
    async with client.stream("GET", URL, headers=headers) as resp:
        resp.raise_for_status()
        if on_attached is not None:
            asyncio.create_task(on_attached())  # noqa: RUF006 — fire-and-forget, short-lived
        parser = SSEParser()
        async for chunk in resp.aiter_text():
            for frame in parser.feed(chunk):
                if frame.data == "":
                    continue  # priming event
                msg = json.loads(frame.data)
                last_id = frame.original_id
                if msg.get("method") != "notifications/progress":
                    continue
                value = int(msg["params"]["progress"])
                seen.append(value)
                got += 1
                print(f"  {paint(label)} {dim('id=' + str(last_id)):<14} {_short(msg)}")
                if stop_after is not None and got >= stop_after:
                    return last_id
                if stop_value is not None and value >= stop_value:
                    return last_id
    return last_id


def _result_text(result: dict[str, Any] | None) -> Any:
    if result and "structuredContent" in result:
        return result["structuredContent"]
    if result and result.get("content"):
        return result["content"][0].get("text")
    return result


async def run_client() -> None:
    seen: list[int] = []  # steps received, across both connections
    holder: dict[str, Any] = {}

    # follow_redirects: the server mounts at /mcp and 307-redirects to /mcp/.
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        banner("handshake")
        session_id, version = await initialize(client)
        auth = {"mcp-session-id": session_id, "MCP-Protocol-Version": version}

        banner("streaming a long tool call")
        print(dim("open the server→client event stream, then call slow_count" + f"(n={STEPS})"))
        print(dim(f"we drop the connection after {DROP_AFTER} events\n"))

        async def on_attached() -> None:
            await asyncio.sleep(0.3)  # let the GET stream register before progress starts
            holder["task"] = asyncio.create_task(fire_tool(client, auth))

        last_id = await read_get(
            client, auth, label="recv", paint=green, seen=seen, stop_after=DROP_AFTER, on_attached=on_attached
        )
        print()
        print(red(bold("  ✗ CONNECTION DROPPED")) + dim(f"  (client crashed after id={last_id})"))

        banner("offline — server keeps working")
        print(dim("the client is gone, but the tool runs on and every event is persisted to SQLite…"))
        await asyncio.sleep(STEPS * STEP_DELAY + 0.6)
        result = await holder["task"]
        print(
            dim(
                f"meanwhile {count_events()} events are durable in {DB_PATH}; "
                f"the tool result returned on its own stream → {json.dumps(_result_text(result))}"
            )
        )

        banner("reconnect with Last-Event-ID")
        print(dim(f"GET {URL}  →  Last-Event-ID: {last_id}\n"))
        await read_get(client, auth, label="replay", paint=yellow, seen=seen, last_event_id=last_id, stop_value=STEPS)

        banner("result")
        if seen == list(range(1, STEPS + 1)):
            print(green(bold(f"  ✓ resumed cleanly — steps 1..{STEPS} received exactly once, zero loss")))
        else:
            print(red(bold(f"  ✗ gaps/dupes detected: saw {seen}")))
        print(
            dim(
                f"    live before crash: {list(range(1, DROP_AFTER + 1))}    "
                f"replayed after reconnect: {list(range(DROP_AFTER + 1, STEPS + 1))}"
            )
        )


def count_events() -> int:
    with contextlib.closing(sqlite3.connect(DB_PATH)) as db:
        return db.execute("SELECT count(*) FROM mcp_events").fetchone()[0]


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def main() -> None:
    with contextlib.suppress(FileNotFoundError):
        DB_PATH.unlink()  # fresh store each run

    print(bold("mcp-persist · SSE resumability demo"))
    print(dim("a real MCP server (SQLiteEventStore) + a real client that crashes and resumes\n"))

    server = start_server()
    print(green("✓") + f" server listening on {URL}  {dim('(SQLiteEventStore → ' + str(DB_PATH) + ')')}")
    try:
        asyncio.run(run_client())
    finally:
        server.should_exit = True
        time.sleep(0.3)
    print()


if __name__ == "__main__":
    main()
