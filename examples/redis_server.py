"""
examples/redis_server.py
========================
Minimal MCP server using RedisEventStore for SSE resumability.

Ideal for: multi-process / multi-worker deployments where multiple server
instances share state via Redis. Clients can reconnect to *any* worker and
have missed events replayed.

Install extras:
    pip install "mcp-persist[redis]"
    pip install uvicorn starlette

Run (requires a local Redis instance):
    redis-server &
    python examples/redis_server.py

The server exposes an MCP endpoint at http://localhost:8000/mcp
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import redis.asyncio as aioredis
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp_persist import RedisEventStore
from starlette.applications import Starlette
from starlette.routing import Mount

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")

# ---------------------------------------------------------------------------
# In-memory application state
# ---------------------------------------------------------------------------

@dataclass
class Note:
    id: str
    title: str
    body: str
    created_at: float = field(default_factory=time.time)


_notes: dict[str, Note] = {}

# ---------------------------------------------------------------------------
# MCP server — tools and resources
# ---------------------------------------------------------------------------

mcp = FastMCP(name="NoteServer")


@mcp.tool()
def add_note(title: str, body: str) -> dict[str, str]:
    """Create a new note and return its ID."""
    note_id = uuid.uuid4().hex[:8]
    _notes[note_id] = Note(id=note_id, title=title, body=body)
    return {"note_id": note_id, "status": "created"}


@mcp.tool()
def list_notes() -> list[dict[str, Any]]:
    """Return a summary list of all notes."""
    return [
        {"note_id": n.id, "title": n.title, "created_at": n.created_at}
        for n in sorted(_notes.values(), key=lambda n: n.created_at)
    ]


@mcp.tool()
def get_note(note_id: str) -> dict[str, Any]:
    """Fetch a single note by its ID."""
    note = _notes.get(note_id)
    if note is None:
        return {"error": f"Note '{note_id}' not found"}
    return {"note_id": note.id, "title": note.title, "body": note.body}


@mcp.tool()
async def slow_echo(message: str, delay: float = 1.0) -> dict[str, Any]:
    """Echo a message after a delay — useful for observing SSE keepalives."""
    await asyncio.sleep(delay)
    return {"echo": message, "delay": delay}


@mcp.resource("notes://all")
def all_notes() -> str:
    """All notes as plain text."""
    if not _notes:
        return "No notes yet.\n"
    lines = ["Notes", "-----"]
    for n in sorted(_notes.values(), key=lambda n: n.created_at):
        lines.append(f"[{n.id}] {n.title}")
    return "\n".join(lines) + "\n"


@mcp.resource("notes://{note_id}")
def single_note(note_id: str) -> str:
    """Full content of a single note."""
    note = _notes.get(note_id)
    if note is None:
        return f"Note '{note_id}' not found.\n"
    return f"Title: {note.title}\n\n{note.body}\n"


# ---------------------------------------------------------------------------
# ASGI app with RedisEventStore
# ---------------------------------------------------------------------------

REDIS_URL = "redis://localhost:6379"
KEY_PREFIX = "mcp-notes:"


@contextlib.asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[None]:
    redis_client = aioredis.from_url(REDIS_URL)
    try:
        store = RedisEventStore(redis_client, key_prefix=KEY_PREFIX, ttl=3600)

        session_manager = StreamableHTTPSessionManager(
            app=mcp._mcp_server,
            event_store=store,
            session_idle_timeout=300,
        )
        app.state.session_manager = session_manager

        async with session_manager.run():
            logging.info("Server ready — Redis event store at %s (prefix=%s)", REDIS_URL, KEY_PREFIX)
            yield
    finally:
        await redis_client.aclose()


async def handle_mcp(scope: Any, receive: Any, send: Any) -> None:
    await scope["app"].state.session_manager.handle_request(scope, receive, send)


app = Starlette(
    lifespan=lifespan,
    routes=[Mount("/mcp", app=handle_mcp)],
)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
