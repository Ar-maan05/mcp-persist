# pyright: reportPrivateUsage=false
"""Tests for single-stream export/import (dump/load)."""

from __future__ import annotations

import asyncio

import aiosqlite
import pytest
from mcp.types import JSONRPCMessage, JSONRPCRequest

from mcp_persist import SQLiteEventStore, export_stream, import_stream
from mcp_persist.portability import DUMP_FORMAT, DUMP_VERSION


def _msg(i: int) -> JSONRPCMessage:
    return JSONRPCMessage(JSONRPCRequest(jsonrpc="2.0", id=str(i), method="tools/call", params={"n": i}))


async def _make_store(path: str, ttl: int | None = 3600) -> tuple[aiosqlite.Connection, SQLiteEventStore]:
    conn = await aiosqlite.connect(path)
    store = SQLiteEventStore(conn, ttl=ttl)
    await store.initialize()
    return conn, store


def test_export_shape_and_priming_event(tmp_path):
    db = str(tmp_path / "src.db")

    async def run() -> dict:
        conn, store = await _make_store(db)
        for i in range(3):
            await store.store_event("chat", _msg(i))
        await store.store_event("chat", None)  # priming event
        doc = await export_stream(store, "chat", backend="sqlite")
        await conn.close()
        return doc

    doc = asyncio.run(run())
    assert doc["format"] == DUMP_FORMAT
    assert doc["version"] == DUMP_VERSION
    assert doc["stream_id"] == "chat"
    assert doc["backend"] == "sqlite"
    assert isinstance(doc["exported_at"], float)
    assert len(doc["events"]) == 4
    # First three carry a JSON-RPC message; the last is a priming event.
    assert all(e["message"] is not None for e in doc["events"][:3])
    assert doc["events"][3]["message"] is None
    assert doc["events"][0]["message"]["method"] == "tools/call"


def test_export_unknown_stream_is_empty(tmp_path):
    db = str(tmp_path / "src.db")

    async def run() -> dict:
        conn, store = await _make_store(db)
        doc = await export_stream(store, "does-not-exist")
        await conn.close()
        return doc

    doc = asyncio.run(run())
    assert doc["events"] == []
    assert "backend" not in doc  # omitted when not supplied


def test_round_trip_restores_content_and_order(tmp_path):
    src_db = str(tmp_path / "src.db")
    dst_db = str(tmp_path / "dst.db")

    async def run() -> list:
        conn, store = await _make_store(src_db)
        for i in range(5):
            await store.store_event("s", _msg(i))
        doc = await export_stream(store, "s")
        await conn.close()

        conn2, store2 = await _make_store(dst_db)
        written = await import_stream(store2, doc)
        assert written == 5
        restored = [(eid, m) async for eid, m in store2._iter_stream_events("s")]
        await conn2.close()
        return restored

    restored = asyncio.run(run())
    assert [m.root.params["n"] for _eid, m in restored] == [0, 1, 2, 3, 4]  # type: ignore[union-attr]
    # IDs are reassigned by the destination, so they are a fresh monotonic run.
    assert [int(eid) for eid, _m in restored] == sorted(int(eid) for eid, _m in restored)


def test_import_stream_id_override(tmp_path):
    dst_db = str(tmp_path / "dst.db")
    doc = {
        "format": DUMP_FORMAT,
        "version": DUMP_VERSION,
        "stream_id": "original",
        "events": [{"event_id": "1", "message": _msg(1).model_dump(mode="json", by_alias=True)}],
    }

    async def run() -> list:
        conn, store = await _make_store(dst_db)
        await import_stream(store, doc, stream_id="renamed")
        streams = [s async for s in store.list_streams()]
        await conn.close()
        return streams

    assert asyncio.run(run()) == ["renamed"]


@pytest.mark.parametrize(
    "bad,match",
    [
        ({"format": "other", "version": 1, "events": []}, "not an mcp-persist dump"),
        ({"format": DUMP_FORMAT, "version": 999, "events": []}, "unsupported dump version"),
        ({"format": DUMP_FORMAT, "version": DUMP_VERSION, "events": {}}, "must be a list"),
        (["not", "a", "dict"], "must be a JSON object"),
    ],
)
def test_import_rejects_bad_documents(tmp_path, bad, match):
    dst_db = str(tmp_path / "dst.db")

    async def run() -> None:
        conn, store = await _make_store(dst_db)
        try:
            with pytest.raises(ValueError, match=match):
                await import_stream(store, bad)
        finally:
            await conn.close()

    asyncio.run(run())


def test_import_requires_a_stream_id(tmp_path):
    dst_db = str(tmp_path / "dst.db")
    doc = {"format": DUMP_FORMAT, "version": DUMP_VERSION, "events": []}  # no stream_id, none supplied

    async def run() -> None:
        conn, store = await _make_store(dst_db)
        try:
            with pytest.raises(ValueError, match="no stream_id"):
                await import_stream(store, doc)
        finally:
            await conn.close()

    asyncio.run(run())
