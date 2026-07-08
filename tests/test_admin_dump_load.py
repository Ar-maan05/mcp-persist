# pyright: reportPrivateUsage=false
"""Tests for the admin dump / load subcommands, duration parsing, and older-than purge."""

from __future__ import annotations

import asyncio
import json
import time

import aiosqlite
import pytest
from mcp.types import JSONRPCMessage, JSONRPCRequest

from mcp_persist import SQLiteEventStore, _admin


def _msg(i: int) -> JSONRPCMessage:
    return JSONRPCMessage(JSONRPCRequest(jsonrpc="2.0", id=str(i), method="ping", params={"n": i}))


async def _seed(path: str, stream: str, count: int, *, ttl: int | None = 3600) -> None:
    conn = await aiosqlite.connect(path)
    store = SQLiteEventStore(conn, ttl=ttl)
    await store.initialize()
    for i in range(count):
        await store.store_event(stream, _msg(i))
    await conn.close()


@pytest.mark.parametrize(
    "text,expected",
    [
        ("3600", 3600.0),
        ("3600s", 3600.0),
        ("45m", 2700.0),
        ("12h", 43200.0),
        ("30d", 2592000.0),
        ("2w", 1209600.0),
        ("1.5h", 5400.0),
    ],
)
def test_parse_duration_ok(text, expected):
    assert _admin.parse_duration(text) == expected


@pytest.mark.parametrize("bad", ["", "30x", "abc", "-5d", "d"])
def test_parse_duration_rejects_bad(bad):
    with pytest.raises(ValueError):
        _admin.parse_duration(bad)


def test_dump_to_file_then_load_round_trip(tmp_path, capsys):
    src = tmp_path / "src.db"
    dst = tmp_path / "dst.db"
    dump_path = tmp_path / "dump.json"
    asyncio.run(_seed(str(src), "chat", 3))

    args = _admin._parse_args(["dump", "chat", "--backend", "sqlite", "--url", str(src), "-o", str(dump_path)])
    assert _admin._run_dump(args) == 0
    doc = json.loads(dump_path.read_text())
    assert doc["stream_id"] == "chat" and len(doc["events"]) == 3

    args = _admin._parse_args(["load", str(dump_path), "--backend", "sqlite", "--url", str(dst)])
    assert _admin._run_load(args) == 0
    assert "loaded 3 event(s) into stream chat" in capsys.readouterr().out

    # The restored store reports the same three events.
    args = _admin._parse_args(["stats", "--backend", "sqlite", "--url", str(dst), "--json"])
    assert _admin._run_stats(args) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["total_events"] == 3


def test_dump_to_stdout(tmp_path, capsys):
    src = tmp_path / "src.db"
    asyncio.run(_seed(str(src), "s", 2))
    args = _admin._parse_args(["dump", "s", "--backend", "sqlite", "--url", str(src), "--json"])
    assert _admin._run_dump(args) == 0
    doc = json.loads(capsys.readouterr().out)
    assert len(doc["events"]) == 2


def test_load_with_stream_id_override(tmp_path, capsys):
    dst = tmp_path / "dst.db"
    dump_path = tmp_path / "dump.json"
    dump_path.write_text(
        json.dumps(
            {
                "format": "mcp-persist-dump",
                "version": 1,
                "stream_id": "old",
                "events": [{"event_id": "1", "message": _msg(1).model_dump(mode="json", by_alias=True)}],
            }
        )
    )
    args = _admin._parse_args(["load", str(dump_path), "--backend", "sqlite", "--url", str(dst), "--stream-id", "new"])
    assert _admin._run_load(args) == 0
    assert "into stream new" in capsys.readouterr().out


def test_load_rejects_bad_dump(tmp_path):
    dst = tmp_path / "dst.db"
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"format": "nope", "version": 1, "events": []}))
    args = _admin._parse_args(["load", str(bad), "--backend", "sqlite", "--url", str(dst)])
    with pytest.raises(SystemExit) as exc:
        _admin._run_load(args)
    assert exc.value.code == 2


def test_purge_older_than(tmp_path, capsys):
    db = tmp_path / "purge.db"

    async def setup() -> None:
        conn = await aiosqlite.connect(str(db))
        # No ttl configured: older-than must still work.
        store = SQLiteEventStore(conn, ttl=None)
        await store.initialize()
        await store.store_event("s", _msg(0))
        await conn.execute("UPDATE mcp_events SET created_at = ?", (time.time() - 86400 * 10,))
        await conn.commit()
        await conn.close()

    asyncio.run(setup())

    # Dry run counts by age, no ttl needed.
    args = _admin._parse_args(["purge", "--backend", "sqlite", "--url", str(db), "--older-than", "7d", "--dry-run"])
    assert _admin._run_purge(args) == 0
    assert "would purge 1" in capsys.readouterr().out

    # A newer cutoff leaves the row alone.
    args = _admin._parse_args(["purge", "--backend", "sqlite", "--url", str(db), "--older-than", "30d"])
    assert _admin._run_purge(args) == 0
    assert "purged 0" in capsys.readouterr().out

    # The 7d cutoff removes it.
    args = _admin._parse_args(["purge", "--backend", "sqlite", "--url", str(db), "--older-than", "7d"])
    assert _admin._run_purge(args) == 0
    assert "purged 1" in capsys.readouterr().out


def test_purge_older_than_rejected_for_redis(capsys):
    args = _admin._parse_args(["purge", "--backend", "redis", "--url", "redis://localhost", "--older-than", "30d"])
    with pytest.raises(SystemExit) as exc:
        _admin._run_purge(args)
    assert exc.value.code == 2
    assert "not supported for redis" in capsys.readouterr().err
