# pyright: reportPrivateUsage=false
"""Tests for admin purge and migrate subcommands."""

from __future__ import annotations

import asyncio
import json
import time

import aiosqlite
from mcp.types import JSONRPCRequest

from mcp_persist import SQLiteEventStore, _admin

SAMPLE = JSONRPCRequest(jsonrpc="2.0", id="1", method="ping")


def test_purge_dry_run_and_delete(tmp_path, capsys):
    db = tmp_path / "purge.db"

    async def setup() -> None:
        conn = await aiosqlite.connect(str(db))
        store = SQLiteEventStore(conn, ttl=60)
        await store.initialize()
        await store.store_event("s", SAMPLE)
        await conn.execute("UPDATE mcp_events SET created_at = ?", (time.time() - 120,))
        await conn.commit()
        await conn.close()

    asyncio.run(setup())

    args = _admin._parse_args(["purge", "--backend", "sqlite", "--url", str(db), "--ttl", "60", "--dry-run"])
    assert _admin._run_purge(args) == 0
    assert "would purge 1" in capsys.readouterr().out

    args = _admin._parse_args(["purge", "--backend", "sqlite", "--url", str(db), "--ttl", "60"])
    assert _admin._run_purge(args) == 0
    assert "purged 1" in capsys.readouterr().out


def test_migrate_cli_sqlite_to_sqlite(tmp_path, capsys):
    src = tmp_path / "src.db"
    dst = tmp_path / "dst.db"

    async def setup() -> None:
        conn = await aiosqlite.connect(str(src))
        store = SQLiteEventStore(conn, ttl=None)
        await store.initialize()
        await store.store_event("stream-x", SAMPLE)
        await conn.close()

    asyncio.run(setup())

    args = _admin._parse_args(
        [
            "migrate",
            "--from-backend",
            "sqlite",
            "--from-url",
            str(src),
            "--to-backend",
            "sqlite",
            "--to-url",
            str(dst),
        ]
    )
    assert _admin._run_migrate(args) == 0
    assert "migrated 1 event" in capsys.readouterr().out

    args = _admin._parse_args(
        [
            "migrate",
            "--from-backend",
            "sqlite",
            "--from-url",
            str(src),
            "--to-backend",
            "sqlite",
            "--to-url",
            str(dst),
            "--json",
        ]
    )
    _admin._run_migrate(args)
    data = json.loads(capsys.readouterr().out)
    assert data["events_migrated"] == 1
