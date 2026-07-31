# pyright: reportPrivateUsage=false
"""Tests for admin purge and migrate subcommands."""

from __future__ import annotations

import asyncio
import base64
import json
import time

import aiosqlite
import pytest
from mcp_types import JSONRPCRequest

from mcp_persist import SQLiteEventStore, _admin
from mcp_persist.encryption import KeyRing, generate_key

SAMPLE = JSONRPCRequest(jsonrpc="2.0", id="1", method="ping")


def _migrate_argv(src, dst, *extra: str) -> list[str]:
    return [
        "migrate",
        "--from-backend",
        "sqlite",
        "--from-url",
        str(src),
        "--to-backend",
        "sqlite",
        "--to-url",
        str(dst),
        *extra,
    ]


def _seed(db, *, events: int = 1, stream: str = "stream-x", **store_kwargs) -> None:
    """Write ``events`` events into ``db`` with the given store configuration."""

    async def run() -> None:
        async with SQLiteEventStore.create(str(db), **store_kwargs) as store:
            for _ in range(events):
                await store.store_event(stream, SAMPLE)

    asyncio.run(run())


def _read_rows(db, table: str = "mcp_events") -> list[tuple[str | None, str]]:
    async def run() -> list[tuple[str | None, str]]:
        async with aiosqlite.connect(str(db)) as conn:
            async with conn.execute(f"SELECT tenant_id, stream_id FROM {table} ORDER BY event_id") as cur:
                return [(r[0], r[1]) for r in await cur.fetchall()]

    return asyncio.run(run())


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


def test_purge_dry_run_honors_json(tmp_path, capsys):
    """--json applies to a dry run too, not only to a real purge."""
    db = tmp_path / "purge-json.db"
    _seed(db, ttl=60)

    args = _admin._parse_args(["purge", "--backend", "sqlite", "--url", str(db), "--ttl", "60", "--dry-run", "--json"])
    assert _admin._run_purge(args) == 0
    assert json.loads(capsys.readouterr().out) == {"purged": 0, "dry_run": True}


def test_migrate_reads_an_encrypted_source_with_the_env_keyring(tmp_path, capsys, monkeypatch):
    """The keyring reaches the source store, so encrypted payloads are copied."""
    pytest.importorskip("cryptography")
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    key = generate_key()
    _seed(src, events=2, keyring=KeyRing({"k1": base64.b64decode(key)}, active_key_id="k1"))
    monkeypatch.setenv("MCP_PERSIST_ENCRYPTION_KEY", key)
    monkeypatch.setenv("MCP_PERSIST_ENCRYPTION_KEY_ID", "k1")

    assert _admin._run_migrate(_admin._parse_args(_migrate_argv(src, dst, "--json"))) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["events_migrated"] == 2
    assert data["skipped_events"] == 0


def test_migrate_fails_loudly_when_the_source_cannot_be_decoded(tmp_path, capsys, monkeypatch):
    """An unreadable source must not look like a clean migration of an empty store.

    The read path skips an undecryptable event rather than raising, so before the
    skip count existed this exited 0 reporting a successful copy of 0 events.
    """
    pytest.importorskip("cryptography")
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src, events=2, keyring=KeyRing({"k1": base64.b64decode(generate_key())}, active_key_id="k1"))
    monkeypatch.delenv("MCP_PERSIST_ENCRYPTION_KEY", raising=False)

    assert _admin._run_migrate(_admin._parse_args(_migrate_argv(src, dst, "--json"))) == 1
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["events_migrated"] == 0
    assert data["skipped_events"] == 2
    assert "NOT copied" in captured.err


def test_migrate_scopes_each_side_to_its_tenant(tmp_path, capsys):
    """Per-side --tenant-id both filters the source and binds the destination."""
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src, stream="stream-a", tenant_id="team-a")
    _seed(src, stream="stream-b", tenant_id="team-b")

    argv = _migrate_argv(src, dst, "--from-tenant-id", "team-a", "--to-tenant-id", "team-a", "--json")
    assert _admin._run_migrate(_admin._parse_args(argv)) == 0
    assert json.loads(capsys.readouterr().out)["events_migrated"] == 1
    assert _read_rows(dst) == [("team-a", "stream-a")]


def test_migrate_tenant_falls_back_to_the_environment(tmp_path, capsys, monkeypatch):
    """With no per-side flag, MCP_PERSIST_TENANT_ID scopes both stores."""
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src, stream="stream-a", tenant_id="team-a")
    _seed(src, stream="stream-b", tenant_id="team-b")
    monkeypatch.setenv("MCP_PERSIST_TENANT_ID", "team-b")

    assert _admin._run_migrate(_admin._parse_args(_migrate_argv(src, dst, "--json"))) == 0
    assert json.loads(capsys.readouterr().out)["events_migrated"] == 1
    assert _read_rows(dst) == [("team-b", "stream-b")]


def test_migrate_honors_a_non_default_table_name(tmp_path, capsys):
    """Without --from-table the source's custom table is invisible."""
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _seed(src, events=2, table_name="custom_events")

    argv = _migrate_argv(src, dst, "--from-table", "custom_events", "--to-table", "custom_events", "--json")
    assert _admin._run_migrate(_admin._parse_args(argv)) == 0
    assert json.loads(capsys.readouterr().out)["events_migrated"] == 2
    assert len(_read_rows(dst, "custom_events")) == 2


def test_migrate_rejects_an_unusable_compression_codec(tmp_path):
    """Bad config fails before either store is opened."""
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    argv = _migrate_argv(src, dst, "--from-compression", "gzip")
    args = _admin._parse_args(argv)
    args.from_compression = "nonsense"  # argparse choices would reject this at the flag
    with pytest.raises(SystemExit) as excinfo:
        _admin._run_migrate(args)
    assert excinfo.value.code == 2
