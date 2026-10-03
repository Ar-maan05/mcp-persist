"""Tests for the ``mcp-persist sessions`` command.

Driven through ``main()`` against a real SQLite store, because the value of
these paths is the exit codes and the operator-facing output, not the registry
calls underneath (those are covered in test_durable_sessions.py).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from mcp_persist import SQLiteEventStore, session_registry_for
from mcp_persist._admin import main


def _seed(db: str, *ids: str, handshake: dict[str, object] | None = None) -> None:
    """Create the session records the CLI will read.

    Synchronous on purpose: the CLI calls ``asyncio.run`` itself, which cannot
    run inside an already-running loop, so these tests must not be async.
    """

    async def go() -> None:
        async with SQLiteEventStore.create(db) as store:
            registry = session_registry_for(store)
            await registry.initialize()
            for session_id in ids:
                await registry.register(session_id, handshake=handshake)

    asyncio.run(go())


def _run(monkeypatch: pytest.MonkeyPatch, db: str, *argv: str) -> int:
    monkeypatch.setenv("MCP_PERSIST_BACKEND", "sqlite")
    monkeypatch.setenv("MCP_PERSIST_URL", db)
    monkeypatch.setattr("sys.argv", ["mcp-persist", "sessions", *argv])
    with pytest.raises(SystemExit) as excinfo:
        main()
    return int(excinfo.value.code or 0)


def test_list_and_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    db = str(tmp_path / "e.db")
    _seed(db, "sess-one", "sess-two")

    assert _run(monkeypatch, db, "list") == 0
    assert "sess-one" in capsys.readouterr().out

    assert _run(monkeypatch, db, "list", "--json") == 0
    payload = json.loads(capsys.readouterr().out)
    assert {s["session_id"] for s in payload["sessions"]} == {"sess-one", "sess-two"}


def test_empty_list_hints_at_the_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """An empty list usually means the server never had durable sessions on."""
    db = str(tmp_path / "e.db")
    _seed(db)
    assert _run(monkeypatch, db, "list") == 0
    assert "durable_sessions" in capsys.readouterr().out


def test_terminate_then_hidden_from_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = str(tmp_path / "e.db")
    _seed(db, "sess-one")

    assert _run(monkeypatch, db, "terminate", "sess-one") == 0
    assert json.loads(capsys.readouterr().out)["terminated"] is True

    assert _run(monkeypatch, db, "list") == 0
    assert "sess-one" not in capsys.readouterr().out

    assert _run(monkeypatch, db, "list", "--all") == 0
    assert "sess-one" in capsys.readouterr().out


def test_unknown_session_exits_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A typo in the id must not look like a session that was really ended."""
    db = str(tmp_path / "e.db")
    _seed(db, "sess-one")

    assert _run(monkeypatch, db, "show", "nope") == 1
    assert "no such session" in capsys.readouterr().err

    assert _run(monkeypatch, db, "terminate", "nope") == 1
    assert "no such session" in capsys.readouterr().err


def test_purge_requires_a_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = str(tmp_path / "e.db")
    _seed(db, "sess-one")

    assert _run(monkeypatch, db, "purge") == 2
    assert "--older-than" in capsys.readouterr().err

    assert _run(monkeypatch, db, "purge", "--older-than", "0s", "--json") == 0
    assert json.loads(capsys.readouterr().out) == {"purged": 1}


def test_missing_id_and_stray_flag_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = str(tmp_path / "e.db")
    _seed(db, "sess-one")

    assert _run(monkeypatch, db, "show") == 2
    assert "requires a session id" in capsys.readouterr().err

    assert _run(monkeypatch, db, "list", "--older-than", "1d") == 2
    assert "only applies" in capsys.readouterr().err


def test_list_names_each_sessions_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = str(tmp_path / "e.db")
    _seed(db, "sess-known", handshake={"clientInfo": {"name": "inspector", "version": "0.9"}})
    _seed(db, "sess-legacy")

    assert _run(monkeypatch, db, "list") == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split()[-1] == "CLIENT"
    known = next(line for line in lines if line.startswith("sess-known"))
    legacy = next(line for line in lines if line.startswith("sess-legacy"))
    assert known.endswith("inspector 0.9")
    assert legacy.endswith(" -")

    assert _run(monkeypatch, db, "list", "--json") == 0
    clients = {s["session_id"]: s["client"] for s in json.loads(capsys.readouterr().out)["sessions"]}
    assert clients == {"sess-known": "inspector 0.9", "sess-legacy": None}
