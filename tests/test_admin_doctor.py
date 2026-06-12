"""Tests for the ``mcp-persist doctor`` diagnostic CLI.

These exercise the pure logic without binding ports or needing a real backend:
config resolution from flags and env, each individual check, the resilient
``diagnose`` flow (driver missing, store down), and the two renderers. A fake
store stands in for the live connection so connectivity is tested without a
server.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager

import pytest

from mcp_persist import _admin
from mcp_persist._admin import Check, StoreConfig

# Config resolution


def test_resolve_config_prefers_flags(monkeypatch):
    monkeypatch.delenv("MCP_PERSIST_BACKEND", raising=False)
    args = _admin._parse_args(["doctor", "--backend", "sqlite", "--url", "e.db", "--ttl", "60"])
    cfg = _admin._resolve_config(args)
    assert (cfg.backend, cfg.url, cfg.ttl) == ("sqlite", "e.db", 60)


def test_resolve_config_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("MCP_PERSIST_BACKEND", "Redis")  # case/space tolerant
    monkeypatch.setenv("MCP_PERSIST_URL", "redis://h:6379")
    monkeypatch.setenv("MCP_PERSIST_TTL", "300")
    monkeypatch.setenv("MCP_PERSIST_MAX_STREAM_LENGTH", "1000")
    cfg = _admin._resolve_config(_admin._parse_args(["doctor"]))
    assert (cfg.backend, cfg.url, cfg.ttl, cfg.max_stream_length) == ("redis", "redis://h:6379", 300, 1000)


def test_resolve_config_requires_backend(monkeypatch):
    monkeypatch.delenv("MCP_PERSIST_BACKEND", raising=False)
    with pytest.raises(ValueError, match="set --backend"):
        _admin._resolve_config(_admin._parse_args(["doctor", "--url", "e.db"]))


def test_resolve_config_requires_url(monkeypatch):
    monkeypatch.delenv("MCP_PERSIST_URL", raising=False)
    with pytest.raises(ValueError, match="set --url"):
        _admin._resolve_config(_admin._parse_args(["doctor", "--backend", "sqlite"]))


def test_resolve_config_rejects_unknown_backend(monkeypatch):
    monkeypatch.delenv("MCP_PERSIST_BACKEND", raising=False)
    args = _admin._parse_args(["doctor", "--url", "e.db"])
    args.backend = "mongo"  # bypass argparse choices to hit the validation branch
    with pytest.raises(ValueError, match="unknown backend"):
        _admin._resolve_config(args)


# Individual checks


def test_check_python_pass():
    assert _admin._check_python().status == "pass"


def test_check_python_fail(monkeypatch):
    monkeypatch.setattr(_admin, "_MIN_PYTHON", (99, 0))
    assert _admin._check_python().status == "fail"


def test_check_driver_installed():
    # aiosqlite is a test dependency, so the sqlite driver is always present here.
    assert _admin._check_driver("sqlite").status == "pass"


def test_check_driver_missing(monkeypatch):
    monkeypatch.setattr(_admin.importlib.util, "find_spec", lambda name: None)
    check = _admin._check_driver("postgres")
    assert check.status == "fail"
    assert "mcp-persist[postgres]" in check.detail


def test_check_retention_pass_when_ttl_set():
    cfg = StoreConfig(backend="sqlite", url="e.db", ttl=60)
    checks = _admin._check_retention(cfg)
    assert [c.status for c in checks] == ["pass"]


def test_check_retention_warns_sqlite_without_ttl():
    checks = _admin._check_retention(StoreConfig(backend="sqlite", url="e.db", ttl=None))
    assert checks[0].status == "warn" and "purge_expired" in checks[0].detail


def test_check_retention_double_warns_redis_maxlen_without_ttl():
    cfg = StoreConfig(backend="redis", url="redis://h", ttl=None, max_stream_length=100)
    statuses = [c.status for c in _admin._check_retention(cfg)]
    assert statuses == ["warn", "warn"]


# diagnose flow


class _FakeStore:
    def __init__(self, *, ping_ok: bool = True) -> None:
        self._ping_ok = ping_ok

    async def ping(self) -> bool:
        if not self._ping_ok:
            raise ConnectionError("refused")
        return True


def _fake_open(store: _FakeStore):
    @asynccontextmanager
    async def _open():
        yield store

    return _open


@pytest.mark.anyio
async def test_diagnose_all_pass_with_ttl():
    cfg = StoreConfig(backend="sqlite", url="e.db", ttl=60)
    checks = await _admin.diagnose(cfg, open_store=_fake_open(_FakeStore()))
    names = [c.name for c in checks]
    assert names == ["python", "driver", "connectivity", "retention"]
    assert all(c.status == "pass" for c in checks)


@pytest.mark.anyio
async def test_diagnose_connectivity_failure_is_reported_not_raised():
    cfg = StoreConfig(backend="sqlite", url="e.db", ttl=60)
    checks = await _admin.diagnose(cfg, open_store=_fake_open(_FakeStore(ping_ok=False)))
    conn = next(c for c in checks if c.name == "connectivity")
    assert conn.status == "fail" and "refused" in conn.detail


@pytest.mark.anyio
async def test_diagnose_skips_connectivity_when_driver_missing(monkeypatch):
    monkeypatch.setattr(_admin.importlib.util, "find_spec", lambda name: None)
    cfg = StoreConfig(backend="postgres", url="postgres://h", ttl=60)
    # open_store must never be called when the driver is absent.
    def _boom():
        raise AssertionError("open_store called despite missing driver")

    checks = await _admin.diagnose(cfg, open_store=_boom)
    conn = next(c for c in checks if c.name == "connectivity")
    assert conn.status == "fail" and "driver is not installed" in conn.detail


# Rendering


def test_render_human_marks_failures():
    cfg = StoreConfig(backend="redis", url="redis://h", ttl=None)
    checks = [
        Check("python", "pass", "ok"),
        Check("connectivity", "fail", "down"),
        Check("retention", "warn", "no ttl"),
    ]
    out = _admin._render(cfg, checks)
    assert "[fail]" in out and "[warn]" in out
    assert "1 failed, 1 warning(s)" in out


def test_render_json_round_trips():
    cfg = StoreConfig(backend="sqlite", url="e.db", ttl=60)
    checks = [Check("python", "pass", "ok"), Check("connectivity", "fail", "down")]
    payload = json.loads(_admin._render_json(cfg, checks))
    assert payload["ok"] is False
    assert payload["backend"] == "sqlite"
    assert [c["name"] for c in payload["checks"]] == ["python", "connectivity"]


# main()


def test_main_doctor_exit_code_on_failure(monkeypatch, capsys):
    cfg = StoreConfig(backend="sqlite", url="e.db", ttl=60)
    monkeypatch.setattr(_admin.sys, "argv", ["mcp-persist", "doctor", "--backend", "sqlite", "--url", "e.db"])
    monkeypatch.setattr(_admin, "_resolve_config", lambda args: cfg)

    async def _fake_diagnose(cfg, **kwargs):
        return [Check("connectivity", "fail", "down")]

    monkeypatch.setattr(_admin, "diagnose", _fake_diagnose)
    with pytest.raises(SystemExit) as exc:
        _admin.main()
    assert exc.value.code == 1
    assert "[fail]" in capsys.readouterr().out
