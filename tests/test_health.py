# pyright: reportPrivateUsage=false
"""Tests for structured store health probes."""

from __future__ import annotations

import asyncio

import aiosqlite
from mcp_types import JSONRPCRequest

from mcp_persist import SQLiteEventStore
from mcp_persist.health import HealthReport, probe_health


def test_sqlite_health_reports_healthy_with_size(tmp_path):
    db = str(tmp_path / "h.db")

    async def run() -> HealthReport:
        conn = await aiosqlite.connect(db)
        store = SQLiteEventStore(conn, ttl=None)
        await store.initialize()
        await store.store_event("s", JSONRPCRequest(jsonrpc="2.0", id="1", method="ping"))
        report = await store.health()
        await conn.close()
        return report

    report = asyncio.run(run())
    assert report.healthy is True
    assert report.backend == "sqlite"
    assert report.latency_ms is not None and report.latency_ms >= 0
    assert isinstance(report.detail["size_bytes"], int) and report.detail["size_bytes"] > 0


def test_health_report_as_dict_rounds_latency():
    report = HealthReport(healthy=True, backend="sqlite", latency_ms=1.23456, detail={"size_bytes": 4096})
    assert report.as_dict() == {
        "healthy": True,
        "backend": "sqlite",
        "latency_ms": 1.235,
        "detail": {"size_bytes": 4096},
    }


def test_probe_health_folds_ping_failure_into_report():
    class _Boom:
        async def ping(self) -> bool:
            raise RuntimeError("connection reset")

    report = asyncio.run(probe_health(_Boom(), "postgres"))
    assert report.healthy is False
    assert report.latency_ms is None
    assert "connection reset" in report.detail["error"]


def test_probe_health_falsy_ping_is_unhealthy():
    class _Falsy:
        async def ping(self) -> bool:
            return False

    report = asyncio.run(probe_health(_Falsy(), "redis"))
    assert report.healthy is False
    assert report.latency_ms is not None  # a round trip completed, it just answered falsy
    assert report.detail["error"] == "ping returned falsy"
