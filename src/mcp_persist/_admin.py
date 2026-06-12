"""``mcp-persist`` admin CLI: diagnostics and (later) inspection subcommands.

This is the home of the operator-facing subcommands that are not the proxy. The
proxy keeps its own focused entry point (``mcp-persist-proxy``); everything you
run to inspect or check a deployment lives here under ``mcp-persist <command>``.

The first subcommand is ``doctor``, a pass/fail checklist for the things that
usually explain a broken or silently degrading store: the Python runtime, a
missing backend driver extra, live connectivity, and config that lets events
accumulate without bound. Doctor is deliberately resilient to a store that will
not open: the runtime, driver, and retention checks read resolved config and run
even when the backend is down, which is exactly when you reach for it.

Usage::

    mcp-persist doctor --backend sqlite --url events.db
    mcp-persist doctor                      # read MCP_PERSIST_* from the env
    mcp-persist doctor --json               # machine-readable checklist

Exit code is ``1`` when any check fails and ``0`` otherwise. Warnings (for
example an unset ``ttl``) are surfaced but do not fail the command, matching the
spirit of tools like ``flutter doctor``.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import logging
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractAsyncContextManager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, NoReturn

from mcp_persist.config import _PREFIX, _optional_int
from mcp_persist.postgres import PostgresEventStore
from mcp_persist.redis import RedisEventStore
from mcp_persist.sqlite import SQLiteEventStore

if TYPE_CHECKING:
    from mcp.server.streamable_http import EventStore

# The lowest Python the package supports (pyproject ``requires-python``).
_MIN_PYTHON = (3, 10)

# The import name of the driver each backend needs, used both for the "is the
# extra installed" check and for the pip hint when it is not.
_DRIVER_MODULE = {"sqlite": "aiosqlite", "redis": "redis", "postgres": "asyncpg"}
_DRIVER_EXTRA = {"sqlite": "sqlite", "redis": "redis", "postgres": "postgres"}

Status = Literal["pass", "warn", "fail"]


@dataclass(frozen=True)
class Check:
    """One diagnostic result: a short name, a status, and a human detail line."""

    name: str
    status: Status
    detail: str


@dataclass(frozen=True)
class StoreConfig:
    """The store settings doctor needs, resolved from CLI flags or the env.

    Only the fields the checks and the connection actually read are kept here;
    this is not a full mirror of every backend constructor argument.
    """

    backend: str
    url: str
    ttl: int | None = None
    table_name: str | None = None
    key_prefix: str | None = None
    max_stream_length: int | None = None


# Config resolution


def _resolve_config(args: argparse.Namespace) -> StoreConfig:
    """Build a :class:`StoreConfig` from CLI flags, falling back to ``MCP_PERSIST_*``.

    ``--backend``/``--url``/``--ttl``/``--table`` win when given; anything not
    passed is read from the environment, so ``mcp-persist doctor`` with no flags
    checks exactly the store a deployment is configured for. Raises ``ValueError``
    with an actionable message for a missing or unknown backend, a missing URL, or
    a non-integer ttl.
    """
    import os

    env = os.environ
    backend = (args.backend or env.get(f"{_PREFIX}BACKEND") or "").strip().lower()
    if not backend:
        raise ValueError("set --backend (sqlite|redis|postgres) or MCP_PERSIST_BACKEND")
    if backend not in _DRIVER_MODULE:
        raise ValueError(f"unknown backend {backend!r}: use sqlite, redis, or postgres")

    url = args.url or env.get(f"{_PREFIX}URL")
    if not url:
        raise ValueError(f"set --url or MCP_PERSIST_URL for the {backend} backend")

    ttl = args.ttl if args.ttl is not None else _optional_int(env, f"{_PREFIX}TTL")
    table_name = args.table or env.get(f"{_PREFIX}TABLE_NAME")
    key_prefix = env.get(f"{_PREFIX}KEY_PREFIX")
    max_stream_length = _optional_int(env, f"{_PREFIX}MAX_STREAM_LENGTH")

    return StoreConfig(
        backend=backend,
        url=url,
        ttl=ttl,
        table_name=table_name,
        key_prefix=key_prefix or None,
        max_stream_length=max_stream_length,
    )


@contextmanager
def _quiet_package_log() -> Iterator[None]:
    """Silence the ``mcp_persist`` logger below ERROR for the duration of the block.

    The stores log a ttl=None warning at construction. Both doctor and stats open
    a store of their own and report what they need from it directly, so the
    construction warning is noise that would clutter the checklist or the table.
    """
    package_log = logging.getLogger("mcp_persist")
    previous = package_log.level
    package_log.setLevel(logging.ERROR)
    try:
        yield
    finally:
        package_log.setLevel(previous)


def _build_store(cfg: StoreConfig) -> AbstractAsyncContextManager[EventStore]:
    """Open the configured store as an async context manager (connection closed on exit).

    Mirrors how :func:`mcp_persist.config.event_store_from_env` maps settings to a
    backend ``create``, passing only the fields doctor resolved. The connection is
    established on ``__aenter__`` and closed on ``__aexit__``.
    """
    if cfg.backend == "sqlite":
        kwargs: dict[str, object] = {"ttl": cfg.ttl}
        if cfg.table_name:
            kwargs["table_name"] = cfg.table_name
        return SQLiteEventStore.create(cfg.url, **kwargs)  # type: ignore[arg-type]
    if cfg.backend == "redis":
        kwargs = {"ttl": cfg.ttl}
        if cfg.key_prefix:
            kwargs["key_prefix"] = cfg.key_prefix
        if cfg.max_stream_length is not None:
            kwargs["max_stream_length"] = cfg.max_stream_length
        return RedisEventStore.create(cfg.url, **kwargs)  # type: ignore[arg-type]
    # postgres (the only remaining value; _resolve_config validated the set)
    kwargs = {"ttl": cfg.ttl}
    if cfg.table_name:
        kwargs["table_name"] = cfg.table_name
    return PostgresEventStore.create(cfg.url, **kwargs)  # type: ignore[arg-type]


# Individual checks


def _check_python() -> Check:
    major, minor = sys.version_info[:2]
    want = ".".join(str(p) for p in _MIN_PYTHON)
    got = f"{major}.{minor}.{sys.version_info[2]}"
    if (major, minor) >= _MIN_PYTHON:
        return Check("python", "pass", f"Python {got} (>= {want})")
    return Check("python", "fail", f"Python {got} is below the supported floor {want}")


def _check_driver(backend: str) -> Check:
    module = _DRIVER_MODULE[backend]
    if importlib.util.find_spec(module) is not None:
        return Check("driver", "pass", f"{module} is installed for the {backend} backend")
    extra = _DRIVER_EXTRA[backend]
    return Check(
        "driver",
        "fail",
        f"{module} is not installed; run: pip install 'mcp-persist[{extra}]'",
    )


async def _server_version(store: object, backend: str) -> str | None:
    """Best-effort backend version string for the connectivity detail line.

    Returns ``None`` on any failure so a version read never turns a healthy
    connection into a failed check.
    """
    try:
        if backend == "redis":
            info = await store._redis.info("server")  # type: ignore[attr-defined]
            version = info.get("redis_version")
            return f"redis {version}" if version else None
        if backend == "postgres":
            version = await store._pool.fetchval("SHOW server_version")  # type: ignore[attr-defined]
            return f"postgres {version}" if version else None
        if backend == "sqlite":
            import sqlite3

            return f"sqlite {sqlite3.sqlite_version}"
    except Exception:
        return None
    return None


async def _check_connectivity(
    cfg: StoreConfig,
    open_store: Callable[[], AbstractAsyncContextManager[EventStore]],
) -> Check:
    """Open the store and ping it, reporting the backend version when reachable.

    Doctor reports a ttl=None store as the retention check, so the construction
    warning is quieted here to keep the checklist the single source of truth.
    """
    try:
        with _quiet_package_log():
            async with open_store() as store:
                await store.ping()  # type: ignore[attr-defined]
                version = await _server_version(store, cfg.backend)
    except Exception as exc:
        return Check("connectivity", "fail", f"cannot reach {cfg.backend} at {cfg.url}: {exc}")
    suffix = f" ({version})" if version else ""
    return Check("connectivity", "pass", f"connected to {cfg.backend}{suffix}")


def _check_retention(cfg: StoreConfig) -> list[Check]:
    """Flag config that lets events accumulate without bound.

    These mirror the warnings the stores already log at construction, surfaced up
    front so an operator sees them before the store has run long enough to grow.
    """
    if cfg.ttl is not None and cfg.ttl > 0:
        return [Check("retention", "pass", f"ttl={cfg.ttl}s: events expire and are reclaimed")]

    checks: list[Check] = []
    if cfg.backend == "redis":
        checks.append(
            Check(
                "retention",
                "warn",
                "ttl is not set: events accumulate in Redis indefinitely; set --ttl "
                "(at least 2x your session idle timeout)",
            )
        )
        if cfg.max_stream_length is not None:
            checks.append(
                Check(
                    "retention",
                    "warn",
                    "max_stream_length is set but ttl is not: trimming drops old event IDs "
                    "while their payload hashes never expire; set --ttl",
                )
            )
    else:
        checks.append(
            Check(
                "retention",
                "warn",
                f"ttl is not set: {cfg.backend} has no auto expiry and purge_expired() is a "
                "no-op, so events accumulate; set --ttl and schedule PurgeScheduler",
            )
        )
    return checks


async def diagnose(
    cfg: StoreConfig,
    *,
    open_store: Callable[[], AbstractAsyncContextManager[EventStore]] | None = None,
) -> list[Check]:
    """Run every doctor check and return the results in display order.

    ``open_store`` builds the live store context manager; it defaults to
    :func:`_build_store` and is injectable so tests can supply a fake store. The
    runtime, driver, and retention checks do not touch it, so they still run when
    the driver is missing or the backend is down; connectivity is reported as a
    failure in that case rather than raising.
    """
    if open_store is None:
        open_store = lambda: _build_store(cfg)  # noqa: E731 - tiny factory, a def adds no clarity

    checks = [_check_python(), _check_driver(cfg.backend)]
    if checks[-1].status == "pass":
        checks.append(await _check_connectivity(cfg, open_store))
    else:
        checks.append(Check("connectivity", "fail", f"skipped: the {cfg.backend} driver is not installed"))
    checks.extend(_check_retention(cfg))
    return checks


# Rendering


_GLYPH = {"pass": "[ ok ]", "warn": "[warn]", "fail": "[fail]"}


def _render(cfg: StoreConfig, checks: list[Check]) -> str:
    width = max(len(c.name) for c in checks)
    lines = [f"mcp-persist doctor: {cfg.backend} ({cfg.url})", ""]
    lines += [f"{_GLYPH[c.status]} {c.name.ljust(width)}  {c.detail}" for c in checks]

    fails = sum(c.status == "fail" for c in checks)
    warns = sum(c.status == "warn" for c in checks)
    lines.append("")
    if fails:
        lines.append(f"{fails} failed, {warns} warning(s). Fix the failures above.")
    elif warns:
        lines.append(f"All checks passed with {warns} warning(s).")
    else:
        lines.append("All checks passed.")
    return "\n".join(lines)


def _render_json(cfg: StoreConfig, checks: list[Check]) -> str:
    return json.dumps(
        {
            "backend": cfg.backend,
            "url": cfg.url,
            "ok": not any(c.status == "fail" for c in checks),
            "checks": [{"name": c.name, "status": c.status, "detail": c.detail} for c in checks],
        }
    )


# Stats


@dataclass(frozen=True)
class StreamStat:
    """Per-stream counts: the number of stored events and their event ID range."""

    stream_id: str
    events: int
    min_event_id: int | None
    max_event_id: int | None


@dataclass(frozen=True)
class StatsReport:
    """A whole-store snapshot: per-stream rows plus totals and a latency probe."""

    backend: str
    streams: list[StreamStat]
    total_events: int
    total_streams: int
    last_event_id: int | None
    latency_ms: float


async def _redis_stats(store: object, stream_id: str | None) -> tuple[list[StreamStat], int | None]:
    """Count events per stream from the Redis sorted-set index, oldest/newest by score.

    Each stream is a ZSET whose scores are the (monotonic) event IDs, so ``ZCARD``
    is the count and the lowest/highest scores are the ID range. All reads for a
    pass are issued in one pipeline. ``last_event_id`` comes from the never-expired
    counter key, so it reflects the latest ID assigned even after old events expire.
    """
    redis = store._redis  # type: ignore[attr-defined]
    if stream_id is not None:
        stream_ids = [stream_id]
    else:
        stream_ids = [sid async for sid in store.list_streams()]  # type: ignore[attr-defined]

    stats: list[StreamStat] = []
    if stream_ids:
        async with redis.pipeline(transaction=False) as pipe:
            for sid in stream_ids:
                key = store._stream_key(sid)  # type: ignore[attr-defined]
                pipe.zcard(key)
                pipe.zrange(key, 0, 0, withscores=True)
                pipe.zrange(key, -1, -1, withscores=True)
            results = await pipe.execute()
        # An explicit --stream-id shows a zero row when the stream is absent; a
        # full listing only yields streams that exist, so empties are dropped.
        include_empty = stream_id is not None
        for i, sid in enumerate(stream_ids):
            count, lo, hi = results[3 * i], results[3 * i + 1], results[3 * i + 2]
            if count or include_empty:
                min_id = int(lo[0][1]) if lo else None
                max_id = int(hi[0][1]) if hi else None
                stats.append(StreamStat(sid, count, min_id, max_id))

    raw_counter = await redis.get(store._counter_key())  # type: ignore[attr-defined]
    last_event_id = int(raw_counter) if raw_counter is not None else None
    return stats, last_event_id


async def _sql_stats(store: object, stream_id: str | None, *, backend: str) -> tuple[list[StreamStat], int | None]:
    """Count events per stream with a grouped aggregate over the events table.

    One ``GROUP BY stream_id`` (or a single filtered row for ``--stream-id``) plus
    a ``MAX(event_id)`` for the latest assigned ID. ``last_event_id`` is the
    highest ID still stored, which can trail the sequence after rows are purged.
    """
    table = store._table  # type: ignore[attr-defined]
    if backend == "postgres":
        pool = store._pool  # type: ignore[attr-defined]
        if stream_id is not None:
            row = await pool.fetchrow(
                f"SELECT COUNT(*) AS c, MIN(event_id) AS lo, MAX(event_id) AS hi FROM {table} WHERE stream_id = $1",
                stream_id,
            )
            rows = [(stream_id, row["c"], row["lo"], row["hi"])]
        else:
            records = await pool.fetch(
                f"SELECT stream_id, COUNT(*) AS c, MIN(event_id) AS lo, MAX(event_id) AS hi "
                f"FROM {table} GROUP BY stream_id"
            )
            rows = [(r["stream_id"], r["c"], r["lo"], r["hi"]) for r in records]
        last_event_id = await pool.fetchval(f"SELECT MAX(event_id) FROM {table}")
    else:  # sqlite
        conn = store._conn  # type: ignore[attr-defined]
        if stream_id is not None:
            async with conn.execute(
                f"SELECT COUNT(*), MIN(event_id), MAX(event_id) FROM {table} WHERE stream_id = ?",
                (stream_id,),
            ) as cur:
                count, lo, hi = await cur.fetchone()
            rows = [(stream_id, count, lo, hi)]
        else:
            async with conn.execute(
                f"SELECT stream_id, COUNT(*), MIN(event_id), MAX(event_id) FROM {table} GROUP BY stream_id"
            ) as cur:
                rows = [(r[0], r[1], r[2], r[3]) for r in await cur.fetchall()]
        async with conn.execute(f"SELECT MAX(event_id) FROM {table}") as cur:
            (last_event_id,) = await cur.fetchone()

    include_empty = stream_id is not None
    stats = [StreamStat(sid, count, lo, hi) for (sid, count, lo, hi) in rows if count or include_empty]
    return stats, last_event_id


async def gather_stats(cfg: StoreConfig, store: object, *, stream_id: str | None = None) -> StatsReport:
    """Build a :class:`StatsReport` from an open store, timing a ping round trip.

    The latency is measured against the store's own ``ping()`` (the backend's
    native ``PING`` / ``SELECT 1``), so it reflects the same round trip the store
    pays on every operation.
    """
    start = time.perf_counter()
    await store.ping()  # type: ignore[attr-defined]
    latency_ms = (time.perf_counter() - start) * 1000.0

    if cfg.backend == "redis":
        stats, last_event_id = await _redis_stats(store, stream_id)
    else:
        stats, last_event_id = await _sql_stats(store, stream_id, backend=cfg.backend)

    stats.sort(key=lambda s: s.stream_id)
    total_events = sum(s.events for s in stats)
    total_streams = sum(1 for s in stats if s.events)
    return StatsReport(cfg.backend, stats, total_events, total_streams, last_event_id, latency_ms)


def _fmt_id(value: int | None) -> str:
    return "-" if value is None else str(value)


def _render_stats(cfg: StoreConfig, report: StatsReport) -> str:
    lines = [f"mcp-persist stats: {cfg.backend} ({cfg.url})", ""]
    if report.streams:
        headers = ("stream", "events", "min", "max")
        rows = [(s.stream_id, str(s.events), _fmt_id(s.min_event_id), _fmt_id(s.max_event_id)) for s in report.streams]
        widths = [max(len(headers[i]), *(len(r[i]) for r in rows)) for i in range(4)]
        # stream left-aligned; the numeric columns right-aligned.
        aligns = ("l", "r", "r", "r")

        def _row(cells: tuple[str, ...]) -> str:
            return "  ".join(c.ljust(w) if a == "l" else c.rjust(w) for c, w, a in zip(cells, widths, aligns))

        lines.append(_row(headers))
        lines += [_row(r) for r in rows]
    else:
        lines.append("no streams stored")

    lines.append("")
    lines.append(
        f"{report.total_streams} stream(s), {report.total_events} event(s), "
        f"last id {_fmt_id(report.last_event_id)}, ping {report.latency_ms:.2f} ms"
    )
    return "\n".join(lines)


def _render_stats_json(cfg: StoreConfig, report: StatsReport) -> str:
    return json.dumps(
        {
            "backend": cfg.backend,
            "url": cfg.url,
            "total_streams": report.total_streams,
            "total_events": report.total_events,
            "last_event_id": report.last_event_id,
            "latency_ms": round(report.latency_ms, 3),
            "streams": [
                {
                    "stream_id": s.stream_id,
                    "events": s.events,
                    "min_event_id": s.min_event_id,
                    "max_event_id": s.max_event_id,
                }
                for s in report.streams
            ],
        }
    )


# CLI


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mcp-persist",
        description="Inspect and diagnose an mcp-persist event store.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def _store_flags(p: argparse.ArgumentParser) -> None:
        p.add_argument("--backend", choices=("sqlite", "redis", "postgres"), help="event store backend")
        p.add_argument("--url", help="store path / URL / DSN (defaults to MCP_PERSIST_URL)")
        p.add_argument("--ttl", type=int, help="event ttl in seconds (defaults to MCP_PERSIST_TTL)")
        p.add_argument("--table", help="table name for sqlite/postgres (defaults to MCP_PERSIST_TABLE_NAME)")
        p.add_argument("--json", action="store_true", help="emit machine-readable JSON")

    doctor = sub.add_parser("doctor", help="run a pass/fail diagnostic on the configured store")
    _store_flags(doctor)

    stats = sub.add_parser("stats", help="show event counts per stream and a backend latency probe")
    _store_flags(stats)
    stats.add_argument("--stream-id", help="restrict the report to a single stream")

    return parser.parse_args(argv)


def _run_doctor(args: argparse.Namespace) -> int:
    try:
        cfg = _resolve_config(args)
    except ValueError as exc:
        _die(str(exc))
    checks = asyncio.run(diagnose(cfg))
    print(_render_json(cfg, checks) if args.json else _render(cfg, checks))
    return 1 if any(c.status == "fail" for c in checks) else 0


async def _collect_stats(cfg: StoreConfig, stream_id: str | None) -> StatsReport:
    with _quiet_package_log():
        async with _build_store(cfg) as store:
            return await gather_stats(cfg, store, stream_id=stream_id)


def _run_stats(args: argparse.Namespace) -> int:
    try:
        cfg = _resolve_config(args)
    except ValueError as exc:
        _die(str(exc))
    try:
        report = asyncio.run(_collect_stats(cfg, args.stream_id))
    except Exception as exc:
        # A CLI prints a clean line rather than a traceback when the store can't
        # be read (connection refused, missing table, bad DSN).
        print(f"mcp-persist: error: cannot read stats from {cfg.backend} at {cfg.url}: {exc}", file=sys.stderr)
        return 1
    print(_render_stats_json(cfg, report) if args.json else _render_stats(cfg, report))
    return 0


def main() -> None:
    args = _parse_args(sys.argv[1:])
    if args.command == "doctor":
        raise SystemExit(_run_doctor(args))
    if args.command == "stats":
        raise SystemExit(_run_stats(args))
    _die(f"unknown command {args.command!r}")  # pragma: no cover - argparse rejects first


def _die(message: str) -> NoReturn:
    print(f"mcp-persist: error: {message}", file=sys.stderr)
    raise SystemExit(2)


if __name__ == "__main__":  # pragma: no cover
    main()
