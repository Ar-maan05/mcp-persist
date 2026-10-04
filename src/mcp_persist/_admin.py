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
import os
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractAsyncContextManager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, NoReturn, cast

from mcp_persist.compression import validate_compression
from mcp_persist.config import _PREFIX, _optional_int, build_store_context
from mcp_persist.encryption import keyring_from_env
from mcp_persist.migration import MigrationResult, migrate
from mcp_persist.portability import export_stream, import_stream
from mcp_persist.stored import count_expired

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mcp.server.streamable_http import EventStore


def _package_version() -> str:
    """Return the installed ``mcp-persist`` version, or a sentinel from a source tree."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("mcp-persist")
    except PackageNotFoundError:  # pragma: no cover - running uninstalled
        return "0.0.0+unknown"


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
    tenant_id: str | None = None
    compression: str | None = None
    keyring: Any | None = None


# Config resolution


def _resolve_config(args: argparse.Namespace) -> StoreConfig:
    """Build a :class:`StoreConfig` from CLI flags, falling back to ``MCP_PERSIST_*``.

    Non-secret CLI flags win when given; everything else comes from the
    environment, including encryption keys so secrets never need to appear in a
    process list. This keeps every command pointed at the same effective store as
    the application. Raises ``ValueError`` with an actionable message for invalid
    configuration.
    """
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
    key_prefix = args.key_prefix or env.get(f"{_PREFIX}KEY_PREFIX")
    max_stream_length = (
        args.max_stream_length
        if args.max_stream_length is not None
        else _optional_int(env, f"{_PREFIX}MAX_STREAM_LENGTH")
    )
    tenant_id = args.tenant_id or env.get(f"{_PREFIX}TENANT_ID")
    compression = args.compression or env.get(f"{_PREFIX}COMPRESSION")

    return StoreConfig(
        backend=backend,
        url=url,
        ttl=ttl,
        table_name=table_name,
        key_prefix=key_prefix or None,
        max_stream_length=max_stream_length,
        tenant_id=tenant_id or None,
        compression=compression or None,
        keyring=keyring_from_env(env),
    )


def _resolve_migrate_side(args: argparse.Namespace, side: str) -> StoreConfig:
    """Build the :class:`StoreConfig` for one side of ``migrate`` (``from`` or ``to``).

    ``migrate`` names two stores, so it takes a ``--from-``/``--to-`` prefixed copy
    of every store setting instead of the single set the other commands share.
    Each setting falls back to its ``MCP_PERSIST_*`` variable when the flag is
    absent, which is what makes the common case (migrating the deployment this
    shell is already configured for onto a new backend) correct by default;
    migrating between two differently configured deployments spells the
    difference out with the per-side flags.

    The encryption keyring comes from the environment for both sides, never from
    a flag, so keys stay out of the process list. One keyring covers a rekeying
    migration too: ``MCP_PERSIST_ENCRYPTION_KEYS`` can hold the source's old key
    alongside the destination's new one, with ``MCP_PERSIST_ENCRYPTION_KEY_ID``
    selecting which is written.
    """
    env = os.environ

    def flag(name: str) -> Any:
        return getattr(args, f"{side}_{name}")

    def env_str(suffix: str) -> str | None:
        return env.get(f"{_PREFIX}{suffix}") or None

    def env_int(suffix: str) -> int | None:
        return _optional_int(env, f"{_PREFIX}{suffix}")

    backend = flag("backend")
    compression = flag("compression") or env_str("COMPRESSION")
    if compression is not None:
        # Fail before opening either store rather than at the first write.
        validate_compression(compression)

    ttl = flag("ttl")
    max_stream_length = flag("max_stream_length")
    return StoreConfig(
        backend=backend,
        url=flag("url"),
        ttl=ttl if ttl is not None else env_int("TTL"),
        table_name=flag("table") or env_str("TABLE_NAME"),
        key_prefix=flag("key_prefix") or env_str("KEY_PREFIX"),
        max_stream_length=max_stream_length if max_stream_length is not None else env_int("MAX_STREAM_LENGTH"),
        tenant_id=flag("tenant_id") or env_str("TENANT_ID"),
        compression=compression,
        keyring=keyring_from_env(env),
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

    Uses the shared construction path, so commands honor the same tenancy,
    compression, and encryption settings as an environment-configured app. The
    connection is established on ``__aenter__`` and closed on ``__aexit__``.
    """
    return build_store_context(
        cfg.backend,
        cfg.url,
        ttl=cfg.ttl,
        table_name=cfg.table_name,
        key_prefix=cfg.key_prefix,
        max_stream_length=cfg.max_stream_length,
        tenant_id=cfg.tenant_id,
        compression=cfg.compression,
        keyring=cfg.keyring,
    )


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
        return Check("connectivity", "fail", f"cannot reach {cfg.backend} at {redact_url(cfg.url)}: {exc}")
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


def _check_compression(cfg: StoreConfig) -> Check:
    """Verify a configured ``MCP_PERSIST_COMPRESSION`` codec is usable.

    Catches the two ways compression fails only once events start flowing: an
    unknown codec, or ``zstd`` configured without the ``zstd`` extra installed.
    Both raise from :func:`validate_compression` (the same guard the stores run
    at construction), surfaced here as a ``fail`` with the pip hint so an operator
    sees it before the first write instead of at it.
    """
    if cfg.compression is None:
        return Check("compression", "pass", "compression is disabled")
    try:
        validate_compression(cfg.compression)
    except ValueError as exc:
        return Check("compression", "fail", str(exc))
    return Check("compression", "pass", f"compression={cfg.compression}: payloads compressed before write")


def _check_encryption(env: Mapping[str, str] | None = None) -> Check:
    """Verify ``MCP_PERSIST_ENCRYPTION_*`` config parses and its driver is present.

    Two failure modes that otherwise only surface at the first encrypted write:
    a malformed key set (:func:`keyring_from_env` raises ``ValueError`` for a bad
    base64 key, a wrong length, or an ambiguous active id), and a keyring that is
    configured while the ``crypto`` extra is not installed (the keyring builds
    without ``cryptography`` because AES-GCM is imported lazily, so the missing
    driver stays silent until a write). Both are reported as a ``fail`` with the
    fix; an unconfigured keyring is a ``pass`` (encryption is opt-in).
    """
    try:
        keyring = keyring_from_env(env)
    except ValueError as exc:
        return Check("encryption", "fail", f"encryption config is invalid: {exc}")
    if keyring is None:
        return Check("encryption", "pass", "encryption is disabled")
    if importlib.util.find_spec("cryptography") is None:
        return Check(
            "encryption",
            "fail",
            "encryption keys are configured but the crypto extra is not installed; "
            "run: pip install 'mcp-persist[crypto]'",
        )
    active_id, _ = keyring.active()
    return Check("encryption", "pass", f"encryption enabled (active key id {active_id!r})")


def _check_protocol_support(env: Mapping[str, str] | None = None) -> Check:
    """Report which protocol revisions this deployment's persistence applies to.

    The single most confusing thing about running this library today: the SDK
    routes any non-handshake protocol version to a stateless single-exchange
    transport that never reaches the event store, so SSE replay and durable
    sessions quietly do not apply to those clients. Nothing is broken, but an
    operator who does not know it will go looking for events that were never
    going to be written. Records cover every revision and close that gap, so the
    check reports whether they are on.
    """
    from mcp_persist.config import env_flag
    from mcp_persist.middleware import protocol_support

    handshake_versions, modern_versions = protocol_support()
    handshake = ", ".join(handshake_versions)
    modern = ", ".join(modern_versions)
    if env_flag("MCP_PERSIST_RECORD", env):
        return Check(
            "protocol support",
            "pass",
            f"events and durable sessions apply to {handshake}; {modern} is stateless and bypasses "
            "the event store, but recording is enabled so those requests are still captured",
        )
    return Check(
        "protocol support",
        "warn",
        f"events and durable sessions apply to {handshake} only; {modern} is a stateless "
        "single-exchange transport that never reaches the event store, so nothing is persisted for "
        "clients on it. Enable records (record=True or MCP_PERSIST_RECORD=1) to capture every revision",
    )


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
    checks.append(_check_compression(cfg))
    checks.append(_check_encryption())
    checks.append(_check_protocol_support())
    return checks


# Rendering


_GLYPH = {"pass": "[ ok ]", "warn": "[warn]", "fail": "[fail]"}


def _render(cfg: StoreConfig, checks: list[Check]) -> str:
    width = max(len(c.name) for c in checks)
    lines = [f"mcp-persist doctor: {cfg.backend} ({redact_url(cfg.url)})", ""]
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
            "url": redact_url(cfg.url),
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
    lines = [f"mcp-persist stats: {cfg.backend} ({redact_url(cfg.url)})", ""]
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
            "url": redact_url(cfg.url),
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


# Config


def redact_url(url: str) -> str:
    """Mask the password in a ``scheme://user:password@host`` URL.

    ``config`` output is the kind of thing that gets pasted into a bug report, so
    a Redis or Postgres DSN carrying inline credentials is printed with the
    password replaced by ``***``. Anything without that shape (an SQLite path, a
    DSN with no password) is returned unchanged.
    """
    scheme, sep, rest = url.partition("://")
    if not sep:
        return url
    rest, qmark, query = rest.partition("?")
    if query:
        # libpq-style URIs also accept the password as a query parameter.
        query = "&".join(
            f"{name}=***" if name.lower() == "password" else part
            for part in query.split("&")
            for name in [part.partition("=")[0]]
        )
    if "@" in rest:
        userinfo, _, hostpart = rest.rpartition("@")
        user, colon, _password = userinfo.partition(":")
        if colon:
            rest = f"{user}:***@{hostpart}"
    return f"{scheme}://{rest}{qmark}{query}"


def _config_fields(cfg: StoreConfig) -> list[tuple[str, str]]:
    """The resolved settings as ordered ``(name, display value)`` pairs."""
    keyring = cfg.keyring
    if keyring is None:
        encryption = "off"
    else:
        # Report the shape of the key set, never a key.
        encryption = f"on (active key {keyring.active_key_id!r}, {len(keyring.key_ids)} key(s) available)"
    return [
        ("backend", cfg.backend),
        ("url", redact_url(cfg.url)),
        ("ttl", "unset (events never expire)" if cfg.ttl is None else f"{cfg.ttl}s"),
        ("table_name", cfg.table_name or "default"),
        ("key_prefix", cfg.key_prefix or "default"),
        ("max_stream_length", "unset" if cfg.max_stream_length is None else str(cfg.max_stream_length)),
        ("tenant_id", cfg.tenant_id or "unbound (sees every tenant)"),
        ("compression", cfg.compression or "off"),
        ("encryption", encryption),
    ]


def _render_config(cfg: StoreConfig) -> str:
    fields = _config_fields(cfg)
    width = max(len(name) for name, _ in fields)
    lines = ["mcp-persist config: resolved from MCP_PERSIST_* and command-line flags", ""]
    lines += [f"  {name.ljust(width)}  {value}" for name, value in fields]
    return "\n".join(lines)


def _render_config_json(cfg: StoreConfig) -> str:
    keyring = cfg.keyring
    return json.dumps(
        {
            "backend": cfg.backend,
            "url": redact_url(cfg.url),
            "ttl": cfg.ttl,
            "table_name": cfg.table_name,
            "key_prefix": cfg.key_prefix,
            "max_stream_length": cfg.max_stream_length,
            "tenant_id": cfg.tenant_id,
            "compression": cfg.compression,
            "encryption": {
                "enabled": keyring is not None,
                "active_key_id": None if keyring is None else keyring.active_key_id,
                "key_ids": [] if keyring is None else list(keyring.key_ids),
            },
        }
    )


# Duration parsing (for `purge --older-than`)

_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_duration(text: str) -> float:
    """Parse a duration like ``30d`` / ``12h`` / ``45m`` / ``3600s`` into seconds.

    A bare number is read as seconds, so ``--older-than 3600`` and
    ``--older-than 1h`` are equivalent. Accepts ``s`` (seconds), ``m`` (minutes),
    ``h`` (hours), ``d`` (days), and ``w`` (weeks). Raises ``ValueError`` with an
    actionable message on anything else.
    """
    raw = text.strip().lower()
    if not raw:
        raise ValueError("empty duration")
    unit = raw[-1]
    if unit.isdigit():
        value, multiplier = raw, 1
    elif unit in _DURATION_UNITS:
        value, multiplier = raw[:-1], _DURATION_UNITS[unit]
    else:
        raise ValueError(f"unknown duration unit {unit!r} in {text!r}; use s, m, h, d, or w")
    try:
        number = float(value)
    except ValueError:
        raise ValueError(f"invalid duration {text!r}: expected a number optionally suffixed with s/m/h/d/w") from None
    if number < 0:
        raise ValueError(f"duration must not be negative: {text!r}")
    return number * multiplier


# CLI


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mcp-persist",
        description="Inspect and diagnose an mcp-persist event store.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"mcp-persist {_package_version()}",
        help="show the installed mcp-persist version and exit",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def _store_flags(p: argparse.ArgumentParser) -> None:
        p.add_argument("--backend", choices=("sqlite", "redis", "postgres"), help="event store backend")
        p.add_argument("--url", help="store path / URL / DSN (defaults to MCP_PERSIST_URL)")
        p.add_argument("--ttl", type=int, help="event ttl in seconds (defaults to MCP_PERSIST_TTL)")
        p.add_argument("--table", help="table name for sqlite/postgres (defaults to MCP_PERSIST_TABLE_NAME)")
        p.add_argument("--key-prefix", help="Redis key prefix (defaults to MCP_PERSIST_KEY_PREFIX)")
        p.add_argument(
            "--max-stream-length", type=int, help="Redis stream cap (defaults to MCP_PERSIST_MAX_STREAM_LENGTH)"
        )
        p.add_argument("--tenant-id", help="tenant namespace (defaults to MCP_PERSIST_TENANT_ID)")
        p.add_argument(
            "--compression", choices=("gzip", "zstd"), help="payload codec (defaults to MCP_PERSIST_COMPRESSION)"
        )
        p.add_argument("--json", action="store_true", help="emit machine-readable JSON")

    config_p = sub.add_parser("config", help="print the resolved store configuration (secrets redacted)")
    _store_flags(config_p)

    doctor = sub.add_parser("doctor", help="run a pass/fail diagnostic on the configured store")
    _store_flags(doctor)

    stats = sub.add_parser("stats", help="show event counts per stream and a backend latency probe")
    _store_flags(stats)
    stats.add_argument("--stream-id", help="restrict the report to a single stream")

    purge = sub.add_parser("purge", help="delete expired events from the configured store")
    _store_flags(purge)
    purge.add_argument("--batch-size", type=int, help="delete in chunks of N rows")
    purge.add_argument("--dry-run", action="store_true", help="count expired rows without deleting")
    purge.add_argument(
        "--older-than",
        metavar="DURATION",
        help="purge by age instead of ttl, e.g. 30d / 12h / 3600s (sqlite/postgres only)",
    )

    dump = sub.add_parser("dump", help="export one stream's events to portable JSON")
    _store_flags(dump)
    dump.add_argument("stream_id", help="the stream to export")
    dump.add_argument("-o", "--output", help="write to this file instead of stdout")

    load = sub.add_parser("load", help="import a stream dumped by `dump` into the configured store")
    _store_flags(load)
    load.add_argument("path", nargs="?", help="dump file to read (defaults to stdin)")
    load.add_argument("--stream-id", help="restore into this stream instead of the one in the dump")

    sessions = sub.add_parser("sessions", help="inspect and end durable sessions (durable_sessions=True)")
    _store_flags(sessions)
    sessions.add_argument(
        "action",
        choices=("list", "show", "terminate", "purge"),
        help="list live sessions, show one, end one, or delete stale records",
    )
    sessions.add_argument("session_id", nargs="?", help="required by show and terminate")
    sessions.add_argument("--limit", type=int, default=50, help="maximum sessions to list (default 50)")
    sessions.add_argument(
        "--all", action="store_true", dest="include_terminated", help="include terminated sessions in list"
    )
    sessions.add_argument(
        "--older-than",
        metavar="DURATION",
        help="for purge: delete sessions not seen for this long, e.g. 30d / 12h / 3600s",
    )

    dash = sub.add_parser("dashboard", help="serve a local read-only web view of the store")
    _store_flags(dash)
    dash.add_argument("--port", type=int, default=8765, help="port to listen on (default 8765)")
    dash.add_argument(
        "--host",
        default="127.0.0.1",
        help="address to bind (default 127.0.0.1; the dashboard has no authentication)",
    )
    dash.add_argument(
        "--unsafe-bind",
        action="store_true",
        help="allow binding a non-loopback address, exposing store contents to the network",
    )
    dash.add_argument(
        "--allowed-host",
        action="append",
        default=[],
        metavar="HOST",
        help="a host name the dashboard may be reached by, besides localhost (repeatable); "
        "requests naming any other Host are refused, which is what stops DNS rebinding",
    )
    dash.add_argument(
        "--redact-payloads",
        action="store_true",
        help="hide message bodies, showing only counts, ids and method names",
    )

    migrate_p = sub.add_parser("migrate", help="copy events from one store to another")
    for side, label in (("from", "source"), ("to", "destination")):
        migrate_p.add_argument(f"--{side}-backend", choices=("sqlite", "redis", "postgres"), required=True)
        migrate_p.add_argument(f"--{side}-url", required=True)
        migrate_p.add_argument(f"--{side}-ttl", type=int, help=f"event ttl in seconds for the {label} store")
        migrate_p.add_argument(f"--{side}-table", help=f"table name for the {label} sqlite/postgres store")
        migrate_p.add_argument(f"--{side}-key-prefix", help=f"Redis key prefix for the {label} store")
        migrate_p.add_argument(f"--{side}-max-stream-length", type=int, help=f"Redis stream cap for the {label} store")
        migrate_p.add_argument(f"--{side}-tenant-id", help=f"tenant namespace to bind the {label} store to")
        migrate_p.add_argument(
            f"--{side}-compression", choices=("gzip", "zstd"), help=f"payload codec for the {label} store"
        )
    migrate_p.add_argument("--batch-size", type=int, default=500)
    migrate_p.add_argument("--json", action="store_true")

    return parser.parse_args(argv)


def _run_config(args: argparse.Namespace) -> int:
    try:
        cfg = _resolve_config(args)
    except ValueError as exc:
        _die(str(exc))
    print(_render_config_json(cfg) if args.json else _render_config(cfg))
    return 0


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
        print(
            f"mcp-persist: error: cannot read stats from {cfg.backend} at {redact_url(cfg.url)}: {exc}", file=sys.stderr
        )
        return 1
    print(_render_stats_json(cfg, report) if args.json else _render_stats(cfg, report))
    return 0


async def _purge_store(cfg: StoreConfig, *, batch_size: int | None, dry_run: bool, older_than: float | None) -> int:
    with _quiet_package_log():
        async with _build_store(cfg) as store:
            if dry_run:
                if older_than is not None:
                    return await store.count_expired(older_than=older_than)  # type: ignore[attr-defined]
                return await count_expired(store)
            kwargs: dict[str, Any] = {}
            if batch_size is not None:
                kwargs["batch_size"] = batch_size
            if older_than is not None:
                kwargs["older_than"] = older_than
            return await store.purge_expired(**kwargs)  # type: ignore[attr-defined]


def _run_purge(args: argparse.Namespace) -> int:
    try:
        cfg = _resolve_config(args)
        older_than = parse_duration(args.older_than) if args.older_than else None
    except ValueError as exc:
        _die(str(exc))
    if older_than is not None and cfg.backend == "redis":
        _die("--older-than is not supported for redis (keys expire natively via ttl)")
    try:
        removed = asyncio.run(
            _purge_store(cfg, batch_size=args.batch_size, dry_run=args.dry_run, older_than=older_than)
        )
    except Exception as exc:
        print(f"mcp-persist: error: purge failed for {cfg.backend} at {redact_url(cfg.url)}: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps({"purged": removed, "dry_run": args.dry_run}))
    elif args.dry_run:
        print(f"would purge {removed} expired event(s)")
    else:
        print(f"purged {removed} expired event(s)")
    return 0


async def _dump_stream(cfg: StoreConfig, stream_id: str) -> dict[str, Any]:
    with _quiet_package_log():
        async with _build_store(cfg) as store:
            return await export_stream(cast(Any, store), stream_id, backend=cfg.backend)


def _run_dump(args: argparse.Namespace) -> int:
    try:
        cfg = _resolve_config(args)
    except ValueError as exc:
        _die(str(exc))
    try:
        document = asyncio.run(_dump_stream(cfg, args.stream_id))
    except Exception as exc:
        print(f"mcp-persist: error: dump failed for {cfg.backend} at {redact_url(cfg.url)}: {exc}", file=sys.stderr)
        return 1
    text = json.dumps(document, indent=None if args.json else 2)
    if args.output:
        try:
            # A dump is decrypted plaintext, so a new file is created readable by
            # its owner only rather than with the process umask.
            fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with open(fd, "w", encoding="utf-8") as handle:
                handle.write(text + "\n")
        except OSError as exc:
            print(f"mcp-persist: error: cannot write {args.output}: {exc}", file=sys.stderr)
            return 1
        print(f"wrote {len(document['events'])} event(s) to {args.output}", file=sys.stderr)
    else:
        print(text)
    return 0


async def _load_stream(cfg: StoreConfig, document: dict[str, Any], stream_id: str | None) -> int:
    with _quiet_package_log():
        async with _build_store(cfg) as store:
            return await import_stream(cast(Any, store), document, stream_id=stream_id)


def _run_load(args: argparse.Namespace) -> int:
    try:
        cfg = _resolve_config(args)
    except ValueError as exc:
        _die(str(exc))
    try:
        raw = _read_text(args.path)
        document = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"mcp-persist: error: cannot read dump: {exc}", file=sys.stderr)
        return 1
    try:
        written = asyncio.run(_load_stream(cfg, document, args.stream_id))
    except ValueError as exc:
        _die(str(exc))
    except Exception as exc:
        print(f"mcp-persist: error: load failed for {cfg.backend} at {redact_url(cfg.url)}: {exc}", file=sys.stderr)
        return 1
    target = args.stream_id or document.get("stream_id")
    if args.json:
        print(json.dumps({"loaded": written, "stream_id": target}))
    else:
        print(f"loaded {written} event(s) into stream {target}")
    return 0


def _read_text(path: str | None) -> str:
    """Read a dump from ``path``, or from stdin when ``path`` is ``None``/``-``."""
    if path is None or path == "-":
        return sys.stdin.read()
    with open(path, encoding="utf-8") as handle:
        return handle.read()


async def _migrate_stores(
    source_cfg: StoreConfig,
    dest_cfg: StoreConfig,
    *,
    batch_size: int,
    on_progress: Callable[[str, int], None] | None,
) -> MigrationResult:
    with _quiet_package_log():
        async with _build_store(source_cfg) as source, _build_store(dest_cfg) as dest:
            # migrate() narrows to its _MigrationSource/_MigrationDest protocols;
            # the concrete backends satisfy them structurally (list_streams /
            # _iter_stream_events / store_event), which EventStore doesn't declare.
            return await migrate(cast(Any, source), cast(Any, dest), batch_size=batch_size, on_progress=on_progress)


def _run_migrate(args: argparse.Namespace) -> int:
    try:
        source = _resolve_migrate_side(args, "from")
        dest = _resolve_migrate_side(args, "to")
    except ValueError as exc:
        _die(str(exc))

    def progress(sid: str, n: int) -> None:
        if not args.json:
            print(f"{sid}: {n} event(s)", flush=True)

    try:
        result = asyncio.run(
            _migrate_stores(source, dest, batch_size=args.batch_size, on_progress=progress if not args.json else None)
        )
    except Exception as exc:
        print(f"mcp-persist: error: migrate failed: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(
            json.dumps(
                {
                    "streams_migrated": result.streams_migrated,
                    "events_migrated": result.events_migrated,
                    "failed_streams": result.failed_streams,
                    "skipped_events": result.skipped_events,
                }
            )
        )
    else:
        print(
            f"migrated {result.events_migrated} event(s) across {result.streams_migrated} stream(s); "
            f"failed: {len(result.failed_streams)}"
        )
    if result.skipped_events:
        # Undecodable events are dropped silently by the read path, so a migration
        # that lost data would otherwise exit 0 looking like a clean run.
        print(
            f"mcp-persist: error: skipped {result.skipped_events} event(s) the source could not decode; "
            f"they were NOT copied. If the source store is encrypted, set MCP_PERSIST_ENCRYPTION_KEY(S) "
            f"to its key and run again.",
            file=sys.stderr,
        )
    return 1 if result.failed_streams or result.skipped_events else 0


async def _session_action(cfg: StoreConfig, args: argparse.Namespace) -> Any:
    from mcp_persist.sessions import session_registry_for

    with _quiet_package_log():
        async with _build_store(cfg) as store:
            registry = session_registry_for(store)
            await registry.initialize()
            if args.action == "list":
                return await registry.list_sessions(include_terminated=args.include_terminated, limit=args.limit)
            if args.action == "show":
                return await registry.get(args.session_id)
            if args.action == "terminate":
                # Report whether there was anything to end, so a typo in the id
                # is not indistinguishable from a session that really was ended.
                existing = await registry.get(args.session_id)
                if existing is None:
                    return None
                await registry.terminate(args.session_id)
                return await registry.get(args.session_id)
            return await registry.purge(older_than=parse_duration(args.older_than))


def _run_sessions(args: argparse.Namespace) -> int:
    if args.action in ("show", "terminate") and not args.session_id:
        _die(f"sessions {args.action} requires a session id")
    if args.action == "purge" and not args.older_than:
        _die("sessions purge requires --older-than, e.g. --older-than 30d")
    if args.action != "purge" and args.older_than:
        _die("--older-than only applies to `sessions purge`")

    try:
        cfg = _resolve_config(args)
        if args.action == "purge":
            parse_duration(args.older_than)
    except ValueError as exc:
        _die(str(exc))

    try:
        result = asyncio.run(_session_action(cfg, args))
    except TypeError as exc:
        # session_registry_for() rejects a backend with no registry.
        _die(str(exc))
    except Exception as exc:
        print(
            f"mcp-persist: error: cannot read sessions from {cfg.backend} at {redact_url(cfg.url)}: {exc}",
            file=sys.stderr,
        )
        return 1

    if args.action == "purge":
        print(json.dumps({"purged": result}) if args.json else f"purged {result} session record(s)")
        return 0

    if args.action == "list":
        records = [r.as_dict() for r in result]
        if args.json:
            print(json.dumps({"sessions": records}, indent=2))
        elif not records:
            print("no sessions recorded (is durable_sessions enabled on the server?)")
        else:
            print(f"{'SESSION ID':<34} {'LAST SEEN (UTC)':<26} {'STATE':<11} CLIENT")
            for record in records:
                seen = datetime.fromtimestamp(record["last_seen_at"], tz=timezone.utc).isoformat(timespec="seconds")
                state = "terminated" if record["terminated"] else "live"
                print(f"{record['session_id']:<34} {seen:<26} {state:<11} {record['client'] or '-'}")
        return 0

    # show / terminate
    if result is None:
        print(f"mcp-persist: error: no such session {args.session_id!r}", file=sys.stderr)
        return 1
    print(json.dumps(result.as_dict(), indent=2))
    return 0


def _is_loopback(host: str) -> bool:
    import ipaddress

    if host in ("localhost", ""):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        # A hostname we cannot classify is treated as exposed, which is the
        # safe direction for a page that has no authentication.
        return False


def _run_dashboard(args: argparse.Namespace) -> int:
    try:
        cfg = _resolve_config(args)
    except ValueError as exc:
        _die(str(exc))

    if not _is_loopback(args.host) and not args.unsafe_bind:
        _die(
            f"refusing to bind {args.host}: the dashboard has no authentication and would expose "
            "the store's contents, including message payloads, to anyone who can reach that "
            "address. Pass --unsafe-bind if that is genuinely what you want, and consider "
            "--redact-payloads."
        )

    try:
        import uvicorn
    except ImportError:  # pragma: no cover - uvicorn ships with mcp today
        _die("the dashboard needs uvicorn: pip install uvicorn")

    from mcp_persist.dashboard import LOOPBACK_HOSTS, create_dashboard

    allowed_hosts = [*LOOPBACK_HOSTS, *args.allowed_host]
    if args.host in ("0.0.0.0", "::"):
        if not args.allowed_host:
            # Reachable at whatever names the machine has, none of which are
            # known here; the operator already accepted exposure with --unsafe-bind.
            allowed_hosts = ["*"]
            print("accepting any Host header; pass --allowed-host to restrict it", file=sys.stderr)
    elif args.host not in allowed_hosts:
        allowed_hosts.append(args.host)
    app = create_dashboard(cfg, redact_payloads=args.redact_payloads, allowed_hosts=allowed_hosts)
    shown = "localhost" if args.host in ("127.0.0.1", "::1", "") else args.host
    print(f"mcp-persist dashboard: http://{shown}:{args.port}  ({cfg.backend} at {redact_url(cfg.url)})")
    if args.redact_payloads:
        print("message payloads are redacted")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


def main() -> None:
    args = _parse_args(sys.argv[1:])
    if args.command == "config":
        raise SystemExit(_run_config(args))
    if args.command == "doctor":
        raise SystemExit(_run_doctor(args))
    if args.command == "stats":
        raise SystemExit(_run_stats(args))
    if args.command == "purge":
        raise SystemExit(_run_purge(args))
    if args.command == "dump":
        raise SystemExit(_run_dump(args))
    if args.command == "load":
        raise SystemExit(_run_load(args))
    if args.command == "migrate":
        raise SystemExit(_run_migrate(args))
    if args.command == "sessions":
        raise SystemExit(_run_sessions(args))
    if args.command == "dashboard":
        raise SystemExit(_run_dashboard(args))
    _die(f"unknown command {args.command!r}")  # pragma: no cover - argparse rejects first


def _die(message: str) -> NoReturn:
    print(f"mcp-persist: error: {message}", file=sys.stderr)
    raise SystemExit(2)


if __name__ == "__main__":  # pragma: no cover
    main()
