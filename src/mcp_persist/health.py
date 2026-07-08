"""Structured store health probes: ``await store.health()``.

A :class:`HealthReport` is a single, uniform snapshot of a store's liveness and
latency, with a backend-specific ``detail`` map for the numbers that only make
sense per backend (a SQLite database file's size on disk, a Postgres pool's
in-use connections, a batching store's pending writes). It is meant to back a
container ``/healthz`` endpoint or an operator's spot check, complementing the
``mcp-persist doctor`` CLI (which is a one-shot pass/fail checklist) and
``MetricsCollector`` (which is a continuous per-operation stream).

The probe reuses the store's own ``ping()`` for the round trip, so ``latency_ms``
reflects the same cost the store pays on every real operation. A store that fails
to respond reports ``healthy=False`` with the error in ``detail['error']``
instead of raising, so a health endpoint can turn it into a 503 rather than a
500.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class HealthReport:
    """A point-in-time liveness snapshot of one store.

    Attributes:
        healthy:    ``True`` when the backend answered its ping.
        backend:    The backend name (``sqlite`` / ``redis`` / ``postgres`` / a
                    wrapper's name).
        latency_ms: Round-trip time of the ping in milliseconds. ``None`` when
                    the ping failed (no round trip completed).
        detail:     Backend-specific fields, e.g. ``disk_bytes`` for SQLite,
                    ``pool_size``/``pool_in_use`` for Postgres,
                    ``pending_writes`` for a batching store, and ``error`` when
                    unhealthy.
    """

    healthy: bool
    backend: str
    latency_ms: float | None = None
    detail: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable view (for a ``/healthz`` body or logs)."""
        return {
            "healthy": self.healthy,
            "backend": self.backend,
            "latency_ms": None if self.latency_ms is None else round(self.latency_ms, 3),
            "detail": self.detail,
        }


async def probe_health(store: Any, backend: str, detail: dict[str, Any] | None = None) -> HealthReport:
    """Ping ``store`` and build a :class:`HealthReport`, never raising on failure.

    Shared by every backend's ``health()`` so the ping/timing/error handling is
    written once. ``detail`` carries the backend-specific fields the caller has
    already gathered; a failed ping still returns a report (with the error
    folded into ``detail``) so callers can branch on ``healthy`` rather than
    catch exceptions.
    """
    merged: dict[str, Any] = dict(detail or {})
    start = time.perf_counter()
    try:
        ok = await store.ping()
    except Exception as exc:  # noqa: BLE001 - a health probe reports failure, it does not raise
        merged["error"] = f"{type(exc).__name__}: {exc}"
        return HealthReport(healthy=False, backend=backend, latency_ms=None, detail=merged)

    latency_ms = (time.perf_counter() - start) * 1000.0
    if not ok:
        merged.setdefault("error", "ping returned falsy")
    return HealthReport(healthy=bool(ok), backend=backend, latency_ms=latency_ms, detail=merged)
