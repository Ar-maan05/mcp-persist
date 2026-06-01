"""Optional observability hooks for mcp-persist event stores.

Every store accepts an optional ``metrics=`` collector. When none is supplied a
:class:`NoOpMetricsCollector` is used, and the store takes a fast path that skips
all timing and dispatch — so the default configuration carries no measurable
overhead. Supply your own :class:`MetricsCollector` to emit timing and count data
to Prometheus, Datadog, logs, or anything else::

    from mcp_persist import RedisEventStore, MetricsCollector

    class MyMetrics:
        def on_store_event(self, stream_id, event_id, duration_ms):
            STORE_LATENCY.observe(duration_ms)

        def on_replay(self, stream_id, events_replayed, duration_ms):
            REPLAY_EVENTS.inc(events_replayed)

        def on_error(self, operation, error):
            ERRORS.labels(operation).inc()

    store = RedisEventStore(client, ttl=3600, metrics=MyMetrics())

A collector only needs the three methods structurally; it does not have to
subclass :class:`MetricsCollector`. :class:`LoggingMetricsCollector` is provided
as a batteries-included collector that logs one line per operation at ``DEBUG``.

Hook calls are isolated: a collector that raises is logged and ignored rather
than allowed to turn a successful store or replay into a failure.
"""

from __future__ import annotations

import inspect
import logging
from typing import Protocol

from mcp.server.streamable_http import EventId, StreamId

logger = logging.getLogger(__name__)


class MetricsCollector(Protocol):
    """Structural interface for observability hooks on an event store.

    Implement any object with these three methods and pass it as ``metrics=``.
    All methods are synchronous and must not block — offload expensive work
    (network I/O to a metrics backend, etc.) to your client library's own
    buffering. Methods should not raise; if one does, the store logs and ignores
    it so the underlying operation is unaffected.
    """

    def on_store_event(self, stream_id: StreamId, event_id: EventId, duration_ms: float) -> None:
        """Called after an event is durably stored. ``duration_ms`` times the write."""
        ...

    def on_replay(self, stream_id: StreamId | None, events_replayed: int, duration_ms: float) -> None:
        """Called after a replay completes.

        ``events_replayed`` counts events handed to the send callback (priming,
        expired, and corrupt events that were skipped are not counted).
        ``stream_id`` is ``None`` when the anchor event could not be found.
        """
        ...

    def on_error(self, operation: str, error: Exception) -> None:
        """Called when ``operation`` ("store_event" or "replay_events_after") raises.

        The original exception is re-raised by the store after this returns.
        """
        ...


class NoOpMetricsCollector:
    """Collector that records nothing.

    Used as the default when no ``metrics=`` is supplied. The stores special-case
    this exact type to skip timing and dispatch entirely, so it is genuinely
    zero-cost rather than merely cheap.
    """

    def on_store_event(self, stream_id: StreamId, event_id: EventId, duration_ms: float) -> None:
        return None

    def on_replay(self, stream_id: StreamId | None, events_replayed: int, duration_ms: float) -> None:
        return None

    def on_error(self, operation: str, error: Exception) -> None:
        return None


class LoggingMetricsCollector:
    """Collector that logs one line per operation at ``DEBUG``.

    Covers the common case of "I just want to see store/replay timings in my
    logs" without pulling in Prometheus or another metrics backend. Pass a custom
    ``logger`` to route the lines wherever you like; the default is the
    ``mcp_persist.metrics`` logger.
    """

    def __init__(self, log: logging.Logger | None = None) -> None:
        self._log = log if log is not None else logging.getLogger("mcp_persist.metrics")

    def on_store_event(self, stream_id: StreamId, event_id: EventId, duration_ms: float) -> None:
        self._log.debug("store_event stream=%s event=%s in %.2fms", stream_id, event_id, duration_ms)

    def on_replay(self, stream_id: StreamId | None, events_replayed: int, duration_ms: float) -> None:
        self._log.debug("replay stream=%s events=%d in %.2fms", stream_id, events_replayed, duration_ms)

    def on_error(self, operation: str, error: Exception) -> None:
        self._log.debug("error in %s: %r", operation, error)


def safe_call(hook: object, *args: object) -> None:
    """Invoke a metrics hook, logging and swallowing any exception it raises.

    A user-supplied collector must never be able to turn a successful store or
    replay into a failure, so every hook dispatch goes through here.
    """
    try:
        result = hook(*args)  # type: ignore[operator]
    except Exception:  # noqa: BLE001 - a misbehaving collector must not break the store
        logger.exception("metrics collector hook raised; ignoring")
        return

    if inspect.iscoroutine(result):
        # Collector methods are synchronous by contract (see MetricsCollector).
        # An `async def` hook returns an un-awaited coroutine here; close it to
        # avoid a "coroutine was never awaited" warning and surface the misuse
        # rather than silently dropping the metric.
        result.close()
        logger.warning(
            "metrics collector hook %r returned a coroutine; collector methods must be synchronous. "
            "The metric was not recorded.",
            getattr(hook, "__name__", hook),
        )
