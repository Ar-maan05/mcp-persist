"""OpenTelemetry metrics collector for mcp-persist event stores."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mcp.server.streamable_http import EventId, StreamId

if TYPE_CHECKING:
    pass


class OTelMetricsCollector:
    """MetricsCollector that records store/replay timings via OpenTelemetry instruments.

    Requires the ``otel`` extra (``pip install "mcp-persist[otel]"``). Pass a
    :class:`~opentelemetry.metrics.Meter` from your SDK setup; this collector
    only uses the API instruments (non-blocking, in-process recording).

    Labels: ``backend`` (required at construction), optional ``tenant_id`` when
    multi-tenancy is enabled.

    Example::

        from opentelemetry import metrics
        from mcp_persist.otel import OTelMetricsCollector

        meter = metrics.get_meter("mcp-persist")
        store = RedisEventStore(client, ttl=3600, metrics=OTelMetricsCollector(meter, backend="redis"))
    """

    def __init__(
        self,
        meter: Any,
        *,
        backend: str,
        tenant_id: str | None = None,
    ) -> None:
        attrs: dict[str, str] = {"backend": backend}
        if tenant_id is not None:
            attrs["tenant_id"] = tenant_id
        self._attrs = attrs

        self._store_duration = meter.create_histogram(
            "mcp_persist.store.duration_ms",
            unit="ms",
            description="Time to store one event",
        )
        self._replay_duration = meter.create_histogram(
            "mcp_persist.replay.duration_ms",
            unit="ms",
            description="Time to replay events after Last-Event-ID",
        )
        self._errors = meter.create_counter(
            "mcp_persist.errors",
            description="Store/replay errors",
        )
        self._proxy_replay = meter.create_counter(
            "mcp_persist.proxy.replay",
            description="Proxy replay deliveries (labeled blocked=true when refused)",
        )

    def on_store_event(self, stream_id: StreamId, event_id: EventId, duration_ms: float) -> None:
        self._store_duration.record(duration_ms, self._attrs)

    def on_replay(self, stream_id: StreamId | None, events_replayed: int, duration_ms: float) -> None:
        self._replay_duration.record(duration_ms, self._attrs)

    def on_error(self, operation: str, error: Exception) -> None:
        self._errors.add(1, {**self._attrs, "operation": operation})

    def on_proxy_replay(
        self, stream_id: str | None, session_id: str, events_replayed: int, blocked: bool, duration_ms: float
    ) -> None:
        self._proxy_replay.add(
            1,
            {**self._attrs, "blocked": str(blocked).lower()},
        )
