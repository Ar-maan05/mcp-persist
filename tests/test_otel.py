"""Tests for OTelMetricsCollector."""

from __future__ import annotations


class _FakeCounter:
    def __init__(self) -> None:
        self.calls: list[tuple[int, dict[str, str]]] = []

    def add(self, amount: int, attributes: dict[str, str]) -> None:
        self.calls.append((amount, attributes))


class _FakeHistogram:
    def __init__(self) -> None:
        self.records: list[tuple[float, dict[str, str]]] = []

    def record(self, value: float, attributes: dict[str, str]) -> None:
        self.records.append((value, attributes))


class _FakeMeter:
    def __init__(self) -> None:
        self.counters: dict[str, _FakeCounter] = {}
        self.histograms: dict[str, _FakeHistogram] = {}

    def create_counter(self, name: str, **kwargs: object) -> _FakeCounter:
        self.counters[name] = _FakeCounter()
        return self.counters[name]

    def create_histogram(self, name: str, **kwargs: object) -> _FakeHistogram:
        self.histograms[name] = _FakeHistogram()
        return self.histograms[name]


def test_otel_collector_records_store_and_error():
    from mcp_persist.otel import OTelMetricsCollector

    meter = _FakeMeter()
    collector = OTelMetricsCollector(meter, backend="sqlite", tenant_id="acme")
    collector.on_store_event("s", "1", 3.5)
    collector.on_error("store_event", RuntimeError("x"))

    store_records = meter.histograms["mcp_persist.store.duration_ms"].records
    assert store_records == [(3.5, {"backend": "sqlite", "tenant_id": "acme"})]
    error_calls = meter.counters["mcp_persist.errors"].calls
    assert error_calls == [(1, {"backend": "sqlite", "tenant_id": "acme", "operation": "store_event"})]
