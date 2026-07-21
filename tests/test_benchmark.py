"""Regression tests for the standalone benchmark harness."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_benchmark():
    path = Path(__file__).parents[1] / "benchmarks" / "benchmark.py"
    spec = importlib.util.spec_from_file_location("mcp_persist_benchmark", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_print_table_orders_raw_batched_and_replay_sections(capsys):
    benchmark = _load_benchmark()
    result = {
        "Redis": {
            "mean_us": 30.0,
            "p50_us": 25.0,
            "p95_us": 40.0,
            "throughput_eps": 10_000.0,
            "batched_throughput_eps": 30_000.0,
            "replay_100_ms": 1.0,
            "replay_1000_ms": 5.0,
            "replay_10000_ms": 50.0,
        }
    }

    benchmark.print_table(result)
    output = capsys.readouterr().out

    raw = output.index("Storage Performance:")
    batched = output.index("Batched Storage Performance")
    replay = output.index("Replay Performance")
    assert raw < batched < replay
    assert "3.00x" in output
