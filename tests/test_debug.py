# pyright: reportPrivateUsage=false
"""Tests for DEBUG_PERSIST developer mode."""

from __future__ import annotations

import logging

from mcp_persist import _debug
from mcp_persist.metrics import LoggingMetricsCollector, NoOpMetricsCollector


def test_debug_enabled_reads_truthy_values():
    assert _debug.debug_enabled({"DEBUG_PERSIST": "1"}) is True
    assert _debug.debug_enabled({"DEBUG_PERSIST": "true"}) is True
    assert _debug.debug_enabled({"DEBUG_PERSIST": "ON"}) is True
    assert _debug.debug_enabled({"DEBUG_PERSIST": "0"}) is False
    assert _debug.debug_enabled({"DEBUG_PERSIST": ""}) is False
    assert _debug.debug_enabled({}) is False


def test_default_metrics_collector_switches_on_flag(monkeypatch):
    monkeypatch.delenv("DEBUG_PERSIST", raising=False)
    assert isinstance(_debug.default_metrics_collector(), NoOpMetricsCollector)

    monkeypatch.setenv("DEBUG_PERSIST", "1")
    assert isinstance(_debug.default_metrics_collector(), LoggingMetricsCollector)


def test_debug_log_emits_only_when_enabled(monkeypatch, caplog):
    monkeypatch.delenv("DEBUG_PERSIST", raising=False)
    with caplog.at_level(logging.DEBUG, logger="mcp_persist"):
        _debug.debug_log("PURGE removed=%d", 7)
    assert "PURGE" not in caplog.text

    monkeypatch.setenv("DEBUG_PERSIST", "1")
    with caplog.at_level(logging.DEBUG, logger="mcp_persist"):
        _debug.debug_log("PURGE removed=%d", 7)
    assert "PURGE removed=7" in caplog.text


def test_configure_debug_logging_is_idempotent_and_gated(monkeypatch):
    logger = logging.getLogger("mcp_persist")
    original_handlers = list(logger.handlers)
    original_installed = _debug._handler_installed
    original_level = logger.level
    try:
        _debug._handler_installed = False

        monkeypatch.delenv("DEBUG_PERSIST", raising=False)
        _debug.configure_debug_logging()
        assert _debug._handler_installed is False  # flag off: nothing configured

        monkeypatch.setenv("DEBUG_PERSIST", "1")
        _debug.configure_debug_logging()
        assert _debug._handler_installed is True
        assert logger.level == logging.DEBUG
        after_first = list(logger.handlers)

        _debug.configure_debug_logging()  # second call is a no-op, adds no handler
        assert logger.handlers == after_first
    finally:
        logger.handlers = original_handlers
        logger.setLevel(original_level)
        _debug._handler_installed = original_installed


def test_configure_debug_logging_attaches_handler_when_unconfigured(monkeypatch):
    """With no app logging configured, developer mode attaches its own stderr handler."""
    logger = logging.getLogger("mcp_persist")
    root = logging.getLogger()
    saved = (list(logger.handlers), logger.level, _debug._handler_installed, list(root.handlers))
    try:
        logger.handlers = []
        root.handlers = []  # simulate a process with no logging setup at all
        _debug._handler_installed = False
        monkeypatch.setenv("DEBUG_PERSIST", "1")

        _debug.configure_debug_logging()
        assert len(logger.handlers) == 1
    finally:
        logger.handlers, logger.level, _debug._handler_installed, root.handlers = (
            saved[0],
            saved[1],
            saved[2],
            saved[3],
        )
