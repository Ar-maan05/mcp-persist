"""Developer mode: ``DEBUG_PERSIST=1`` turns on human-readable operation logs.

Set the environment variable ``DEBUG_PERSIST`` to a truthy value (``1``,
``true``, ``yes``, ``on``) and every store built afterwards narrates what it
does to stderr:

    SAVE  stream=abc event=42 in 0.31ms
    LOAD  stream=abc events=17 in 1.02ms
    FLUSH stream=abc events=5
    PURGE removed=128
    error in store_event: ...

It is a zero-config alternative to wiring a :class:`~mcp_persist.MetricsCollector`
by hand: the store/replay lines come from a :class:`LoggingMetricsCollector`
installed as the default collector when the flag is set, and the FLUSH/PURGE
lines come from :func:`debug_log`, which is a no-op unless the flag is on. When
the flag is unset the default stays :class:`NoOpMetricsCollector`, so there is no
runtime cost.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from mcp_persist.metrics import LoggingMetricsCollector, NoOpMetricsCollector

if TYPE_CHECKING:
    from mcp_persist.metrics import MetricsCollector

_LOGGER_NAME = "mcp_persist"
_ENV_VAR = "DEBUG_PERSIST"
_TRUTHY = frozenset({"1", "true", "yes", "on"})

# The stderr handler is attached at most once, even if debug mode is queried
# many times or from several stores. Guarded by module import (single-threaded).
_handler_installed = False


def debug_enabled(env: os._Environ[str] | dict[str, str] | None = None) -> bool:
    """Return whether developer mode is on, reading ``DEBUG_PERSIST`` from ``env``.

    ``env`` defaults to ``os.environ`` and is injectable so tests can flip the
    flag without mutating the real process environment.
    """
    source = os.environ if env is None else env
    return source.get(_ENV_VAR, "").strip().lower() in _TRUTHY


def configure_debug_logging(env: dict[str, str] | None = None) -> None:
    """Route the ``mcp_persist`` logger to stderr at DEBUG when the flag is set.

    Idempotent: attaches a single stderr handler and lowers the logger level the
    first time it runs with the flag on, and does nothing on later calls or when
    the flag is off. Leaves the logger untouched if the application has already
    configured its own handlers, so it never fights an existing logging setup.
    """
    global _handler_installed
    if _handler_installed or not debug_enabled(env):
        return

    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    # Respect an app that already configured logging for this logger (its own
    # handlers, or propagation to a configured root): only add ours if the lines
    # would otherwise go nowhere.
    if not logger.handlers and not logging.getLogger().handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter("%(name)s %(message)s"))
        logger.addHandler(handler)
    _handler_installed = True


def default_metrics_collector() -> MetricsCollector:
    """The collector a store uses when the caller passes ``metrics=None``.

    Returns a :class:`LoggingMetricsCollector` (which emits SAVE/LOAD lines at
    DEBUG) in developer mode, otherwise the free :class:`NoOpMetricsCollector`
    the stores special-case to skip timing entirely.
    """
    if debug_enabled():
        configure_debug_logging()
        return LoggingMetricsCollector()
    return NoOpMetricsCollector()


def debug_log(message: str, *args: object) -> None:
    """Emit one developer-mode line, cheaply skipped when the flag is off.

    Used for the operations a :class:`MetricsCollector` does not cover (FLUSH,
    PURGE). The env check short-circuits before any formatting, so this is safe
    to sprinkle on paths that are not metrics-instrumented.
    """
    if debug_enabled():
        logging.getLogger(_LOGGER_NAME).debug(message, *args)
