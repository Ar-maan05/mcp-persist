# pyright: reportPrivateUsage=false
# pyright: reportArgumentType=false
"""The recording middleware classifies outcomes and never disturbs the request.

This is the one module that touches the SDK's provisional middleware surface,
so these tests double as its compatibility suite: if the contract shifts, they
are what fails first.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

import pytest
from mcp.server.context import ServerRequestContext
from mcp.shared.exceptions import MCPError
from mcp_types import INVALID_PARAMS, CallToolResult, TextContent
from pydantic import BaseModel, ValidationError

from mcp_persist.middleware import PersistenceRecorder, is_modern_era
from mcp_persist.records import PayloadPolicy, Record

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class CollectingFlusher:
    """Stands in for RecordFlusher; captures what the middleware submits."""

    def __init__(self) -> None:
        self.records: list[Record] = []

    def submit(self, record: Record) -> bool:
        self.records.append(record)
        return True


def _ctx(
    *,
    method: str = "tools/call",
    protocol_version: str = "2026-07-28",
    params: dict[str, Any] | None = None,
    request_id: Any = 1,
) -> ServerRequestContext[Any, Any]:
    return ServerRequestContext(
        session=cast(Any, None),
        lifespan_context={},
        protocol_version=protocol_version,
        method=method,
        params=params if params is not None else {"name": "search", "arguments": {"query": "q"}},
        request_id=request_id,
    )


def _recorder(**kwargs: Any) -> tuple[PersistenceRecorder, CollectingFlusher]:
    flusher = CollectingFlusher()
    return PersistenceRecorder(cast(Any, flusher), **kwargs), flusher


# ── Outcome classification ────────────────────────────────────────────────────


async def test_successful_request_records_ok() -> None:
    recorder, flusher = _recorder()

    async def call_next(ctx):
        return {"content": []}

    await recorder(_ctx(), call_next)

    assert len(flusher.records) == 1
    record = flusher.records[0]
    assert record.outcome == "ok"
    assert record.method == "tools/call"
    assert record.tool_name == "search"
    assert record.carrier == "middleware"
    assert record.protocol_version == "2026-07-28"
    assert record.request_id == "1"
    assert record.duration_ms is not None and record.duration_ms >= 0


async def test_tool_error_model_is_not_a_transport_failure() -> None:
    recorder, flusher = _recorder()

    async def call_next(ctx):
        return CallToolResult(content=[TextContent(type="text", text="boom")], is_error=True)

    await recorder(_ctx(), call_next)

    assert flusher.records[0].outcome == "tool_error"
    assert flusher.records[0].error_code is None


async def test_tool_error_raw_dict_alias_is_detected() -> None:
    recorder, flusher = _recorder()

    async def call_next(ctx):
        return {"isError": True, "content": []}

    await recorder(_ctx(), call_next)

    assert flusher.records[0].outcome == "tool_error"


async def test_a_successful_non_tool_method_is_never_a_tool_error() -> None:
    recorder, flusher = _recorder()

    async def call_next(ctx):
        return {"isError": True}

    await recorder(_ctx(method="prompts/list", params={}), call_next)

    assert flusher.records[0].outcome == "ok"


async def test_mcp_error_records_the_code_and_reraises() -> None:
    recorder, flusher = _recorder()

    async def call_next(ctx):
        raise MCPError(-32601, "no such method")

    with pytest.raises(MCPError):
        await recorder(_ctx(), call_next)

    assert flusher.records[0].outcome == "mcp_error"
    assert flusher.records[0].error_code == -32601


async def test_mcp_error_code_is_read_from_either_shape() -> None:
    """MCPError exposes both `.code` and `.error.code`; the SDK's own middleware
    reads the latter. Reading either keeps this working if one moves."""
    recorder, flusher = _recorder()
    error = MCPError(-32602, "bad params")
    assert error.code == error.error.code

    async def call_next(ctx):
        raise error

    with pytest.raises(MCPError):
        await recorder(_ctx(), call_next)

    assert flusher.records[0].error_code == -32602


async def test_validation_error_records_a_fixed_code_and_no_text() -> None:
    """A pydantic message quotes the client's own input, so it is never stored."""

    class Model(BaseModel):
        n: int

    recorder, flusher = _recorder()

    async def call_next(ctx):
        Model(n=cast(Any, "not-an-int"))

    with pytest.raises(ValidationError):
        await recorder(_ctx(), call_next)

    record = flusher.records[0]
    assert record.outcome == "validation_error"
    assert record.error_code == INVALID_PARAMS
    assert "not-an-int" not in str(record.as_dict())


async def test_arbitrary_exception_is_recorded_and_reraised() -> None:
    recorder, flusher = _recorder()

    async def call_next(ctx):
        raise RuntimeError("secret-value-in-message")

    with pytest.raises(RuntimeError):
        await recorder(_ctx(), call_next)

    record = flusher.records[0]
    assert record.outcome == "exception"
    assert "secret-value-in-message" not in str(record.as_dict())


async def test_client_disconnect_records_cancelled_and_propagates() -> None:
    """The modern transport cancels the handler task group on disconnect."""
    recorder, flusher = _recorder()

    async def call_next(ctx):
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await recorder(_ctx(), call_next)

    assert flusher.records[0].outcome == "cancelled"


async def test_the_original_exception_object_is_reraised_untouched() -> None:
    recorder, _ = _recorder()
    original = RuntimeError("original")

    async def call_next(ctx):
        raise original

    with pytest.raises(RuntimeError) as caught:
        await recorder(_ctx(), call_next)

    assert caught.value is original


async def test_the_handler_result_is_returned_unchanged() -> None:
    recorder, _ = _recorder()
    sentinel = {"content": [], "marker": object()}

    async def call_next(ctx):
        return sentinel

    assert await recorder(_ctx(), call_next) is sentinel


# ── Shape of the record ───────────────────────────────────────────────────────


async def test_notifications_are_recorded_without_a_request_id() -> None:
    recorder, flusher = _recorder()

    async def call_next(ctx):
        return None

    await recorder(_ctx(method="notifications/initialized", params={}, request_id=None), call_next)

    record = flusher.records[0]
    assert record.kind == "notification"
    assert record.request_id is None


async def test_the_era_is_taken_from_the_context() -> None:
    recorder, flusher = _recorder()

    async def call_next(ctx):
        return {}

    await recorder(_ctx(protocol_version="2025-11-25"), call_next)

    assert flusher.records[0].protocol_version == "2025-11-25"


async def test_no_payload_is_captured_by_default() -> None:
    recorder, flusher = _recorder()

    async def call_next(ctx):
        return {}

    await recorder(_ctx(params={"name": "search", "arguments": {"query": "secret"}}), call_next)

    assert flusher.records[0].payload is None


async def test_an_allowed_argument_is_captured() -> None:
    recorder, flusher = _recorder(policy=PayloadPolicy(tool_arguments={"search": ["query"]}))

    async def call_next(ctx):
        return {}

    await recorder(_ctx(params={"name": "search", "arguments": {"query": "q", "key": "sk-x"}}), call_next)

    assert flusher.records[0].payload == {"query": "q"}


async def test_a_broken_context_never_breaks_the_request() -> None:
    """Recording is best effort; the observed request always wins."""

    class Exploding:
        def submit(self, record):
            raise RuntimeError("flusher exploded")

    recorder = PersistenceRecorder(cast(Any, Exploding()))

    async def call_next(ctx):
        return {"ok": True}

    assert await recorder(_ctx(), call_next) == {"ok": True}


# ── The honest boundary ───────────────────────────────────────────────────────


async def test_modern_era_warns_once(caplog: pytest.LogCaptureFixture) -> None:
    recorder, _ = _recorder()

    async def call_next(ctx):
        return {}

    with caplog.at_level(logging.WARNING, logger="mcp_persist.middleware"):
        await recorder(_ctx(protocol_version="2026-07-28"), call_next)
        await recorder(_ctx(protocol_version="2026-07-28"), call_next)

    warnings = [r for r in caplog.records if "stateless single-exchange" in r.getMessage()]
    assert len(warnings) == 1
    assert "2026-07-28" in warnings[0].getMessage()


async def test_handshake_era_does_not_warn(caplog: pytest.LogCaptureFixture) -> None:
    recorder, _ = _recorder()

    async def call_next(ctx):
        return {}

    with caplog.at_level(logging.WARNING, logger="mcp_persist.middleware"):
        await recorder(_ctx(protocol_version="2025-11-25"), call_next)

    assert [r for r in caplog.records if "stateless single-exchange" in r.getMessage()] == []


async def test_the_warning_can_be_turned_off(caplog: pytest.LogCaptureFixture) -> None:
    recorder, _ = _recorder(warn_on_bypass=False)

    async def call_next(ctx):
        return {}

    with caplog.at_level(logging.WARNING, logger="mcp_persist.middleware"):
        await recorder(_ctx(protocol_version="2026-07-28"), call_next)

    assert [r for r in caplog.records if "stateless single-exchange" in r.getMessage()] == []


async def test_no_warning_without_an_event_store(caplog: pytest.LogCaptureFixture) -> None:
    """Nothing is being bypassed if nothing was configured."""
    recorder, _ = _recorder(event_store_configured=False)

    async def call_next(ctx):
        return {}

    with caplog.at_level(logging.WARNING, logger="mcp_persist.middleware"):
        await recorder(_ctx(protocol_version="2026-07-28"), call_next)

    assert [r for r in caplog.records if "stateless single-exchange" in r.getMessage()] == []


async def test_warning_only_mode_passes_through_without_recording() -> None:
    recorder = PersistenceRecorder(None)

    async def call_next(ctx):
        return {"ok": True}

    assert await recorder(_ctx(), call_next) == {"ok": True}


async def test_warning_only_mode_does_not_swallow_errors() -> None:
    recorder = PersistenceRecorder(None)

    async def call_next(ctx):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await recorder(_ctx(), call_next)


# ── Era classification ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("version", "modern"),
    [
        ("2024-11-05", False),
        ("2025-03-26", False),
        ("2025-06-18", False),
        ("2025-11-25", False),
        ("2026-07-28", True),
        ("2099-01-01", True),
    ],
)
def test_era_classification_matches_the_sdks_routing(version: str, modern: bool) -> None:
    """An unknown future revision counts as modern, exactly as the SDK routes it."""
    assert is_modern_era(version) is modern


# ── Client-controlled identifiers are bounded ────────────────────────────────


async def test_an_oversized_request_id_is_clipped() -> None:
    """A client picks its own request id and may make it enormous."""
    from mcp_persist.middleware import MAX_IDENTIFIER_BYTES

    recorder, flusher = _recorder()

    async def call_next(ctx):
        return {}

    await recorder(_ctx(request_id="x" * 100_000), call_next)

    request_id = flusher.records[0].request_id
    assert request_id is not None
    assert len(request_id.encode("utf-8")) <= MAX_IDENTIFIER_BYTES


async def test_an_oversized_tool_name_is_clipped() -> None:
    from mcp_persist.middleware import MAX_IDENTIFIER_BYTES

    recorder, flusher = _recorder()

    async def call_next(ctx):
        return {}

    await recorder(_ctx(params={"name": "t" * 50_000, "arguments": {}}), call_next)

    tool_name = flusher.records[0].tool_name
    assert tool_name is not None
    assert len(tool_name.encode("utf-8")) <= MAX_IDENTIFIER_BYTES


async def test_a_raising_log_handler_cannot_break_a_request() -> None:
    """The bypass warning runs before call_next, so it must never raise."""

    class Exploding(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            raise RuntimeError("logging is broken")

    handler = Exploding()
    log = logging.getLogger("mcp_persist.middleware")
    log.addHandler(handler)
    try:
        recorder, _ = _recorder()

        async def call_next(ctx):
            return {"ok": True}

        assert await recorder(_ctx(protocol_version="2026-07-28"), call_next) == {"ok": True}
    finally:
        log.removeHandler(handler)


def test_registration_goes_through_the_adapter() -> None:
    """`install()` is the only place `MCPServer.middleware` is touched."""
    from mcp_persist.middleware import install

    class FakeServer:
        def __init__(self) -> None:
            self.middleware: list[Any] = []

    server = FakeServer()
    recorder = PersistenceRecorder(None)
    install(server, recorder)

    assert server.middleware == [recorder]


def test_protocol_support_reports_both_eras() -> None:
    from mcp_persist.middleware import protocol_support

    handshake, modern = protocol_support()

    assert "2025-11-25" in handshake
    assert "2026-07-28" in modern
    assert not set(handshake) & set(modern)
