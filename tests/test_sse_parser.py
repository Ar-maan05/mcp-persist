"""Tests for the incremental SSE wire-format parser.

The interesting property is that the *same* frame, fed in every possible split
position and under every line-ending convention, parses to the same result.
"""

import pytest

from mcp_persist._sse_parser import SSEFrame, SSEParser

# A realistic live MCP event: id + event + JSON data, the way sse-starlette
# emits it (each field line and the terminator end in the chosen separator).
MESSAGE_FIELDS = [
    "id: 7",
    "event: message",
    'data: {"jsonrpc":"2.0","id":1,"result":{}}',
]
MESSAGE_DATA = '{"jsonrpc":"2.0","id":1,"result":{}}'

# A priming event: id + empty data, no event field.
PRIMING_FIELDS = ["id: 5", "data: "]


def build(fields: list[str], sep: str) -> str:
    """Render SSE field lines into a single frame terminated by a blank line."""
    return "".join(f"{line}{sep}" for line in fields) + sep


def feed_in_chunks(parser: SSEParser, text: str, sizes: list[int]) -> list[SSEFrame]:
    frames: list[SSEFrame] = []
    pos = 0
    for size in sizes:
        frames += parser.feed(text[pos : pos + size])
        pos += size
    if pos < len(text):
        frames += parser.feed(text[pos:])
    return frames


# The line endings real MCP upstreams emit: "\n" and sse-starlette's default
# "\r\n". Both terminate the final frame with a definite terminator, so they
# parse without an EOF flush. A pure-"\r" stream is ambiguous at EOF and is
# covered separately via flush() below.
SEPS = ["\n", "\r\n"]


# --- whole-frame parsing, all separators -----------------------------------


@pytest.mark.parametrize("sep", SEPS)
def test_message_frame(sep: str) -> None:
    frames = SSEParser().feed(build(MESSAGE_FIELDS, sep))
    assert frames == [SSEFrame(data=MESSAGE_DATA, event="message", original_id="7")]


@pytest.mark.parametrize("sep", SEPS)
def test_priming_frame(sep: str) -> None:
    frames = SSEParser().feed(build(PRIMING_FIELDS, sep))
    assert frames == [SSEFrame(data="", event=None, original_id="5")]


# --- split at every byte boundary, all separators --------------------------


@pytest.mark.parametrize("sep", SEPS)
def test_message_split_at_every_position(sep: str) -> None:
    raw = build(MESSAGE_FIELDS, sep)
    for split_at in range(1, len(raw)):
        parser = SSEParser()
        frames = parser.feed(raw[:split_at]) + parser.feed(raw[split_at:])
        assert frames == [SSEFrame(data=MESSAGE_DATA, event="message", original_id="7")], (
            f"sep={sep!r} split_at={split_at}"
        )


@pytest.mark.parametrize("sep", SEPS)
def test_priming_split_at_every_position(sep: str) -> None:
    raw = build(PRIMING_FIELDS, sep)
    for split_at in range(1, len(raw)):
        parser = SSEParser()
        frames = parser.feed(raw[:split_at]) + parser.feed(raw[split_at:])
        assert frames == [SSEFrame(data="", event=None, original_id="5")], f"sep={sep!r} split_at={split_at}"


@pytest.mark.parametrize("sep", SEPS)
def test_split_into_single_characters(sep: str) -> None:
    raw = build(MESSAGE_FIELDS, sep)
    parser = SSEParser()
    frames: list[SSEFrame] = []
    for ch in raw:
        frames += parser.feed(ch)
    assert frames == [SSEFrame(data=MESSAGE_DATA, event="message", original_id="7")]


# --- data: leading-space handling ------------------------------------------


def test_data_with_leading_space() -> None:
    (frame,) = SSEParser().feed("data: hello\n\n")
    assert frame.data == "hello"


def test_data_without_leading_space() -> None:
    (frame,) = SSEParser().feed("data:hello\n\n")
    assert frame.data == "hello"


def test_data_preserves_second_space() -> None:
    # Only one leading space is framing; the rest is payload.
    (frame,) = SSEParser().feed("data:  hello\n\n")
    assert frame.data == " hello"


def test_empty_data_yields_empty_string() -> None:
    (frame,) = SSEParser().feed("data:\n\n")
    assert frame.data == ""


def test_multiline_data_joined_with_newline() -> None:
    (frame,) = SSEParser().feed("data: a\ndata: b\n\n")
    assert frame.data == "a\nb"


# --- comments and non-dispatching blocks -----------------------------------


def test_comment_line_is_skipped() -> None:
    frames = SSEParser().feed(": this is a comment\n\n")
    assert frames == []


def test_comment_interleaved_with_data() -> None:
    (frame,) = SSEParser().feed(": keep-alive\ndata: payload\n\n")
    assert frame == SSEFrame(data="payload", event=None, original_id=None)


def test_id_only_block_dispatches_nothing() -> None:
    # No data field -> nothing to deliver.
    assert SSEParser().feed("id: 9\n\n") == []


def test_event_only_block_dispatches_nothing() -> None:
    assert SSEParser().feed("event: ping\n\n") == []


def test_stray_blank_lines_dispatch_nothing() -> None:
    assert SSEParser().feed("\n\n\n") == []


# --- multiple frames -------------------------------------------------------


def test_multiple_frames_in_one_chunk() -> None:
    raw = "data: a\n\ndata: b\n\ndata: c\n\n"
    frames = SSEParser().feed(raw)
    assert [f.data for f in frames] == ["a", "b", "c"]


def test_multiple_frames_streamed_across_chunks() -> None:
    raw = build(MESSAGE_FIELDS, "\r\n") + build(PRIMING_FIELDS, "\r\n")
    parser = SSEParser()
    # 3-byte chunks exercise mid-terminator and mid-line boundaries.
    frames = feed_in_chunks(parser, raw, [3] * (len(raw) // 3))
    assert frames == [
        SSEFrame(data=MESSAGE_DATA, event="message", original_id="7"),
        SSEFrame(data="", event=None, original_id="5"),
    ]


# --- incremental / boundary specifics --------------------------------------


def test_crlf_split_between_cr_and_lf() -> None:
    # The terminator "\r\n" is split so "\r" ends one chunk and "\n" starts the
    # next; they must still count as a single line terminator.
    parser = SSEParser()
    assert parser.feed("data: x\r") == []
    frames = parser.feed("\n\r\n")
    assert frames == [SSEFrame(data="x", event=None, original_id=None)]


def test_partial_frame_held_until_blank_line() -> None:
    parser = SSEParser()
    assert parser.feed("data: incomplete\n") == []  # no terminating blank line yet
    assert parser.feed("\n") == [SSEFrame(data="incomplete", event=None, original_id=None)]


def test_empty_chunk_returns_nothing() -> None:
    parser = SSEParser()
    assert parser.feed("") == []
    assert parser.feed("data: x") == []
    assert parser.feed("") == []
    assert parser.feed("\n\n") == [SSEFrame(data="x", event=None, original_id=None)]


def test_parser_reusable_after_frame() -> None:
    parser = SSEParser()
    assert parser.feed("data: one\n\n") == [SSEFrame(data="one", event=None, original_id=None)]
    assert parser.feed("data: two\n\n") == [SSEFrame(data="two", event=None, original_id=None)]


def test_lone_trailing_cr_not_emitted_early() -> None:
    # A lone "\r" at end of buffer must not be treated as a complete line until
    # we know whether an "\n" follows.
    parser = SSEParser()
    assert parser.feed("data: x\r") == []
    assert parser._buf == "data: x\r"  # held back intact


# --- flush() / end-of-stream ----------------------------------------------


def test_flush_resolves_pure_cr_frame() -> None:
    # A frame terminated only by bare "\r" cannot be dispatched until EOF, since
    # mid-stream the trailing "\r" might begin a "\r\n".
    parser = SSEParser()
    assert parser.feed("data: x\r\r") == []
    assert parser.flush() == [SSEFrame(data="x", event=None, original_id=None)]


@pytest.mark.parametrize("split_at", range(1, len("data: x\r\r")))
def test_flush_pure_cr_frame_split_at_every_position(split_at: int) -> None:
    raw = "data: x\r\r"
    parser = SSEParser()
    frames = parser.feed(raw[:split_at]) + parser.feed(raw[split_at:]) + parser.flush()
    assert frames == [SSEFrame(data="x", event=None, original_id=None)], f"split_at={split_at}"


def test_flush_discards_unterminated_final_line() -> None:
    # No terminator at all -> incomplete event, discarded per the SSE spec.
    parser = SSEParser()
    assert parser.feed("data: dangling") == []
    assert parser.flush() == []


def test_flush_is_noop_after_clean_frame() -> None:
    parser = SSEParser()
    assert parser.feed("data: done\n\n") == [SSEFrame(data="done", event=None, original_id=None)]
    assert parser.flush() == []
