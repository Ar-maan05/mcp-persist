"""Incremental Server-Sent Events (SSE) wire-format parser.

The persistence proxy reads an upstream MCP server's SSE response one network
chunk at a time. Chunks do not align with event boundaries, so this module
turns an arbitrary sequence of text chunks into complete :class:`SSEFrame`
objects, holding any partial frame in internal state until the rest arrives.

Line handling follows the WHATWG event-stream rules: lines may be terminated by
``\\n``, ``\\r``, or ``\\r\\n``, and a blank line dispatches the event. This
matters in practice because the SDK's upstream (``sse-starlette``) emits
``\\r\\n`` separators by default, so a real priming event arrives as
``id: 5\\r\\ndata: \\r\\n\\r\\n``.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["SSEFrame", "SSEParser"]


@dataclass
class SSEFrame:
    """One dispatched SSE event.

    Attributes:
        data: The event payload. Multiple ``data:`` lines are joined with
            ``\\n``; an empty ``data:`` line yields ``""`` (a priming event).
        event: The ``event:`` field value, or ``None`` if the frame had none.
        original_id: The ``id:`` field value as it appeared on this frame, or
            ``None`` if absent. The proxy assigns its own event IDs, so this is
            informational only. Unlike a browser ``EventSource``, it is *not*
            sticky across frames.
    """

    data: str
    event: str | None
    original_id: str | None


class SSEParser:
    """Stateful, incremental SSE parser.

    Feed raw text chunks in order; each :meth:`feed` returns the frames that
    completed within that chunk. A frame split across chunk boundaries (at any
    character, including in the middle of a ``\\r\\n`` terminator) is buffered
    and emitted once the terminating blank line arrives.
    """

    def __init__(self) -> None:
        # Unconsumed tail: a partial line, or a lone trailing "\r" whose
        # following byte (possibly "\n") has not arrived yet.
        self._buf: str = ""
        # Field accumulators for the event currently being built. Per the SSE
        # spec, a "data" line appends its value plus a "\n" to the data buffer;
        # an empty data buffer at dispatch time means no "data:" field was seen.
        self._data: str = ""
        self._event: str | None = None
        self._id: str | None = None

    def feed(self, chunk: str) -> list[SSEFrame]:
        """Consume a chunk and return any frames it completed."""
        self._buf += chunk
        return self._drain()

    def flush(self) -> list[SSEFrame]:
        """Resolve end-of-stream and return any final frame.

        Call this once the upstream stream has closed. Its only effect is to
        treat a held trailing ``\\r`` as a completed line terminator: mid-stream
        a lone ``\\r`` is ambiguous (it could begin a ``\\r\\n``), so it is held
        until more bytes arrive — but at EOF none will. A genuinely unterminated
        final line (no terminator at all) is discarded, per the SSE spec, which
        does not dispatch an event that lacks its terminating blank line.
        """
        if self._buf.endswith("\r"):
            self._buf = self._buf[:-1] + "\n"
        return self._drain()

    def _drain(self) -> list[SSEFrame]:
        lines, self._buf = _split_lines(self._buf)
        frames: list[SSEFrame] = []
        for line in lines:
            if line == "":
                frame = self._dispatch()
                if frame is not None:
                    frames.append(frame)
                continue
            if line.startswith(":"):
                # Comment line — ignored, never dispatches a frame.
                continue
            self._consume_field(line)
        return frames

    def _consume_field(self, line: str) -> None:
        name, sep, value = line.partition(":")
        if sep and value.startswith(" "):
            # A single leading space after the colon is part of the framing,
            # not the value; any further spaces are significant.
            value = value[1:]

        if name == "data":
            self._data += value + "\n"
        elif name == "event":
            self._event = value
        elif name == "id":
            self._id = value
        # "retry" and unknown fields carry no information the proxy needs.

    def _dispatch(self) -> SSEFrame | None:
        """Build a frame from the accumulated fields and reset for the next one.

        Returns ``None`` when the block had no ``data:`` field (e.g. a stray
        blank line, or an ``id:``/comment-only block), which dispatches nothing.
        """
        data = self._data
        event = self._event
        original_id = self._id
        self._data = ""
        self._event = None
        self._id = None

        if data == "":
            return None
        # Strip the trailing "\n" the last data line contributed.
        if data.endswith("\n"):
            data = data[:-1]
        return SSEFrame(data=data, event=event, original_id=original_id)


def _split_lines(buf: str) -> tuple[list[str], str]:
    """Split ``buf`` into complete lines plus the unconsumed remainder.

    Recognises ``\\n``, ``\\r``, and ``\\r\\n`` terminators. A lone ``\\r`` at
    the very end of ``buf`` is left in the remainder: the next chunk might begin
    with ``\\n``, in which case the two form a single ``\\r\\n`` terminator.
    """
    lines: list[str] = []
    start = 0
    i = 0
    n = len(buf)
    while i < n:
        c = buf[i]
        if c == "\n":
            lines.append(buf[start:i])
            i += 1
            start = i
        elif c == "\r":
            if i + 1 >= n:
                # Possible split "\r\n": don't consume until the next char is in.
                break
            lines.append(buf[start:i])
            i += 2 if buf[i + 1] == "\n" else 1
            start = i
        else:
            i += 1
    return lines, buf[start:]
