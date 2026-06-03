"""Optional payload compression for mcp-persist event stores.

Every backend stores the serialized ``JSONRPCMessage`` as text. For deployments
whose MCP messages carry large tool results or big JSON-RPC bodies, that text can
dominate storage and (on Redis) memory. Passing ``compression="gzip"`` to a store
gzip-compresses payloads above a size threshold before they are written, and the
read path transparently decompresses them.

The on-the-wire form is marker-prefixed so the two forms coexist safely:

* A serialized ``JSONRPCMessage`` is always a JSON object starting with ``{``,
  and priming events are the empty string ``""`` — neither can collide with the
  :data:`_GZIP_PREFIX` marker.
* :func:`decompress_payload` keys entirely off that marker, so a store reads a
  compressed payload even with compression **disabled**. That keeps rolling
  upgrades and cross-backend :func:`mcp_persist.migrate` working when only some
  writers had compression enabled.

Because the compressed bytes are base64-encoded to stay text-safe in the existing
``TEXT`` columns and Redis hash fields, :func:`compress_payload` only keeps the
compressed form when it is actually smaller than the original — small payloads
fall through unchanged and pay nothing.
"""

from __future__ import annotations

import base64
import gzip

# Marker prefixing a gzip+base64-encoded payload. See the module docstring for
# why this can never collide with a real (uncompressed) payload.
_GZIP_PREFIX = "gz:"

# Compression codecs accepted by the ``compression=`` store argument.
SUPPORTED_COMPRESSION = ("gzip",)


def validate_compression(codec: str | None) -> None:
    """Raise ``ValueError`` unless ``codec`` is ``None`` or a supported codec."""
    if codec is not None and codec not in SUPPORTED_COMPRESSION:
        raise ValueError(f"compression must be None or one of {SUPPORTED_COMPRESSION}, got {codec!r}")


def compress_payload(payload: str, *, codec: str | None, min_bytes: int) -> str:
    """Return ``payload`` compressed when ``codec`` is set and it is worthwhile.

    Compresses only when ``codec == "gzip"``, ``payload`` is non-empty, its UTF-8
    size is at least ``min_bytes``, and the gzip+base64 result is strictly smaller
    than the original. Otherwise ``payload`` is returned unchanged (and stored
    plain). The empty string (priming events) always passes through untouched.
    """
    if codec is None or not payload:
        return payload
    raw = payload.encode("utf-8")
    if len(raw) < min_bytes:
        return payload
    encoded = _GZIP_PREFIX + base64.b64encode(gzip.compress(raw)).decode("ascii")
    # base64 adds ~33% overhead; only keep the compressed form if it still wins,
    # so an incompressible payload is never made larger.
    if len(encoded) >= len(payload):
        return payload
    return encoded


def decompress_payload(stored: str) -> str:
    """Inverse of :func:`compress_payload`; plain payloads pass through unchanged.

    Decoding is driven entirely by the :data:`_GZIP_PREFIX` marker, so this is
    safe to call on any stored payload regardless of whether the reading store
    has compression enabled.
    """
    if stored.startswith(_GZIP_PREFIX):
        return gzip.decompress(base64.b64decode(stored[len(_GZIP_PREFIX) :])).decode("utf-8")
    return stored
