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
import zlib

# Marker prefixing a gzip+base64-encoded payload. See the module docstring for
# why this can never collide with a real (uncompressed) payload.
_GZIP_PREFIX = "gz:"

# Compression codecs accepted by the ``compression=`` store argument.
SUPPORTED_COMPRESSION = ("gzip",)

# Hard ceiling on the *decompressed* size of a single payload, enforced while
# inflating so a decompression bomb can never be fully materialized. A real
# JSON-RPC event is orders of magnitude under this; the cap only ever trips on a
# maliciously crafted ``gz:`` payload planted by something with direct write
# access to the backing store. Decoding stays driven by the marker, so a planted
# bomb is rejected (and the one event skipped) rather than expanded into memory.
MAX_DECOMPRESSED_BYTES = 100 * 1024 * 1024  # 100 MiB

# 16 + MAX_WBITS selects gzip framing for zlib's incremental decompressor, so it
# decodes exactly what ``gzip.compress`` produced in :func:`compress_payload`.
_GZIP_WBITS = zlib.MAX_WBITS | 16


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
    has compression enabled. Inflation is bounded by
    :data:`MAX_DECOMPRESSED_BYTES`; a payload that would expand past the cap
    raises ``ValueError`` (a decompression-bomb guard) instead of being fully
    materialized. Callers already treat any decode failure as a corrupt event and
    skip it, so a bomb costs one skipped event, not the worker.
    """
    if stored.startswith(_GZIP_PREFIX):
        raw = base64.b64decode(stored[len(_GZIP_PREFIX) :])
        return _gunzip_bounded(raw, MAX_DECOMPRESSED_BYTES)
    return stored


def _gunzip_bounded(raw: bytes, max_bytes: int) -> str:
    """Gunzip ``raw``, raising ``ValueError`` if it would exceed ``max_bytes``.

    Inflates incrementally with an output cap rather than calling
    ``gzip.decompress`` (which would allocate the whole, possibly enormous,
    result first). Requesting ``max_bytes + 1`` lets a payload that lands exactly
    on the cap through while still detecting anything larger: either the inflate
    returns more than ``max_bytes`` bytes, or it leaves unconsumed input behind
    because it hit the output limit.
    """
    decompressor = zlib.decompressobj(wbits=_GZIP_WBITS)
    out = decompressor.decompress(raw, max_bytes + 1)
    if len(out) > max_bytes or decompressor.unconsumed_tail:
        raise ValueError(f"refusing to decompress payload over {max_bytes}-byte cap (possible decompression bomb)")
    out += decompressor.flush()
    if len(out) > max_bytes:
        raise ValueError(f"refusing to decompress payload over {max_bytes}-byte cap (possible decompression bomb)")
    return out.decode("utf-8")
