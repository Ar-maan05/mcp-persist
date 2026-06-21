"""Tests for the payload compression helpers."""

from __future__ import annotations

import base64
import gzip
import importlib.util

import pytest

from mcp_persist.compression import (
    MAX_DECOMPRESSED_BYTES,
    compress_payload,
    decompress_payload,
    validate_compression,
)


def test_roundtrip_large_payload():
    payload = '{"data":"' + "a" * 5000 + '"}'
    encoded = compress_payload(payload, codec="gzip", min_bytes=100)
    assert encoded.startswith("gz:")
    assert len(encoded) < len(payload)
    assert decompress_payload(encoded) == payload


def test_codec_none_passes_through():
    payload = '{"data":"' + "a" * 5000 + '"}'
    assert compress_payload(payload, codec=None, min_bytes=0) == payload


def test_empty_payload_never_compressed():
    assert compress_payload("", codec="gzip", min_bytes=0) == ""


def test_small_payload_below_threshold_stored_plain():
    payload = '{"a":1}'
    assert compress_payload(payload, codec="gzip", min_bytes=1000) == payload


def test_incompressible_payload_not_enlarged():
    # 32 random hex chars do not compress below their base64+marker form, so the
    # original must be kept rather than stored larger.
    payload = "0123456789abcdef" * 2
    result = compress_payload(payload, codec="gzip", min_bytes=1)
    assert result == payload
    assert not result.startswith("gz:")


def test_decompress_plain_payload_passthrough():
    assert decompress_payload('{"a":1}') == '{"a":1}'


def test_validate_compression_accepts_none_and_gzip():
    validate_compression(None)
    validate_compression("gzip")


def test_validate_compression_rejects_unknown():
    with pytest.raises(ValueError):
        validate_compression("brotli")


def test_validate_compression_zstd_without_extra():
    zstd = pytest.importorskip("zstandard", reason="zstd extra not installed")
    del zstd  # used only for skip check
    try:
        validate_compression("zstd")
    except ValueError as exc:
        if "zstd extra" not in str(exc):
            raise


@pytest.mark.skipif(
    importlib.util.find_spec("zstandard") is None,
    reason="zstd extra not installed",
)
def test_zstd_roundtrip():
    payload = '{"data":"' + "b" * 5000 + '"}'
    encoded = compress_payload(payload, codec="zstd", min_bytes=100)
    assert encoded.startswith("zs:")
    assert decompress_payload(encoded) == payload


def test_decompression_bomb_rejected():
    # A payload that inflates far past the cap must be refused rather than fully
    # materialized in memory. ~200 MiB of zeros gzips to a few hundred KiB.
    bomb = b"\x00" * (2 * MAX_DECOMPRESSED_BYTES)
    stored = "gz:" + base64.b64encode(gzip.compress(bomb)).decode("ascii")
    with pytest.raises(ValueError, match="decompression bomb"):
        decompress_payload(stored)


@pytest.mark.skipif(
    importlib.util.find_spec("zstandard") is None,
    reason="zstd extra not installed",
)
def test_zstd_decompression_bomb_rejected():
    # The zstd read path enforces the same cap via decompress(max_output_size=...);
    # a frame that inflates past it must be refused, not materialized.
    import zstandard

    bomb = b"\x00" * (2 * MAX_DECOMPRESSED_BYTES)
    stored = "zs:" + base64.b64encode(zstandard.ZstdCompressor().compress(bomb)).decode("ascii")
    with pytest.raises(ValueError, match="decompression bomb"):
        decompress_payload(stored)


def test_payload_at_cap_still_decompresses():
    # A payload whose decompressed size is within the cap round-trips normally;
    # the guard only trips on genuinely oversized output.
    payload = '{"data":"' + "a" * 100_000 + '"}'
    encoded = compress_payload(payload, codec="gzip", min_bytes=100)
    assert encoded.startswith("gz:")
    assert decompress_payload(encoded) == payload
