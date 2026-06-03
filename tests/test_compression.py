"""Tests for the payload compression helpers."""

from __future__ import annotations

import pytest

from mcp_persist.compression import compress_payload, decompress_payload, validate_compression


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
        validate_compression("zstd")
