# pyright: reportArgumentType=false
# pyright: reportPrivateUsage=false
"""Tests for payload encryption at rest (the encryption codec + store wiring).

The codec-level tests are pure and need only ``cryptography`` (the ``crypto``
extra, installed in dev). The store-integration tests run against fakeredis and an
in-memory SQLite database, so no external service is required.
"""

from __future__ import annotations

import base64
import os

import fakeredis.aioredis as fakeredis
import pytest
from mcp.types import JSONRPCRequest

from mcp_persist import KeyRing, RedisEventStore, SQLiteEventStore, generate_key, keyring_from_env
from mcp_persist.compression import compress_payload, decompress_payload
from mcp_persist.encryption import _ENC_PREFIX, decrypt_payload, encrypt_payload

SAMPLE_MSG = JSONRPCRequest(jsonrpc="2.0", id="1", method="tools/list")


def _key() -> bytes:
    return os.urandom(32)


def _ring(active: str = "k1", **extra: bytes) -> KeyRing:
    keys = {active: _key(), **extra}
    return KeyRing(keys, active_key_id=active)


# ── KeyRing construction ────────────────────────────────────────────────────


def test_keyring_rejects_empty():
    with pytest.raises(ValueError, match="at least one key"):
        KeyRing({}, active_key_id="k1")


def test_keyring_rejects_wrong_length_key():
    with pytest.raises(ValueError, match="32 bytes"):
        KeyRing({"k1": b"too-short"}, active_key_id="k1")


def test_keyring_rejects_colon_in_id():
    with pytest.raises(ValueError, match="may not contain"):
        KeyRing({"a:b": _key()}, active_key_id="a:b")


def test_keyring_rejects_unknown_active_id():
    with pytest.raises(ValueError, match="not one of"):
        KeyRing({"k1": _key()}, active_key_id="k2")


def test_keyring_get_missing_key_raises():
    ring = _ring()
    with pytest.raises(ValueError, match="no encryption key for id"):
        ring.get("nope")


def test_generate_key_is_32_bytes_base64():
    raw = base64.b64decode(generate_key())
    assert len(raw) == 32


# ── Codec round-trip and marker behavior ────────────────────────────────────


def test_roundtrip():
    ring = _ring()
    payload = '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
    enc = encrypt_payload(payload, keyring=ring)
    assert enc.startswith(f"{_ENC_PREFIX}k1:")
    assert enc != payload
    assert decrypt_payload(enc, keyring=ring) == payload


def test_empty_payload_passes_through():
    # Priming events carry no body and must stay the empty string under both ops.
    ring = _ring()
    assert encrypt_payload("", keyring=ring) == ""
    assert decrypt_payload("", keyring=ring) == ""


def test_no_keyring_is_a_passthrough():
    assert encrypt_payload("{}", keyring=None) == "{}"
    assert decrypt_payload("{}", keyring=None) == "{}"


def test_nonce_is_random_per_call():
    # Two encryptions of the same plaintext differ (fresh nonce), both decrypt back.
    ring = _ring()
    a = encrypt_payload("{}", keyring=ring)
    b = encrypt_payload("{}", keyring=ring)
    assert a != b
    assert decrypt_payload(a, keyring=ring) == decrypt_payload(b, keyring=ring) == "{}"


def test_plain_payload_decrypts_to_itself():
    # A value without the marker is returned untouched, so a keyring reads rows
    # written before encryption was enabled.
    ring = _ring()
    assert decrypt_payload('{"plain":true}', keyring=ring) == '{"plain":true}'


# ── Fail-closed and tamper detection ────────────────────────────────────────


def test_encrypted_payload_without_keyring_fails_closed():
    ring = _ring()
    enc = encrypt_payload("{}", keyring=ring)
    with pytest.raises(ValueError, match="no encryption keyring is configured"):
        decrypt_payload(enc, keyring=None)


def test_encrypted_payload_with_wrong_key_id_raises():
    enc = encrypt_payload("{}", keyring=_ring("k1"))
    other = _ring("k2")  # does not contain k1
    with pytest.raises(ValueError, match="no encryption key for id 'k1'"):
        decrypt_payload(enc, keyring=other)


def test_tampered_ciphertext_is_rejected():
    from cryptography.exceptions import InvalidTag

    ring = _ring()
    enc = encrypt_payload('{"a":1}', keyring=ring)
    prefix, _, b64 = enc.partition("k1:")
    blob = bytearray(base64.b64decode(b64))
    blob[-1] ^= 0x01  # flip a bit in the auth tag
    tampered = f"{prefix}k1:{base64.b64encode(bytes(blob)).decode()}"
    with pytest.raises(InvalidTag):
        decrypt_payload(tampered, keyring=ring)


def test_malformed_marker_without_key_id_raises():
    with pytest.raises(ValueError, match="missing key id"):
        decrypt_payload("en:no-delimiter-here", keyring=_ring())


# ── Composition with compression (encrypt is the outer layer) ───────────────


def test_compose_with_compression_roundtrip():
    # Build the same order the stores use: compress then encrypt; decrypt then
    # decompress. A large, compressible payload so gzip actually triggers.
    ring = _ring()
    payload = '{"data":"' + ("x" * 5000) + '"}'
    compressed = compress_payload(payload, codec="gzip", min_bytes=1024)
    assert compressed.startswith("gz:")
    stored = encrypt_payload(compressed, keyring=ring)
    assert stored.startswith(f"{_ENC_PREFIX}k1:")

    # Reader: decrypt yields the gz: payload, decompress yields the original.
    inner = decrypt_payload(stored, keyring=ring)
    assert inner == compressed
    assert decompress_payload(inner) == payload


# ── Key rotation ────────────────────────────────────────────────────────────


def test_rotation_old_key_still_decrypts():
    # Encrypt under k1, then build a ring whose active key is k2 but which still
    # holds k1. The old ciphertext stays readable; new writes use k2.
    k1 = _key()
    ring_v1 = KeyRing({"k1": k1}, active_key_id="k1")
    old = encrypt_payload('{"v":1}', keyring=ring_v1)

    ring_v2 = KeyRing({"k1": k1, "k2": _key()}, active_key_id="k2")
    assert decrypt_payload(old, keyring=ring_v2) == '{"v":1}'
    new = encrypt_payload('{"v":2}', keyring=ring_v2)
    assert new.startswith(f"{_ENC_PREFIX}k2:")
    assert decrypt_payload(new, keyring=ring_v2) == '{"v":2}'


# ── keyring_from_env ────────────────────────────────────────────────────────


def test_env_returns_none_when_unset():
    assert keyring_from_env({}) is None


def test_env_single_key():
    ring = keyring_from_env({"MCP_PERSIST_ENCRYPTION_KEY": generate_key()})
    assert ring is not None
    assert ring.active()[0] == "default"


def test_env_single_key_with_custom_id():
    ring = keyring_from_env(
        {"MCP_PERSIST_ENCRYPTION_KEY": generate_key(), "MCP_PERSIST_ENCRYPTION_KEY_ID": "primary"}
    )
    assert ring is not None
    assert ring.active()[0] == "primary"


def test_env_multi_key_requires_active_id():
    env = {"MCP_PERSIST_ENCRYPTION_KEYS": f"k1:{generate_key()},k2:{generate_key()}"}
    with pytest.raises(ValueError, match="set MCP_PERSIST_ENCRYPTION_KEY_ID"):
        keyring_from_env(env)


def test_env_multi_key_with_active_id():
    env = {
        "MCP_PERSIST_ENCRYPTION_KEYS": f"k1:{generate_key()},k2:{generate_key()}",
        "MCP_PERSIST_ENCRYPTION_KEY_ID": "k2",
    }
    ring = keyring_from_env(env)
    assert ring is not None
    assert ring.active()[0] == "k2"


def test_env_single_entry_keys_defaults_active():
    ring = keyring_from_env({"MCP_PERSIST_ENCRYPTION_KEYS": f"only:{generate_key()}"})
    assert ring is not None
    assert ring.active()[0] == "only"


def test_env_rejects_both_forms():
    env = {
        "MCP_PERSIST_ENCRYPTION_KEY": generate_key(),
        "MCP_PERSIST_ENCRYPTION_KEYS": f"k1:{generate_key()}",
    }
    with pytest.raises(ValueError, match="not both"):
        keyring_from_env(env)


def test_env_rejects_bad_base64_length():
    with pytest.raises(ValueError, match="must decode to 32 bytes"):
        keyring_from_env({"MCP_PERSIST_ENCRYPTION_KEY": base64.b64encode(b"short").decode()})


def test_env_rejects_entry_without_colon():
    with pytest.raises(ValueError, match="must be 'key_id:base64key'"):
        keyring_from_env({"MCP_PERSIST_ENCRYPTION_KEYS": "no-colon-value"})


# ── Store integration: SQLite ───────────────────────────────────────────────


async def _replay(store, last_event_id, stream_id):
    events = []

    async def cb(ev):
        events.append(ev)

    resolved = await store.replay_events_after(last_event_id, cb, stream_id=stream_id)
    return resolved, events


@pytest.mark.anyio
async def test_sqlite_encrypts_at_rest_and_replays():
    ring = _ring()
    async with SQLiteEventStore.create(":memory:", ttl=60, keyring=ring) as store:
        e1 = await store.store_event("s1", SAMPLE_MSG)
        await store.store_event("s1", SAMPLE_MSG)

        # The raw column holds ciphertext, not the JSON payload.
        async with store._conn.execute(
            f"SELECT payload FROM {store._table} WHERE event_id = ?", (int(e1),)
        ) as cur:
            raw = (await cur.fetchone())[0]
        assert raw.startswith(_ENC_PREFIX)
        assert "tools/list" not in raw

        # Replay decrypts transparently.
        resolved, events = await _replay(store, e1, "s1")
        assert resolved == "s1"
        assert [e.message.root.method for e in events] == ["tools/list"]


@pytest.mark.anyio
async def test_sqlite_keyring_reads_plaintext_rows():
    # A store written without encryption stays readable once a keyring is added,
    # because decryption is marker-driven.
    ring = _ring()
    async with SQLiteEventStore.create(":memory:", ttl=60) as plain:
        await plain.store_event("s1", SAMPLE_MSG)
        # Reuse the same connection under an encrypting store.
        encrypting = SQLiteEventStore(plain._conn, ttl=60, keyring=ring)
        e2 = await encrypting.store_event("s1", SAMPLE_MSG)
        resolved, events = await _replay(encrypting, "0", "s1")
        assert resolved == "s1"
        assert len(events) == 2  # the plaintext row and the encrypted row
        assert e2 == "2"


@pytest.mark.anyio
async def test_sqlite_without_keyring_never_returns_ciphertext(caplog):
    import logging

    ring = _ring()
    async with SQLiteEventStore.create(":memory:", ttl=60, keyring=ring) as enc_store:
        e1 = await enc_store.store_event("s1", SAMPLE_MSG)
        await enc_store.store_event("s1", SAMPLE_MSG)
        # A second store on the same connection, but with no keyring, cannot read
        # the encrypted rows. The replay loop's poison-payload guard skips them
        # (logging a warning) rather than ever handing back ciphertext.
        plain = SQLiteEventStore(enc_store._conn, ttl=60)
        with caplog.at_level(logging.WARNING, logger="mcp_persist.sqlite"):
            resolved, events = await _replay(plain, e1, "s1")
        assert resolved == "s1"
        assert events == []  # the one later encrypted row was skipped, not returned
        assert "no encryption keyring is configured" in caplog.text


# ── Store integration: Redis ────────────────────────────────────────────────


@pytest.mark.anyio
async def test_redis_encrypts_at_rest_and_replays():
    ring = _ring()
    client = fakeredis.FakeRedis()
    store = RedisEventStore(client, ttl=60, keyring=ring)
    e1 = await store.store_event("s1", SAMPLE_MSG)
    await store.store_event("s1", SAMPLE_MSG)

    raw = await client.hget("mcp:event:1", "payload")
    assert raw.decode().startswith(_ENC_PREFIX)
    assert b"tools/list" not in raw

    resolved, events = await _replay(store, e1, "s1")
    assert resolved == "s1"
    assert [e.message.root.method for e in events] == ["tools/list"]


@pytest.mark.anyio
async def test_redis_encryption_works_on_both_write_paths():
    # The scripted and pipelined write paths must both encrypt and round-trip.
    ring = _ring()
    for force_pipeline in (False, True):
        store = RedisEventStore(fakeredis.FakeRedis(), ttl=60, keyring=ring)
        if force_pipeline:
            store._script_ok = False
        e1 = await store.store_event("s1", SAMPLE_MSG)
        await store.store_event("s1", SAMPLE_MSG)
        resolved, events = await _replay(store, e1, "s1")
        assert resolved == "s1"
        assert [e.message.root.method for e in events] == ["tools/list"]
