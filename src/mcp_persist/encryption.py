"""Optional payload encryption at rest for mcp-persist event stores.

Every backend stores the serialized ``JSONRPCMessage`` as text. Those payloads can
carry tool arguments and results, so a deployment that shares a Redis or Postgres
with other services (or simply must encrypt data at rest for compliance) wants the
payload unreadable to anything holding only the backing store. Passing a
:class:`KeyRing` to a store encrypts every payload with AES-256-GCM before it is
written, and the read path transparently decrypts it.

Encryption composes with compression and is the **outer** layer:

* On write the payload is compressed first (``gz:`` / ``zs:``), then encrypted, so
  the codec sees plaintext (ciphertext does not compress) and the stored form is
  ``en:<key_id>:<base64(nonce + ciphertext)>``.
* On read it is decrypted first, yielding the inner (possibly compressed) payload,
  which :func:`~mcp_persist.compression.decompress_payload` then handles by its own
  marker. The two codecs nest without either knowing about the other.

The on-the-wire form is marker-prefixed exactly like compression, for the same
reasons:

* A serialized ``JSONRPCMessage`` is always a JSON object starting with ``{`` and
  priming events are the empty string ``""``, so neither collides with the
  :data:`_ENC_PREFIX` marker.
* :func:`decrypt_payload` keys off that marker, so a store recognizes an encrypted
  payload even with encryption otherwise unconfigured, and **fails closed**
  (raising) rather than handing ciphertext to JSON validation.
* The ``key_id`` is embedded in the marker, so writers holding different keys
  coexist. Rotating a key is the same zero-downtime story as the gzip/zstd
  rollout: add the new key, point new writes at it, and old events stay readable
  under their original key until they age out via ttl/retention.

AES-GCM is authenticated, so a payload tampered with by something with direct
write access to the store fails decryption (``InvalidTag``) instead of decrypting
to silently wrong bytes. Authenticating the bytes is not enough on its own,
though: a valid ciphertext copied from one row into another would still decrypt
cleanly, handing one stream (or one tenant sharing the key) another's messages.
So a payload is also bound to where it is stored. Events carry their stream id
and records their record id as GCM associated data, written with the
``ea:<key_id>:`` marker, and a copy moved anywhere else fails authentication
like any other tamper. Payloads written before this (``en:``) carry no binding
and stay readable; they age out through ttl/retention as usual.

A reader older than 2.1.1 does not know the ``ea:`` marker. For a rolling
upgrade across that boundary, set ``bind_context=False`` (or
``MCP_PERSIST_ENCRYPTION_BIND_CONTEXT=0``) until every reader is upgraded, then
turn it back on. Because GCM ciphertext is not length-expanding and the
inner :func:`~mcp_persist.compression.decompress_payload` still enforces its
decompression-bomb cap, layering encryption over compression adds no new
unbounded-allocation surface.

Requires the ``crypto`` extra (``pip install "mcp-persist[crypto]"``); the
``cryptography`` import happens lazily inside the functions, so the package loads
without it.
"""

from __future__ import annotations

import base64
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

# Marker prefixing an encrypted payload. See the module docstring for why this
# can never collide with a real (plaintext) payload.
_ENC_PREFIX = "en:"

# Marker for a payload encrypted with its storage location as GCM associated
# data. Same shape as _ENC_PREFIX; decrypting it requires that same context.
_BOUND_PREFIX = "ea:"

# AES-GCM standard nonce length. A fresh random nonce is generated per write; at
# 96 bits the chance of a collision under a single key is negligible for the event
# volumes a session store sees, and a unique nonce is all GCM requires.
_NONCE_BYTES = 12

# AES-256 key length in bytes. Keys are supplied as base64 of exactly this many
# raw bytes.
_KEY_BYTES = 32


class KeyRing:
    """A set of AES-256 keys with one designated active key for writes.

    Holds ``key_id -> raw 32-byte key`` and the id of the key new writes use. Reads
    look the key up by the id embedded in a payload's marker, so a retired key
    stays decryptable as long as it remains on the ring, which is what makes
    rotation seamless.

    Construct one directly, or use :func:`keyring_from_env` to read it from the
    environment. :func:`generate_key` produces a fresh base64 key for either.
    """

    __slots__ = ("_keys", "_active_key_id", "bind_context")

    def __init__(self, keys: dict[str, bytes], active_key_id: str, *, bind_context: bool = True) -> None:
        if not keys:
            raise ValueError("KeyRing requires at least one key")
        for key_id, raw in keys.items():
            if not key_id:
                raise ValueError("key id must be a non-empty string")
            if ":" in key_id:
                raise ValueError(f"key id {key_id!r} may not contain ':' (the marker delimiter)")
            if len(raw) != _KEY_BYTES:
                raise ValueError(f"key {key_id!r} must be {_KEY_BYTES} bytes (AES-256), got {len(raw)}")
        if active_key_id not in keys:
            raise ValueError(f"active_key_id {active_key_id!r} is not one of the supplied keys {sorted(keys)}")
        self._keys = dict(keys)
        self._active_key_id = active_key_id
        # Whether new writes are bound to where they are stored (see the module
        # docstring). Reads accept both forms either way.
        self.bind_context = bind_context

    @property
    def active_key_id(self) -> str:
        """The id of the key new writes encrypt with."""
        return self._active_key_id

    @property
    def key_ids(self) -> tuple[str, ...]:
        """Every key id this ring can decrypt with, sorted.

        Key ids are not secret (they are stored in the payload marker), so this
        is safe to print; the keys themselves are never exposed as a group.
        """
        return tuple(sorted(self._keys))

    def active(self) -> tuple[str, bytes]:
        """Return ``(key_id, raw_key)`` for the key new writes encrypt with."""
        return self._active_key_id, self._keys[self._active_key_id]

    def get(self, key_id: str) -> bytes:
        """Return the raw key for ``key_id``; raise ``ValueError`` if absent."""
        try:
            return self._keys[key_id]
        except KeyError:
            raise ValueError(
                f"no encryption key for id {key_id!r}; it was rotated out of the keyring or the "
                "keyring is misconfigured. Add the key back to read events written under it."
            ) from None


def generate_key() -> str:
    """Return a fresh, random AES-256 key as a base64 string for env/config use."""
    return base64.b64encode(os.urandom(_KEY_BYTES)).decode("ascii")


def _decode_key(value: str, *, key_id: str) -> bytes:
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"encryption key {key_id!r} is not valid base64") from exc
    if len(raw) != _KEY_BYTES:
        raise ValueError(
            f"encryption key {key_id!r} must decode to {_KEY_BYTES} bytes (AES-256), got {len(raw)}; "
            "generate one with mcp_persist.encryption.generate_key()"
        )
    return raw


def event_context(stream_id: str) -> str:
    """The associated data binding an event payload to its stream."""
    return f"event\x00{stream_id}"


def record_context(record_id: str) -> str:
    """The associated data binding a record payload to its record."""
    return f"record\x00{record_id}"


def encrypt_payload(payload: str, *, keyring: KeyRing | None, context: str | None = None) -> str:
    """Encrypt an (already-compressed) payload, or pass it through unchanged.

    Returns ``payload`` untouched when ``keyring`` is ``None`` or ``payload`` is the
    empty string (priming events carry no body and stay trivially recognizable).
    Otherwise returns ``ea:<key_id>:<base64(nonce + ciphertext)>`` with
    ``context`` (see :func:`event_context`) authenticated as associated data, or
    the unbound ``en:`` form when no context is given or the keyring has
    ``bind_context`` off. Unlike compression there is no size threshold:
    confidentiality is all-or-nothing.
    """
    if keyring is None or not payload:
        return payload
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key_id, key = keyring.active()
    bound = context is not None and keyring.bind_context
    nonce = os.urandom(_NONCE_BYTES)
    aad = context.encode("utf-8") if bound and context is not None else None
    ciphertext = AESGCM(key).encrypt(nonce, payload.encode("utf-8"), aad)
    encoded = base64.b64encode(nonce + ciphertext).decode("ascii")
    return f"{_BOUND_PREFIX if bound else _ENC_PREFIX}{key_id}:{encoded}"


def decrypt_payload(stored: str, *, keyring: KeyRing | None, context: str | None = None) -> str:
    """Inverse of :func:`encrypt_payload`; non-encrypted payloads pass through.

    Decoding is driven entirely by the marker, so this is safe to call on any
    stored payload. An encrypted payload encountered without a keyring (or without
    the key that wrote it) raises ``ValueError`` rather than returning ciphertext:
    the store fails closed instead of feeding garbage to JSON validation. A
    tampered payload fails GCM authentication and also raises, and so does a bound
    (``ea:``) payload read with any ``context`` other than the one it was written
    with, which is what a payload copied to another row looks like.
    """
    bound = stored.startswith(_BOUND_PREFIX)
    if not bound and not stored.startswith(_ENC_PREFIX):
        return stored
    if bound and context is None:
        raise ValueError("encrypted payload is bound to its storage location, but no context was supplied to check it")
    if keyring is None:
        raise ValueError(
            "encountered an encrypted payload (en: marker) but no encryption keyring is configured; "
            "set one so the store can decrypt it"
        )
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    body = stored[len(_ENC_PREFIX) :]  # both markers are the same length
    key_id, sep, b64 = body.partition(":")
    if not sep:
        raise ValueError("malformed encrypted payload: missing key id")
    key = keyring.get(key_id)
    blob = base64.b64decode(b64)
    nonce, ciphertext = blob[:_NONCE_BYTES], blob[_NONCE_BYTES:]
    aad = context.encode("utf-8") if bound and context is not None else None
    plaintext = AESGCM(key).decrypt(nonce, ciphertext, aad)
    return plaintext.decode("utf-8")


def keyring_from_env(env: Mapping[str, str] | None = None) -> KeyRing | None:
    """Build a :class:`KeyRing` from ``MCP_PERSIST_ENCRYPTION_*`` vars, or ``None``.

    Reads, in order of precedence:

    * ``MCP_PERSIST_ENCRYPTION_KEYS`` ``"id1:<b64>,id2:<b64>"`` plus
      ``MCP_PERSIST_ENCRYPTION_KEY_ID`` naming the active key (required when more
      than one key is listed; defaults to the sole key otherwise). Use this form to
      rotate keys.
    * ``MCP_PERSIST_ENCRYPTION_KEY`` ``"<b64>"``, a single key bound to the id
      ``"default"``. A convenience for the common single-key deployment.

    ``MCP_PERSIST_ENCRYPTION_BIND_CONTEXT`` (default on) sets the keyring's
    ``bind_context``; turn it off only for a rolling upgrade from before 2.1.1.

    Returns ``None`` when none of these is set, so encryption stays opt-in. Raises
    ``ValueError`` for a malformed key, a missing/ambiguous active id, or both
    forms set at once.
    """
    source: Mapping[str, str] = os.environ if env is None else env

    keys_raw = source.get("MCP_PERSIST_ENCRYPTION_KEYS") or None
    key_raw = source.get("MCP_PERSIST_ENCRYPTION_KEY") or None
    active_id = source.get("MCP_PERSIST_ENCRYPTION_KEY_ID") or None

    if keys_raw is None and key_raw is None:
        return None
    if keys_raw is not None and key_raw is not None:
        raise ValueError(
            "set either MCP_PERSIST_ENCRYPTION_KEY (single key) or MCP_PERSIST_ENCRYPTION_KEYS "
            "(rotation list), not both"
        )

    bind_context = _bind_context_from_env(source)
    if key_raw is not None:
        key_id = active_id or "default"
        return KeyRing({key_id: _decode_key(key_raw, key_id=key_id)}, active_key_id=key_id, bind_context=bind_context)

    keys: dict[str, bytes] = {}
    assert keys_raw is not None
    for entry in keys_raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        key_id, sep, value = entry.partition(":")
        if not sep:
            raise ValueError(f"MCP_PERSIST_ENCRYPTION_KEYS entry {entry!r} must be 'key_id:base64key'")
        key_id = key_id.strip()
        keys[key_id] = _decode_key(value.strip(), key_id=key_id)
    if not keys:
        raise ValueError("MCP_PERSIST_ENCRYPTION_KEYS was set but listed no keys")

    if active_id is None:
        if len(keys) > 1:
            raise ValueError(
                "MCP_PERSIST_ENCRYPTION_KEYS lists multiple keys; set MCP_PERSIST_ENCRYPTION_KEY_ID "
                "to pick the one new writes use"
            )
        active_id = next(iter(keys))
    return KeyRing(keys, active_key_id=active_id, bind_context=bind_context)


def _bind_context_from_env(source: Mapping[str, str]) -> bool:
    raw = (source.get("MCP_PERSIST_ENCRYPTION_BIND_CONTEXT") or "").strip().lower()
    if raw in ("", "1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"MCP_PERSIST_ENCRYPTION_BIND_CONTEXT must be a boolean, got {raw!r}")
