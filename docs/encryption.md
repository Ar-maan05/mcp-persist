# Encryption at rest

Event payloads are serialized `JSONRPCMessage` objects, and they can carry tool
arguments and tool results. When the backing store is shared with other services,
lives on infrastructure you do not fully control, or simply must encrypt data at
rest for compliance, you want those payloads unreadable to anything that holds
only the store. mcp-persist supports this with AES-256-GCM encryption applied
before a payload reaches the backend and removed transparently on read.

Encryption is opt-in and requires the `crypto` extra:

```bash
pip install "mcp-persist[crypto]"
```

## Enabling it

Build a `KeyRing` and pass it as `keyring=` to any backend (constructor or
`create()`):

```python
from mcp_persist import KeyRing, RedisEventStore, generate_key
import base64

# generate_key() returns a fresh base64 AES-256 key; store it as a secret.
ring = KeyRing({"k1": base64.b64decode(generate_key())}, active_key_id="k1")

store = RedisEventStore(redis_client, ttl=3600, keyring=ring)
```

Every backend accepts `keyring=`: `SQLiteEventStore`, `RedisEventStore`,
`PostgresEventStore`, and their `create()` context managers. From that point on,
`store_event` encrypts each payload before writing it and `replay_events_after`
(and `subscribe`, `migrate`, the read side of archival) decrypt it on the way out.
Nothing else in your code changes.

`KeyRing` holds raw 32-byte keys. Generate one with
`mcp_persist.generate_key()` (a base64 string, convenient for secrets managers and
environment variables) and `base64.b64decode` it into the ring, or supply your own
32 bytes from a KMS/HSM.

## From the environment

`event_store_from_env` reads the keyring from `MCP_PERSIST_ENCRYPTION_*`, so a
deployment can turn on encryption without code changes:

```bash
# Single key (bound to the id "default"):
export MCP_PERSIST_ENCRYPTION_KEY="$(python -c 'import mcp_persist; print(mcp_persist.generate_key())')"

# Or a rotation list, naming the key new writes use:
export MCP_PERSIST_ENCRYPTION_KEYS="k1:<base64key>,k2:<base64key>"
export MCP_PERSIST_ENCRYPTION_KEY_ID="k2"
```

| Variable | Meaning |
|---|---|
| `MCP_PERSIST_ENCRYPTION_KEY` | A single base64 AES-256 key, bound to the id `default`. The common case. |
| `MCP_PERSIST_ENCRYPTION_KEYS` | `id:base64,id:base64` list of keys, for rotation. |
| `MCP_PERSIST_ENCRYPTION_KEY_ID` | Which listed key new writes encrypt with. Required when more than one key is listed; defaults to the sole key otherwise. |

Set either `MCP_PERSIST_ENCRYPTION_KEY` or `MCP_PERSIST_ENCRYPTION_KEYS`, not both.
When neither is set, encryption stays off and stores are constructed exactly as
before, so the variables are safe to leave unset.

You can also build the ring directly with `keyring_from_env()` and pass it to a
store yourself.

## On-the-wire form

A stored, encrypted payload looks like:

```
en:<key_id>:<base64( 12-byte nonce + AES-GCM ciphertext and tag )>
```

The `en:` marker and the embedded `key_id` make three properties hold:

- **A reader recognizes ciphertext.** A real serialized `JSONRPCMessage` always
  starts with `{` and a priming event is the empty string, so neither collides
  with `en:`. Decryption is driven entirely by the marker.
- **Plaintext rows stay readable.** A value without the marker is returned
  untouched, so a store that gains a keyring still reads rows written before
  encryption was enabled. Turning encryption on is incremental, exactly like the
  compression rollout.
- **Writers with different keys coexist.** The `key_id` travels with each payload,
  so a reader looks up the right key per row. This is what makes rotation
  seamless.

## Composition with compression

Encryption composes with `compression=` and is the **outer** layer. On write a
payload is compressed first (ciphertext does not compress), then encrypted; on
read it is decrypted first, then decompressed:

```
write:  JSON  ->  gz:/zs: (if compression on)  ->  en:...   (stored)
read:   en:...  ->  gz:/zs: or plain  ->  JSON
```

The two codecs nest without either knowing about the other, so you can run both:

```python
store = PostgresEventStore(pool, ttl=3600, compression="zstd", keyring=ring)
```

Because AES-GCM ciphertext is not length-expanding and the decompression step
still enforces its existing decompression-bomb cap (100 MiB), layering encryption
over compression adds no new unbounded-allocation surface.

## Key rotation

Rotation is zero-downtime because old ciphertext stays decryptable under its
original key:

1. Generate a new key and add it to the ring **alongside** the old one, with the
   new key as active:

   ```python
   ring_v2 = KeyRing({"k1": old_key, "k2": new_key}, active_key_id="k2")
   ```

   Or, from the environment, list both keys and point `MCP_PERSIST_ENCRYPTION_KEY_ID`
   at the new one.

2. New writes are encrypted under `k2`. Events already written under `k1` keep
   decrypting under `k1`, since their marker still names it.

3. Once every `k1` event has aged out (via `ttl`/retention), drop `k1` from the
   ring. Until then, keep it: a store that meets a `k1` payload with no `k1` key
   on the ring cannot read it.

## Behavior when a key is missing

The encryption codec **fails closed**: `decrypt_payload` raises rather than ever
returning ciphertext, whether the keyring is absent or simply lacks the key a
payload names, or the payload was tampered with (AES-GCM authentication fails with
`InvalidTag`). A store never hands ciphertext back to the client.

During replay, that raised error meets the store's existing per-event
poison-payload guard: the undecryptable event is **skipped with a logged warning**
and replay continues with the events it can read, rather than aborting the whole
resume. The practical effect is that a misconfigured or rotated-out key shows up as
a warning and missing events on resume, not a crash and not a ciphertext leak.
After a key change, watch for `no encryption key for id` warnings in the logs:
they mean a key a stored payload names is no longer on the ring.

## Threat model

What encryption at rest protects against, and what it does not:

- **Protects**: an attacker (or a co-tenant service) that can read the backing
  store directly (Redis memory/RDB/AOF, a Postgres table or its backups, a SQLite
  file) sees only ciphertext for event payloads. A party that can write to the
  store cannot forge or silently alter a payload: GCM authentication rejects it on
  read.
- **Does not protect**: stream IDs and event IDs are stored in the clear (they are
  index keys, not payloads), so this is payload confidentiality, not metadata
  confidentiality. It also does not protect data in memory in the running process,
  in transit (use TLS for that), or against an attacker who also holds the key.
- **Key management is yours**: the security of the scheme reduces to the secrecy of
  the keys. Keep them in a secrets manager or KMS, not in source. Losing a key
  means losing the ability to read events written under it.

## API reference

| Symbol | Purpose |
|---|---|
| `KeyRing(keys, active_key_id)` | A set of `key_id -> 32-byte key` with one active key for writes. |
| `KeyRing.active()` | `(key_id, raw_key)` used to encrypt new writes. |
| `KeyRing.get(key_id)` | Raw key for a given id; raises if absent. |
| `generate_key()` | A fresh random AES-256 key as a base64 string. |
| `keyring_from_env(env=None)` | Build a `KeyRing` from `MCP_PERSIST_ENCRYPTION_*`, or `None` if unset. |

The low-level codec (`encrypt_payload` / `decrypt_payload` in
`mcp_persist.encryption`) is used internally by the stores; you rarely call it
directly.
