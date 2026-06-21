# Tiered storage

The default retention story is deletion: `PurgeScheduler` calls `purge_expired()`
and old events are gone. Tiered storage is the alternative: **archive** events
past their ttl into cold storage instead of dropping them, keeping recent events
in a fast hot store for resumption while moving the long tail somewhere cheaper.

A typical layout pairs a fast hot store (Redis, or a small SQLite/Postgres) with a
durable cold store (a larger Postgres, an S3-backed table, any `ttl=None` store).

## Background archival: `ArchiveScheduler`

`ArchiveScheduler` runs the archive loop for you, the way `PurgeScheduler` runs
the purge loop:

```python
from mcp_persist import ArchiveScheduler

async with ArchiveScheduler(hot, cold, interval=300, batch_size=500):
    async with manager.run():
        yield  # every 300s, expired batches move hot -> cold
```

Each cycle, for every batch up to `batch_size`:

1. read expired events from the hot store (`select_expired`),
2. ID-preserving insert into the cold store (`_store_event_raw`, an upsert),
3. delete that exact batch from the hot store.

The order is **archive then delete**, so a crash mid-cycle can leave duplicates in
cold (harmless: the cold insert is an upsert keyed on `event_id`) but never loses
data from hot before it is safely archived. The loop drains the whole expired
backlog each cycle, so a store accumulating more than `batch_size` expired events
per interval still catches up rather than falling permanently behind.

`ArchiveScheduler` requires a hot store with a positive `ttl` and `select_expired`
(SQLite or Postgres) and a cold store with ID-preserving inserts. Use `ttl=None`
on a Redis cold store so archived events are not re-expired by Redis.

## Resuming across tiers: `ChainedEventStore`

Archival alone moves events to cold storage but does not make them resumable. Wrap
the two stores in a `ChainedEventStore` so a reconnecting client can resume from
either tier:

```python
from mcp_persist import ChainedEventStore

store = ChainedEventStore(hot=hot, cold=cold)
# hand `store` to StreamableHTTPSessionManager / with_persistence / the proxy
```

New events are written to the hot store only. On `replay_events_after`, the chain
resolves the `Last-Event-ID` in hot first; if it is absent from hot but present in
cold (the usual case after archival), it replays from cold and then continues from
hot on the same stream, in monotonic `event_id` order. Because cold storage
preserves the original `event_id`, resumability tokens stay valid across the move.

## Lower-level helpers

If you want a custom loop instead of `ArchiveScheduler`:

```python
from mcp_persist import archive_expired_batch, count_expired

pending = await count_expired(hot)           # how many events are past ttl
moved = await archive_expired_batch(hot, cold, batch_size=500)  # move one batch
```

## Composition

- **Multi-tenancy**: a tenant-bound cold store tags archived rows with its tenant,
  so isolation holds across both tiers (see [multi-tenancy.md](multi-tenancy.md)).
- **Compression**: archived payloads are copied verbatim, so a payload compressed
  in hot stays compressed in cold and is read back transparently.
- **Purge vs archive**: use `PurgeScheduler` when expired events can be dropped,
  `ArchiveScheduler` when they must be retained. Do not run both against the same
  hot store, since purge would delete the events archival means to move.
