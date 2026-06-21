# Multi-tenancy

When one backend serves more than one customer (an OpenRouter-style platform
routing traffic for many tenants), event streams must be isolated per tenant:
one customer must never resume from, list, or purge another customer's events.
mcp-persist supports this with a `tenant_id` bound at store construction.

## Binding a store to a tenant

Pass `tenant_id` to any backend (or `MCP_PERSIST_TENANT_ID` via
`event_store_from_env`):

```python
from mcp_persist import RedisEventStore, PostgresEventStore, SQLiteEventStore

acme   = PostgresEventStore(pool, ttl=3600, tenant_id="acme")
globex = PostgresEventStore(pool, ttl=3600, tenant_id="globex")
```

A tenant-bound store scopes **every** operation to its own rows: `store_event`,
`replay_events_after`, `list_streams`, `purge_expired`, `count_expired`,
`select_expired`, and the migration/archival helpers. Two stores can share one
pool, one Redis, or one database table and stay fully isolated.

An **unbound** store (`tenant_id=None`, the default) is unscoped: it sees every
tenant's events. That is the right choice for a single-tenant deployment and for
an operator/admin view (for example a global `mcp-persist stats`). It is also
fully backward compatible: existing single-tenant deployments are unchanged.

## How isolation is implemented

- **Redis** folds the tenant into the key prefix: `{key_prefix}{tenant}:...`.
  Different tenants occupy disjoint key spaces, so isolation is structural and a
  cross-tenant `Last-Event-ID` simply resolves to nothing.
- **SQLite and Postgres** add a nullable `tenant_id` column plus a
  `(tenant_id, stream_id, event_id)` index, and every query carries a
  `tenant_id = ?` predicate. A table created by an older version is migrated in
  place on first open (`ALTER TABLE ... ADD COLUMN`, run once and cached), so an
  upgrade needs no manual migration.

Because `event_id` is globally unique within a table, a tenant store will not
resolve another tenant's anchor even before the tenant predicate is applied; the
predicate makes listing, purging, and counting tenant-scoped as well.

## Notes

- The MCP SDK fixes the `EventStore.store_event` / `replay_events_after`
  signatures, so the tenant is bound at construction rather than passed per call.
  Run one store instance per tenant (sharing the underlying connection pool).
- Per-tenant metrics: pass `tenant_id` to `OTelMetricsCollector` so timings and
  counts are labeled per tenant in your tracing backend.
- Tiered storage composes with tenancy: a tenant-bound cold store tags archived
  rows with the same tenant, so isolation holds across the hot and cold tiers.
