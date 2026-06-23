# Per-Team Retention Policies with Audit Logging

Compliance requirements often demand that event data be purged after a set period, and that the purge is accompanied by a reliable, tamper-evident audit trail.

`mcp-persist` supports per-tenant retention windows managed by a central scheduler. Each time a purge runs, it captures details of the deletion and writes them to a pluggable audit sink.

## Key Concepts

* **Per-Tenant Windows**: Different teams (tenants) can have different retention windows (expressed in seconds).
* **Pluggable Audit Trail**: When rows are deleted, a summary entry containing the timestamp, tenant, window applied, and count of deleted events is written to a designated sink.
* **Strict Audit Handling**: In strict mode, any failure to record the audit log raises an error to halt operation, preventing silent compliance lapses.

> [!IMPORTANT]
> **Redis is not supported**: Redis key retention relies on native key-level TTL set at write time. It does not support retroactive, per-tenant window updates or multi-tenant sweeps. Use SQLite or Postgres backends for per-team retention policies.

## Quickstart

The fastest way to apply retention is to wire a `RetentionPolicy` and a `DatabaseAuditSink` into a `RetentionScheduler` alongside your event store:

```python
import asyncio
from mcp_persist import SQLiteEventStore, RetentionPolicy, DatabaseAuditSink, RetentionScheduler

async def main():
    async with SQLiteEventStore.create("events.db") as store:
        # Define retention windows (in seconds) for each team
        policy = RetentionPolicy(
            windows={
                "team-a": 86400,     # 1 day
                "team-b": 604800,    # 7 days
                None: 3600,          # untenanted events (1 hour)
            },
            default=172800           # default for other teams (2 days)
        )

        # Audit logs will be stored in "mcp_events_retention_audit"
        sink = DatabaseAuditSink(store)

        # Run the scheduler to check and purge every 5 minutes (300 seconds)
        async with RetentionScheduler(store, policy, sink, interval=300.0):
            # Keep your application or session manager running here
            await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
```

## Compliance and Security

### Append-Only Database Audit Sink
The default `DatabaseAuditSink` writes audit rows into a dedicated database table (e.g., `mcp_events_retention_audit`). To ensure tamper evidence:
* Callers should configure database permissions so the application role only has `INSERT` and `SELECT` privileges on the audit table.
* The application should not have `UPDATE` or `DELETE` access to this table.
* For absolute compliance, stream these records to an external write-once-read-many (WORM) storage system or append-only log manager.

### Strict vs Best-Effort Audit
The `RetentionScheduler` constructor accepts a `strict_audit` boolean flag:
* `strict_audit=True` (default): If the audit sink raises an exception, the scheduler logs an ERROR and propagates the exception to halt operations. This ensures that deletions are never unrecorded.
* `strict_audit=False`: If the audit sink fails, the error is logged and swallowed, continuing to the next tenant.

## Configuration via Environment

You can configure your retention policy using environment variables via the `retention_policy_from_env` helper:

* `MCP_PERSIST_RETENTION_WINDOWS`: A JSON object mapping tenant IDs to integer seconds. For example, `{"team-a": 86400, "team-b": 3600}`.
  * The special key `null` maps to untenanted (`None`) events.
  * The special key `__default__` maps to the default window.
* `MCP_PERSIST_RETENTION_DEFAULT`: An integer specifying the default retention window in seconds. If both `__default__` in JSON and this variable are set, they must match.

### Example Environment Configuration

```bash
export MCP_PERSIST_RETENTION_WINDOWS='{"team-a": 86400, "team-b": 3600, "__default__": 172800}'
```

```python
from mcp_persist import retention_policy_from_env

policy = retention_policy_from_env()
```
