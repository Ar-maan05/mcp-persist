## What does this change?

<!-- A short description of the change and why it's needed. -->

## Checklist

- [ ] Ran the checks locally: `ruff check .`, `ruff format --check .`, `pyright src/`, `pytest tests/`
- [ ] Added or updated tests for the change
- [ ] For Redis changes: verified the suite still passes against a real server (`MCP_TEST_REDIS_URL=...`)
- [ ] For Postgres changes: ran the suite against a real server (`MCP_TEST_POSTGRES_URL=...`); the Postgres tests are skipped without it
