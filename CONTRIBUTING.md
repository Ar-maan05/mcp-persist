# Contributing to mcp-persist

Thanks for your interest in improving `mcp-persist`. This project provides
persistence backends (`RedisEventStore`, `SQLiteEventStore`) for the MCP Python
SDK's `EventStore` interface.

## Development setup

The project uses [uv](https://docs.astral.sh/uv/). One command installs the
package with both backend extras and all dev tooling:

```bash
uv sync --all-extras --dev
```

That's everything — no separate venv or `pip install` step needed. Python 3.10+
is required.

## Running the checks

CI runs these four checks on Python 3.10–3.13, and they must pass before a PR
can merge. Run them locally first:

```bash
uv run ruff check .          # lint
uv run ruff format --check . # formatting (run `ruff format .` to fix)
uv run pyright src/          # static type checking
uv run pytest tests/         # tests
```

Code style is enforced by `ruff format`; please run it rather than hand-tuning
whitespace.

## Testing against a real Redis

The test suite runs against [`fakeredis`](https://github.com/cunla/fakeredis-py)
by default, so no Redis server is needed for normal development. To exercise the
Redis backend against a real server, set `MCP_TEST_REDIS_URL`:

```bash
# start a throwaway Redis (any of these)
docker run --rm -d -p 6379:6379 redis:7

MCP_TEST_REDIS_URL=redis://localhost:6379/0 uv run pytest tests/
```

The suite calls `FLUSHDB` around every test, so it **refuses to run against a
non-empty database** — always point `MCP_TEST_REDIS_URL` at an empty, throwaway
DB, never a real one. CI runs the suite both ways automatically.

When adding behavior to a backend, please cover it with a test. The Redis tests
should pass under both `fakeredis` and a real server.

## Submitting changes

1. Open a pull request against `main`.
2. CI must be green (lint, format, types, and tests across all four Python
   versions, against both fakeredis and a real Redis) before it can merge.
3. A maintainer will review. Keep PRs focused — one logical change per PR makes
   review faster.
