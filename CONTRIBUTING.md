# Contributing to Ferry

Thanks for considering a contribution! Ferry is a small, carefully-built project — quality over quantity in everything, including PRs.

## Getting started

```bash
git clone https://github.com/Sanjays2402/ferry.git
cd ferry
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,redis]"
pytest
```

The full suite should be green before you start: 57 tests, a few seconds.

## What makes a good contribution

- **Bug fixes with a regression test.** If you found it, prove it: a failing test first, then the fix.
- **Small, focused PRs.** One change per PR, with a clear description of *why*.
- **Docs with features.** A feature without README/docs updates is half a feature.

Out of scope without prior discussion: new broker backends, result backends beyond the broker, and anything that adds a required dependency to the core install (core stays dependency-free).

## Code style

- `ruff check ferry tests benchmarks` must pass (config in `pyproject.toml`).
- Target Python 3.10+; keep type annotations accurate.
- No `print()` debugging left behind; use the `logging` module already in place.

## How it works (the 30-second version)

Producers serialize tasks into the broker (`SQLiteBroker` / `RedisBroker`). Workers atomically claim tasks — SQLite via `UPDATE … RETURNING`, Redis via a Lua script — execute them, then ack done/failed. `Beat` handles cron schedules (one per broker; dedup by `scheduled_id`). The dashboard reads everything through the same broker API, so any new broker gets the UI for free.

If you're adding a broker, implement every method on `SQLiteBroker` and mirror `tests/test_broker.py` + `tests/test_redis_broker.py` for it.

## Reporting issues

Use the issue templates. Include: Ferry version, broker URL scheme (`sqlite://` / `redis://`), Python version, a minimal repro, and the full traceback. "It doesn't work" will get a polite request for a repro.

## License

By contributing, you agree your contributions are licensed under the MIT License.
