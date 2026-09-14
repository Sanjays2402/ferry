# Changelog

All notable changes to Ferry are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Task canvases: `app.chain()`, `app.group()`, `app.chord()` with `Task.s()` / `Task.si()` signatures and per-link `.set()` options; `ChainResult`, `GroupResult`, `ChordResult`. Works on both brokers; chord completion uses an atomic barrier so the callback fires exactly once.
- README visuals: architecture diagram, terminal card with real `ferry worker` / `ferry stats` output, and a chain/group/chord diagram (replaces the mermaid sketch).

## [0.1.0] — 2026-09-14

First public release.

### Added
- SQLite broker: WAL mode, atomic `UPDATE … RETURNING` claims, named queues, priorities, FIFO within priority
- Redis broker (`pip install "ferry[redis]"`): same API, atomic Lua claims, sorted-set queues, shared across machines
- `@app.task` / `@app.periodic` decorators, `delay`, `apply_async`, `AsyncResult` with `get()`
- Delayed execution via `countdown` / `eta`; five-field cron parser and `ferry beat` scheduler
- Exponential retry backoff with jitter, per-task tuning, dead-letter queue with one-click retry
- Worker heartbeats, stale-claim recovery, graceful shutdown
- Strict JSON serialization: datetime, date, UUID, bytes, sets, nested payloads
- Live dashboard (`ferry dashboard`): queue depth, throughput chart, filterable tasks, worker roster, retry/purge controls over WebSocket
- CLI: `ferry worker`, `ferry beat`, `ferry dashboard`, `ferry stats`, `ferry purge`
- Benchmarks: ~1,275 tasks/sec end-to-end (2 vCPU, SQLite, no-op tasks)

### Fixed
- `Worker.run_once()` could claim a retry during its double-check and never submit it, leaving it stuck in `claimed`
- Beat could enqueue the same cron slot twice after the prior task finished
- Dashboard static files were missing from the built wheel
