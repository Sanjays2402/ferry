# Changelog

All notable changes to Ferry are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Task canvases: `app.chain()`, `app.group()`, `app.chord()` with `Task.s()` / `Task.si()` signatures and per-link `.set()` options; `ChainResult`, `GroupResult`, `ChordResult`. Works on both brokers; chord completion uses an atomic barrier so the callback fires exactly once.
- README visuals: architecture diagram, terminal card with real `ferry worker` / `ferry stats` output, and a chain/group/chord diagram (replaces the mermaid sketch).
- Control plane: pause/resume queues (`broker.pause_queue()` / `resume_queue()`, `ferry pause` / `ferry resume`, dashboard toggles); workers skip paused queues while tasks pile up untouched.
- `dedupe_key` on `apply_async()`: while a task with the same key is pending, re-enqueueing returns the existing task id instead of a duplicate.
- Result TTL: `Ferry(result_ttl=…)`; the beat drops result payloads older than the TTL and `AsyncResult.get()` raises the new `ResultExpired` (rows stay for history).
- Bulk retry: `broker.retry_dead([queue])`, `ferry retry-dead [--queue]`, dashboard "Retry all dead" plus bulk retry of selected tasks.
- Dashboard: click any task for an inspector drawer (arguments, result/traceback, status timeline); queue status badges with pause/resume buttons; bulk-select checkboxes; toast notifications.
- Task revocation: `broker.revoke(task_id)` moves a not-yet-started task to the terminal `revoked` state (`AsyncResult.get()` raises the new `TaskRevoked`); `ferry revoke <id>`, dashboard revoke buttons on queued tasks and in the inspector. Workers skip tasks revoked between claim and start.
- Time limits: `@app.task(time_limit=…, soft_time_limit=…)` (overridable per `apply_async` call). The soft limit raises `SoftTimeLimitExceeded` in the task thread (catchable for cleanup); the hard limit abandons the thread and fails the task with `TimeLimitExceeded`.
- Lifecycle hooks: `on_success(task_id, result, elapsed)`, `on_failure(task_id, exc, will_retry)`, `on_retry(task_id, exc, attempt, retry_in)` on `@app.task`. Hook errors are logged, never fatal.
- Per-queue rate limits: `broker.set_rate_limit(queue, "100/s" | "10/m" | "5/h")` / `get_rate_limit` / `clear_rate_limit`; `ferry rate-limit <queue> <rate> [--clear]`; dashboard rate-limit column with set/clear. Enforced atomically in the claim path on both brokers, so the cap holds across any number of workers.

- Task expiry: `expires=` (seconds or datetime) on `@app.task`, `apply_async()` and signatures. A worker that claims a task after its deadline skips it; the task moves to the terminal `expired` state, `task_expired` is emitted, and `AsyncResult.get()` raises the new `TaskExpired`. Both brokers; `expired` appears in stats, the dashboard filter, and periodic tasks honor the task's default.
- Task progress: `ferry.update_progress(current, total=None, message=None)` inside a running task; `AsyncResult.progress`, `task_progress` events, progress bars in the dashboard, `progress` in the task API. Stored on the task (both brokers) and cleared on retry.
- Prometheus metrics: `GET /metrics` on the dashboard (`ferry_tasks`, `ferry_queue_tasks`, `ferry_queue_paused`, `ferry_workers`, …) and `ferry.metrics.render_metrics()`; no extra dependency.
- Dashboard authentication: `ferry dashboard --token` / `FERRY_DASHBOARD_TOKEN` (and `create_app(token=…)`) require a Bearer token or Basic-auth password on every route including `/metrics` and the WebSocket; constant-time comparison; warns when binding a non-local address without a token.

- Async tasks: `async def` tasks run natively in workers (own event loop per call); `AsyncResult.aget()` awaits a result without blocking the loop. For async tasks the soft time limit cancels the task (`asyncio.CancelledError`; uncaught → `SoftTimeLimitExceeded`).
- CLI: `ferry tasks [--status] [--queue] [--limit] [--json]` and `ferry inspect <task-id> [--json]`.
- Dashboard restyled in true black (OLED): flat sections with hairline dividers, monospace figures, status shown as a dot + label instead of filled pills.

### Fixed
- Cron: `7` is now accepted for Sunday; empty list items (`5,`) used to become a wildcard and run every minute; negative steps and reversed ranges (`30-10`) silently produced a schedule that never fires and are now rejected; `a/n` starts at `a` (`5/20` = 5,25,45); when both day-of-month and day-of-week are restricted either may match, as in standard cron (previously both had to).
- Worker: a task returning a value that can't be serialized was re-executed on every retry (repeating its side effects); it now fails once, without retries.
- Worker: `shutdown_timeout` was never applied, so a hung task made SIGTERM wait forever. Shutdown now waits at most `shutdown_timeout`, abandons stragglers (`Worker.abandoned`) and the CLI worker exits.
- Worker: the poll loop claimed tasks regardless of `concurrency`, parking a backlog on one busy worker and starving the others (and higher priorities); it now claims only when a thread is free.
- Worker: retry backoff raised `OverflowError` for very large attempt counts (e.g. `max_retries=10**6`), leaving the task stuck in `running`.
- Canvas: using the same signature object twice in a chain gave two links the same task id (`UNIQUE constraint failed`); chains and groups now work on copies of the signatures.
- SQLite broker: `sqlite:///:memory:` was unusable with workers (each thread saw its own empty database); it is now backed by a private temp file that is removed at exit.
- Redis broker: queue names containing `:` were mangled in `stats()`; `list_tasks(limit=N)` returned an arbitrary N tasks instead of the newest; worker `started_at` was reset on every heartbeat.
- Dashboard / CLI: an invalid rate limit returned HTTP 500 (now 400 with the reason, shown in a toast) and crashed `ferry rate-limit` with a traceback (now a one-line error). Pause, resume and rate-limit actions refresh the tables immediately.
- Repo: `ferry.db` and `ferry.egg-info/` were committed despite being in `.gitignore`; they are no longer tracked.
- The `dashboard` extra did not install a WebSocket library, so uvicorn answered every `/ws` upgrade with 404 and the live feed could never connect; it now depends on `websockets`.
- Dashboard WebSocket (`/ws`) rejected every connection (FastAPI could not resolve the `WebSocket` annotation because it was imported inside `create_app`), so the live feed silently fell back to polling.
- A task revoked between claim and start never released its worker slot, so `Worker.run_once()` could wait out its full timeout instead of draining.

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
