"""Worker: polls the broker, executes tasks in a thread pool, handles retries.

Design notes:
- Claims are atomic (single UPDATE ... RETURNING), so any number of workers
  across processes/machines can share one broker without double-execution.
- Tasks run in threads: ideal for I/O-bound work (HTTP, DB, S3). CPU-bound
  workloads should chunk their work or run one worker per core.
- On SIGINT/SIGTERM the worker stops claiming, lets in-flight tasks finish
  (bounded by ``shutdown_timeout``), then exits cleanly.
"""

from __future__ import annotations

import concurrent.futures
import logging
import os
import random
import signal
import socket
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone

from .app import Ferry
from .canvas import fire_continuations

log = logging.getLogger("ferry.worker")


class Worker:
    def __init__(
        self,
        app: Ferry,
        queues: list[str] | None = None,
        concurrency: int = 4,
        poll_interval: float = 0.2,
        heartbeat_interval: float = 10.0,
        shutdown_timeout: float = 30.0,
        worker_id: str | None = None,
    ):
        self.app = app
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.shutdown_timeout = shutdown_timeout
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._stop = threading.Event()
        self._pool: concurrent.futures.ThreadPoolExecutor | None = None
        self._inflight = 0
        self._inflight_lock = threading.Lock()
        self._paused_at = 0.0
        self._paused: set[str] = set()

    def _active_queues(self) -> list[str]:
        """Queues this worker may claim from right now (paused ones excluded).

        The paused set is refreshed at most every 2 seconds so the hot claim
        loop doesn't hit the broker on every poll."""
        now = time.monotonic()
        if now - self._paused_at > 2.0:
            try:
                self._paused = set(self.app.broker.paused_queues())
            except Exception:  # pragma: no cover - broker hiccup: keep going
                log.exception("paused-queue check failed")
            self._paused_at = now
        return [q for q in self.queues if q not in self._paused]

    # -- lifecycle ------------------------------------------------------------
    def run(self) -> None:
        """Run forever until SIGINT/SIGTERM (or :meth:`stop`)."""
        self._install_signal_handlers()
        log.info("worker %s started: queues=%s concurrency=%d",
                 self.worker_id, self.queues, self.concurrency)
        self._pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.concurrency, thread_name_prefix=f"ferry-{self.worker_id[:8]}"
        )
        self.app.broker.heartbeat(
            self.worker_id, self.queues, self.concurrency, socket.gethostname()
        )
        heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat.start()
        try:
            self._poll_loop()
        finally:
            self._shutdown_pool()
            self.app.broker.remove_worker(self.worker_id)
            log.info("worker %s stopped", self.worker_id)

    def run_once(self, timeout: float = 5.0) -> int:
        """Claim and execute tasks until the queues drain or ``timeout`` elapses.

        Intended for tests and scripts. Returns the number of tasks executed.
        """
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency)
        deadline = time.monotonic() + timeout
        executed = 0
        self.app.broker.heartbeat(
            self.worker_id, self.queues, self.concurrency, socket.gethostname()
        )
        try:
            while time.monotonic() < deadline:
                queues = self._active_queues()
                task = self.app.broker.claim(queues, self.worker_id) if queues else None
                if task is None:
                    # nothing visible right now; sleep briefly, then double-check
                    # before giving up (a retry's backoff may elapse while we sleep)
                    time.sleep(self.poll_interval)
                    if self._inflight != 0:
                        continue
                    queues = self._active_queues()
                    task = self.app.broker.claim(queues, self.worker_id) if queues else None
                    if task is None:
                        break
                with self._inflight_lock:
                    self._inflight += 1
                executed += 1
                self._pool.submit(self._execute, task)
            self._pool.shutdown(wait=True)
            return executed
        finally:
            self.app.broker.remove_worker(self.worker_id)

    def stop(self) -> None:
        self._stop.set()

    # -- internals --------------------------------------------------------------
    def _install_signal_handlers(self) -> None:
        def _handle(signum, frame):
            log.info("worker %s received signal %d, shutting down…", self.worker_id, signum)
            self.stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _handle)
            except ValueError:
                pass  # not on the main thread (e.g. embedded use)

    def _heartbeat_loop(self) -> None:
        hostname = socket.gethostname()
        while not self._stop.wait(self.heartbeat_interval):
            try:
                self.app.broker.heartbeat(self.worker_id, self.queues, self.concurrency, hostname)
            except Exception:  # pragma: no cover - best effort
                log.exception("heartbeat failed")

    def _poll_loop(self) -> None:
        assert self._pool is not None
        while not self._stop.is_set():
            task = None
            try:
                queues = self._active_queues()
                if not queues:
                    self._stop.wait(self.poll_interval)
                    continue
                task = self.app.broker.claim(queues, self.worker_id)
            except Exception:
                log.exception("claim failed; backing off")
                self._stop.wait(1.0)
                continue
            if task is None:
                self._stop.wait(self.poll_interval)
                continue
            with self._inflight_lock:
                self._inflight += 1
            self._pool.submit(self._execute, task)

    def _shutdown_pool(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=False)

    def _execute(self, task: dict) -> None:
        task_id, name = task["id"], task["task_name"]
        broker, events = self.app.broker, self.app.events
        try:
            broker.mark_running(task_id)
            func = self.app.registry.get(name)
            if func is None:
                raise RuntimeError(f"unknown task {name!r} (not registered on this worker)")
            args, kwargs = broker.decode_args(task)
            events.emit("task_started", {"task_id": task_id, "task_name": name})
            started = time.monotonic()
            result = func(*args, **kwargs)
            elapsed = time.monotonic() - started
            broker.ack_done(task_id, result)
            events.emit("task_succeeded",
                        {"task_id": task_id, "task_name": name, "elapsed": elapsed})
            for new_id, new_name in fire_continuations(broker, task, result):
                events.emit("task_enqueued", {"task_id": new_id, "task_name": new_name})
            log.info("task %s (%s) done in %.2fs", name, task_id[:8], elapsed)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=5)}"
            attempts = task["attempts"]
            max_retries = task["max_retries"]
            if attempts < max_retries:
                delay = self._backoff(task, attempts)
                retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
                broker.ack_failed(task_id, error, retry_at)
                events.emit("task_retried", {"task_id": task_id, "task_name": name,
                                             "attempt": attempts + 1, "retry_in": delay})
                log.warning("task %s (%s) failed (attempt %d/%d), retrying in %.1fs: %s",
                            name, task_id[:8], attempts + 1, max_retries + 1, delay, exc)
            else:
                broker.ack_failed(task_id, error, None)
                events.emit("task_dead", {"task_id": task_id, "task_name": name, "error": str(exc)})
                log.error("task %s (%s) dead after %d attempts: %s",
                          name, task_id[:8], attempts + 1, exc)
        finally:
            with self._inflight_lock:
                self._inflight -= 1

    def _backoff(self, task: dict, attempts: int) -> float:
        """Exponential backoff with full jitter."""
        entry = self.app.registry.get(task["task_name"])
        base = entry.retry_backoff_base if entry else 5.0
        cap = entry.retry_backoff_max if entry else 600.0
        delay = min(base * (2 ** attempts), cap)
        return delay / 2 + random.uniform(0, delay / 2)
