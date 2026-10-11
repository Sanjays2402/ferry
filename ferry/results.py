"""AsyncResult: a handle on an enqueued task."""

from __future__ import annotations

import asyncio
import time

_TERMINAL = ("done", "failed", "dead", "revoked", "expired")


class TaskFailed(Exception):
    """Raised by ``AsyncResult.get()`` when the task finished with an error."""

    def __init__(self, task_id: str, error: str):
        super().__init__(f"task {task_id} failed: {error}")
        self.task_id = task_id
        self.error = error


class TaskRevoked(Exception):
    """Raised by ``AsyncResult.get()`` when the task was revoked before it ran."""

    def __init__(self, task_id: str):
        super().__init__(f"task {task_id} was revoked before it ran")
        self.task_id = task_id


class TaskExpired(Exception):
    """Raised by ``AsyncResult.get()`` when the task waited past its
    ``expires`` deadline and was skipped by a worker."""

    def __init__(self, task_id: str):
        super().__init__(f"task {task_id} expired before it ran")
        self.task_id = task_id


class ResultExpired(Exception):
    """Raised by ``AsyncResult.get()`` when the task's result payload was
    expired by the broker's result TTL."""

    def __init__(self, task_id: str):
        super().__init__(f"result for task {task_id} has expired")
        self.task_id = task_id


class AsyncResult:
    def __init__(self, broker, task_id: str):
        self.broker = broker
        self.task_id = task_id

    @property
    def status(self) -> str | None:
        task = self.broker.get_task(self.task_id)
        return task["status"] if task else None

    @property
    def progress(self) -> dict | None:
        """Latest progress reported by the running task via
        :func:`ferry.update_progress` (``current``, ``total``, ``percent``,
        ``message``, ``updated_at``), or None if it hasn't reported any."""
        task = self.broker.get_task(self.task_id)
        return self.broker.decode_progress(task) if task else None

    def ready(self) -> bool:
        return self.status in _TERMINAL

    def successful(self) -> bool:
        return self.status == "done"

    def _poll(self) -> tuple[bool, object]:
        """One broker check: ``(finished, result)``; raises for terminal failures."""
        task = self.broker.get_task(self.task_id)
        if task is None:
            raise KeyError(f"unknown task id {self.task_id!r}")
        status = task["status"]
        if status == "done":
            if task.get("result_expired"):
                raise ResultExpired(self.task_id)
            return True, self.broker.decode_result(task)
        if status == "revoked":
            raise TaskRevoked(self.task_id)
        if status == "expired":
            raise TaskExpired(self.task_id)
        if status in ("failed", "dead"):
            raise TaskFailed(self.task_id, task["error"] or "unknown error")
        return False, None

    def get(self, timeout: float | None = None, poll_interval: float = 0.05):
        """Block until the task finishes and return its result (or raise TaskFailed)."""
        start = time.monotonic()
        while True:
            finished, value = self._poll()
            if finished:
                return value
            if timeout is not None and time.monotonic() - start > timeout:
                raise TimeoutError(f"timed out waiting for task {self.task_id}")
            time.sleep(poll_interval)

    async def aget(self, timeout: float | None = None, poll_interval: float = 0.05):
        """Await the task's result without blocking the event loop. Same
        semantics and exceptions as :meth:`get`::

            value = await send_email.delay("ada@example.com").aget(timeout=30)
        """
        start = time.monotonic()
        while True:
            # broker calls are quick local I/O; keep them off the loop's critical path
            finished, value = await asyncio.to_thread(self._poll)
            if finished:
                return value
            if timeout is not None and time.monotonic() - start > timeout:
                raise TimeoutError(f"timed out waiting for task {self.task_id}")
            await asyncio.sleep(poll_interval)

    def retry(self) -> bool:
        """Requeue a failed/dead task. Returns True if it was requeued."""
        return self.broker.retry_task(self.task_id)

    def __repr__(self) -> str:  # pragma: no cover
        return f"AsyncResult({self.task_id!r}, status={self.status!r})"
