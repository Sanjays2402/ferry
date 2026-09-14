"""AsyncResult: a handle on an enqueued task."""

from __future__ import annotations

import time

_TERMINAL = ("done", "failed", "dead")


class TaskFailed(Exception):
    """Raised by ``AsyncResult.get()`` when the task finished with an error."""

    def __init__(self, task_id: str, error: str):
        super().__init__(f"task {task_id} failed: {error}")
        self.task_id = task_id
        self.error = error


class AsyncResult:
    def __init__(self, broker, task_id: str):
        self.broker = broker
        self.task_id = task_id

    @property
    def status(self) -> str | None:
        task = self.broker.get_task(self.task_id)
        return task["status"] if task else None

    def ready(self) -> bool:
        return self.status in _TERMINAL

    def successful(self) -> bool:
        return self.status == "done"

    def get(self, timeout: float | None = None, poll_interval: float = 0.05):
        """Block until the task finishes and return its result (or raise TaskFailed)."""
        start = time.monotonic()
        while True:
            task = self.broker.get_task(self.task_id)
            if task is None:
                raise KeyError(f"unknown task id {self.task_id!r}")
            status = task["status"]
            if status == "done":
                return self.broker.decode_result(task)
            if status in ("failed", "dead"):
                raise TaskFailed(self.task_id, task["error"] or "unknown error")
            if timeout is not None and time.monotonic() - start > timeout:
                raise TimeoutError(f"timed out waiting for task {self.task_id}")
            time.sleep(poll_interval)

    def retry(self) -> bool:
        """Requeue a failed/dead task. Returns True if it was requeued."""
        return self.broker.retry_task(self.task_id)

    def __repr__(self) -> str:  # pragma: no cover
        return f"AsyncResult({self.task_id!r}, status={self.status!r})"
