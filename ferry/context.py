"""Per-task execution context: lets a running task report progress.

The worker wraps every task call in :func:`task_context`; inside the task,
call :func:`update_progress` to publish how far along it is. Progress shows up
on ``AsyncResult.progress``, in the dashboard, and as ``task_progress`` events.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import datetime, timezone

_local = threading.local()


@contextmanager
def task_context(broker, events, task_id: str, task_name: str):
    """Mark the current thread as executing ``task_id`` (worker-internal)."""
    prev = getattr(_local, "ctx", None)
    _local.ctx = (broker, events, task_id, task_name)
    try:
        yield
    finally:
        _local.ctx = prev


def current_task_id() -> str | None:
    """The id of the task running in this thread, or None outside a task."""
    ctx = getattr(_local, "ctx", None)
    return ctx[2] if ctx else None


def update_progress(current: float, total: float | None = None, message: str | None = None) -> dict:
    """Report progress from inside a running task::

        @app.task
        def import_rows(path):
            rows = load(path)
            for i, row in enumerate(rows, 1):
                save(row)
                update_progress(i, total=len(rows), message="importing")

    Returns the stored progress dict (``current``, ``total``, ``percent``,
    ``message``, ``updated_at``). Raises ``RuntimeError`` outside a task."""
    ctx = getattr(_local, "ctx", None)
    if ctx is None:
        raise RuntimeError("update_progress() must be called from inside a running task")
    if isinstance(current, bool) or not isinstance(current, (int, float)) or current < 0:
        raise ValueError("current must be a non-negative number")
    if total is not None and (
        isinstance(total, bool) or not isinstance(total, (int, float)) or total <= 0
    ):
        raise ValueError("total must be a positive number")
    broker, events, task_id, task_name = ctx
    progress = {
        "current": current,
        "total": total,
        "percent": None if total is None else round(min(current / total, 1.0) * 100, 1),
        "message": message,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if broker.set_progress(task_id, progress):
        events.emit("task_progress", {"task_id": task_id, "task_name": task_name, **progress})
    return progress
