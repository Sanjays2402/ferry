"""Ferry application: task registry, enqueue API, and periodic schedules."""

from __future__ import annotations

import functools
import time
from datetime import datetime, timedelta
from typing import Any, Callable

from .broker import SQLiteBroker, open_broker
from .cron import CronSchedule
from .events import EventBus
from .results import AsyncResult


class Task:
    """A registered task. Call ``.delay(...)`` / ``.apply_async(...)`` to enqueue."""

    def __init__(
        self,
        app: "Ferry",
        func: Callable,
        *,
        name: str,
        queue: str,
        max_retries: int,
        priority: int,
        retry_backoff_base: float,
        retry_backoff_max: float,
    ):
        self.app = app
        self.func = func
        self.name = name
        self.queue = queue
        self.max_retries = max_retries
        self.priority = priority
        self.retry_backoff_base = retry_backoff_base
        self.retry_backoff_max = retry_backoff_max
        functools.update_wrapper(self, func)

    def delay(self, *args, **kwargs) -> AsyncResult:
        return self.apply_async(args=args, kwargs=kwargs)

    def apply_async(
        self,
        args: tuple = (),
        kwargs: dict | None = None,
        *,
        queue: str | None = None,
        priority: int | None = None,
        max_retries: int | None = None,
        countdown: float | None = None,
        eta: datetime | None = None,
    ) -> AsyncResult:
        if countdown is not None and eta is not None:
            raise ValueError("pass either countdown or eta, not both")
        if countdown is not None:
            eta = datetime.now() + timedelta(seconds=countdown)
        task_id = self.app.broker.enqueue(
            self.name,
            args,
            kwargs or {},
            queue=queue or self.queue,
            priority=self.priority if priority is None else priority,
            max_retries=self.max_retries if max_retries is None else max_retries,
            eta=eta,
        )
        self.app.events.emit("task_enqueued", {"task_id": task_id, "task_name": self.name})
        return AsyncResult(self.app.broker, task_id)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class Ferry:
    """The Ferry application object. Owns the broker, the task registry, and schedules."""

    def __init__(self, name: str = "ferry", broker: str | SQLiteBroker = "sqlite:///ferry.db"):
        self.name = name
        self.broker = broker if isinstance(broker, SQLiteBroker) else open_broker(broker)
        self.registry: dict[str, Task] = {}
        self.events = EventBus()
        self._periodic: list[tuple[CronSchedule, str, dict]] = []

    def task(
        self,
        func: Callable | None = None,
        *,
        name: str | None = None,
        queue: str = "default",
        max_retries: int = 3,
        priority: int = 0,
        retry_backoff_base: float = 5.0,
        retry_backoff_max: float = 600.0,
    ):
        """Register a function as a task::

            @app.task(queue="emails", max_retries=5)
            def send_email(to, subject): ...
        """

        def register(f: Callable) -> Task:
            task_name = name or f"{f.__module__}.{f.__qualname__}"
            if task_name in self.registry:
                raise ValueError(f"task {task_name!r} is already registered")
            task = Task(
                self,
                f,
                name=task_name,
                queue=queue,
                max_retries=max_retries,
                priority=priority,
                retry_backoff_base=retry_backoff_base,
                retry_backoff_max=retry_backoff_max,
            )
            self.registry[task_name] = task
            return task

        return register(func) if func else register

    def periodic(self, cron: str, **options):
        """Schedule a task on a cron expression::

            @app.periodic("*/5 * * * *", queue="maintenance")
            def cleanup(): ...
        """

        def register(f: Callable) -> Task:
            task = self.task(f, **options)
            self._periodic.append((CronSchedule(cron), task.name, options))
            return task

        return register

    def send_task(self, name: str, *args, **kwargs) -> AsyncResult:
        task = self.registry.get(name)
        if task is None:
            raise KeyError(f"unknown task {name!r}")
        return task.delay(*args, **kwargs)

    def AsyncResult(self, task_id: str) -> AsyncResult:
        return AsyncResult(self.broker, task_id)

    def wait_for(self, result: AsyncResult, timeout: float = 30.0):
        return result.get(timeout=timeout)

    @property
    def periodic_schedules(self):
        return list(self._periodic)

    def __repr__(self) -> str:  # pragma: no cover
        return f"Ferry({self.name!r}, tasks={len(self.registry)})"
