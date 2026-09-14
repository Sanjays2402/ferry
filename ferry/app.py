"""Ferry application: task registry, enqueue API, and periodic schedules."""

from __future__ import annotations

import functools
from collections.abc import Callable
from datetime import datetime, timedelta

from .broker import SQLiteBroker, open_broker
from .canvas import Chain, Chord, Group, Signature
from .cron import CronSchedule
from .events import EventBus
from .results import AsyncResult


class Task:
    """A registered task. Call ``.delay(...)`` / ``.apply_async(...)`` to enqueue."""

    def __init__(
        self,
        app: Ferry,
        func: Callable,
        *,
        name: str,
        queue: str,
        max_retries: int,
        priority: int,
        retry_backoff_base: float,
        retry_backoff_max: float,
        time_limit: float | None = None,
        soft_time_limit: float | None = None,
        on_success: Callable | None = None,
        on_failure: Callable | None = None,
        on_retry: Callable | None = None,
    ):
        if (
            time_limit is not None
            and soft_time_limit is not None
            and soft_time_limit >= time_limit
        ):
            raise ValueError("soft_time_limit must be less than time_limit")
        self.app = app
        self.func = func
        self.name = name
        self.queue = queue
        self.max_retries = max_retries
        self.priority = priority
        self.retry_backoff_base = retry_backoff_base
        self.retry_backoff_max = retry_backoff_max
        self.time_limit = time_limit
        self.soft_time_limit = soft_time_limit
        # lifecycle hooks (worker-side only, never serialized):
        #   on_success(task_id, result, elapsed)
        #   on_failure(task_id, exc, will_retry)
        #   on_retry(task_id, exc, attempt, retry_in)
        self.on_success = on_success
        self.on_failure = on_failure
        self.on_retry = on_retry
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
        dedupe_key: str | None = None,
        time_limit: float | None = None,
        soft_time_limit: float | None = None,
    ) -> AsyncResult:
        """Enqueue a task. ``dedupe_key`` collapses duplicates: while a task
        with the same key is pending, the existing task's id is returned.
        ``time_limit`` / ``soft_time_limit`` override the task's defaults for
        this call."""
        if countdown is not None and eta is not None:
            raise ValueError("pass either countdown or eta, not both")
        if countdown is not None:
            eta = datetime.now() + timedelta(seconds=countdown)
        tl = self.time_limit if time_limit is None else time_limit
        stl = self.soft_time_limit if soft_time_limit is None else soft_time_limit
        if tl is not None and stl is not None and stl >= tl:
            raise ValueError("soft_time_limit must be less than time_limit")
        task_id = self.app.broker.enqueue(
            self.name,
            args,
            kwargs or {},
            queue=queue or self.queue,
            priority=self.priority if priority is None else priority,
            max_retries=self.max_retries if max_retries is None else max_retries,
            eta=eta,
            dedupe_key=dedupe_key,
            time_limit=tl,
            soft_time_limit=stl,
        )
        self.app.events.emit("task_enqueued", {"task_id": task_id, "task_name": self.name})
        return AsyncResult(self.app.broker, task_id)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def s(self, *args, **kwargs) -> Signature:
        """Build a canvas signature. In a chain the previous result is
        prepended to ``args`` unless the signature is immutable (``.si()``)."""
        return Signature(
            self.app,
            self.name,
            args=args,
            kwargs=kwargs,
            options={
                "queue": self.queue,
                "priority": self.priority,
                "max_retries": self.max_retries,
                "time_limit": self.time_limit,
                "soft_time_limit": self.soft_time_limit,
            },
        )

    def si(self, *args, **kwargs) -> Signature:
        """Immutable signature: a chain result is *not* passed to this task."""
        sig = self.s(*args, **kwargs)
        sig.immutable = True
        return sig


class Ferry:
    """The Ferry application object. Owns the broker, the task registry, and schedules."""

    def __init__(
        self,
        name: str = "ferry",
        broker: str | SQLiteBroker = "sqlite:///ferry.db",
        result_ttl: float | None = None,
    ):
        """``result_ttl`` (seconds): how long to keep task result payloads.
        The beat drops payloads older than this; ``AsyncResult.get()`` then
        raises :class:`ResultExpired`. Task rows stay for history."""
        self.name = name
        self.broker = open_broker(broker) if isinstance(broker, str) else broker
        self.registry: dict[str, Task] = {}
        self.events = EventBus()
        self.result_ttl = result_ttl
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
        time_limit: float | None = None,
        soft_time_limit: float | None = None,
        on_success: Callable | None = None,
        on_failure: Callable | None = None,
        on_retry: Callable | None = None,
    ):
        """Register a function as a task::

            @app.task(queue="emails", max_retries=5, time_limit=60,
                      soft_time_limit=30, on_success=notify)
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
                time_limit=time_limit,
                soft_time_limit=soft_time_limit,
                on_success=on_success,
                on_failure=on_failure,
                on_retry=on_retry,
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

    def chain(self, *sigs: Signature) -> Chain:
        """Build a chain: ``app.chain(add.s(2, 2), double.s()).apply_async()``."""
        return Chain(self, list(sigs))

    def group(self, *sigs: Signature) -> Group:
        """Build a group: ``app.group(add.s(1, 1), add.s(2, 2)).apply_async()``."""
        return Group(self, list(sigs))

    def chord(self, header: list[Signature], body: Signature) -> Chord:
        """Build a chord: run ``header`` in parallel, then ``body(results)``."""
        return Chord(self, header, body)

    def AsyncResult(self, task_id: str) -> AsyncResult:
        return AsyncResult(self.broker, task_id)

    def wait_for(self, result: AsyncResult, timeout: float = 30.0):
        return result.get(timeout=timeout)

    @property
    def periodic_schedules(self):
        return list(self._periodic)

    def __repr__(self) -> str:  # pragma: no cover
        return f"Ferry({self.name!r}, tasks={len(self.registry)})"
