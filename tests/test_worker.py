import pytest

from ferry import Ferry, TaskFailed
from ferry.worker import Worker
from tests.conftest import run_until


def test_end_to_end(app, worker):
    @app.task
    def add(a, b):
        return a + b

    result = add.delay(2, 3)
    run_until(worker, result)
    assert result.get() == 5
    assert result.successful()


def test_kwargs_and_complex_payloads(app, worker):
    @app.task
    def echo(data):
        return data

    payload = {"when": "2026-09-13", "ids": [1, 2, 3]}
    result = echo.delay(data=payload)
    run_until(worker, result)
    assert result.get() == payload


def test_retry_then_success(app, worker):
    calls = []

    @app.task(max_retries=3, retry_backoff_base=0.01, retry_backoff_max=0.05)
    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("boom")
        return "recovered"

    result = flaky.delay()
    run_until(worker, result)
    assert result.get() == "recovered"
    stored = app.broker.get_task(result.task_id)
    assert stored["attempts"] == 2  # two failures, then success


def test_dead_letter_after_exhaustion(app, worker):
    @app.task(max_retries=1, retry_backoff_base=0.01, retry_backoff_max=0.05)
    def always_fails():
        raise ValueError("nope")

    result = always_fails.delay()
    run_until(worker, result)
    assert result.status == "dead"
    with pytest.raises(TaskFailed, match="nope"):
        result.get()


def test_unknown_task_goes_to_dead_letter(app, worker):
    task_id = app.broker.enqueue("not.registered", [], {})
    result = app.AsyncResult(task_id)
    # single attempt so it dies fast
    app.broker._connect().execute(
        "UPDATE ferry_tasks SET max_retries=0 WHERE id=?", (task_id,)
    )
    app.broker._connect().commit()
    run_until(worker, result)
    assert result.status == "dead"


def test_countdown_defers_execution(app, worker):
    @app.task
    def quick():
        return "now"

    result = quick.apply_async(countdown=30)
    worker.run_once(timeout=1.0)
    assert not result.ready()
    assert result.status == "queued"


def test_events_fire(app, worker):
    seen = []
    app.events.on("task_succeeded", lambda p: seen.append(p["task_name"]))

    @app.task
    def job():
        return 1

    result = job.delay()
    run_until(worker, result)
    assert seen == [job.name]


def test_duplicate_task_name_rejected(app):
    @app.task(name="dup")
    def a():
        pass

    with pytest.raises(ValueError, match="already registered"):
        @app.task(name="dup")
        def b():
            pass


def test_get_timeout(app, worker):
    @app.task
    def slow():
        return 1

    result = slow.apply_async(countdown=60)
    with pytest.raises(TimeoutError):
        result.get(timeout=0.3)
