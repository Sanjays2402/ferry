import pytest

from ferry import AsyncResult, current_task_id, update_progress


def test_progress_reported_and_final_value_kept(app, worker):
    seen = []

    @app.task
    def import_rows(n):
        for i in range(1, n + 1):
            update_progress(i, total=n, message="importing")
            seen.append(AsyncResult(app.broker, current_task_id()).progress["current"])
        return n

    r = import_rows.delay(4)
    assert r.progress is None
    worker.run_once(timeout=5)
    assert r.get(timeout=1) == 4
    assert seen == [1, 2, 3, 4]  # visible to observers while the task ran
    p = r.progress
    assert p["current"] == 4 and p["total"] == 4
    assert p["percent"] == 100.0
    assert p["message"] == "importing"
    assert p["updated_at"]


def test_progress_without_total(app, worker):
    @app.task
    def job():
        return update_progress(42)

    r = job.delay()
    worker.run_once(timeout=5)
    assert r.get(timeout=1)["percent"] is None
    assert r.progress["current"] == 42 and r.progress["total"] is None


def test_progress_works_under_time_limit_thread(app, worker):
    @app.task(time_limit=10, soft_time_limit=5)
    def job():
        update_progress(1, total=2)
        return "ok"

    r = job.delay()
    worker.run_once(timeout=5)
    assert r.get(timeout=1) == "ok"
    assert r.progress["percent"] == 50.0


def test_progress_emits_events(app, worker):
    events = []
    app.events.on("task_progress", events.append)

    @app.task
    def job():
        update_progress(1, total=4, message="a")
        update_progress(3, total=4, message="b")

    r = job.delay()
    worker.run_once(timeout=5)
    assert [e["current"] for e in events] == [1, 3]
    assert events[0]["task_id"] == r.task_id and events[1]["percent"] == 75.0


def test_percent_is_capped_at_100(app, worker):
    @app.task
    def job():
        return update_progress(15, total=10)["percent"]

    r = job.delay()
    worker.run_once(timeout=5)
    assert r.get(timeout=1) == 100.0


def test_update_progress_outside_task_raises():
    assert current_task_id() is None
    with pytest.raises(RuntimeError):
        update_progress(1, total=2)


def test_update_progress_validation(app, worker):
    errors = []

    @app.task(max_retries=0)
    def job():
        for bad in (dict(current=-1), dict(current="x"), dict(current=1, total=0),
                    dict(current=True)):
            try:
                update_progress(**bad)
            except ValueError as exc:
                errors.append(str(exc))
        return "done"

    r = job.delay()
    worker.run_once(timeout=5)
    assert r.get(timeout=1) == "done"
    assert len(errors) == 4


def test_progress_ignored_unless_running(app):
    @app.task
    def job():
        return 1

    r = job.delay()
    assert app.broker.set_progress(r.task_id, {"current": 1}) is False  # still queued
    assert r.progress is None


def test_progress_cleared_on_retry(app, worker):
    @app.task(max_retries=0)
    def job():
        update_progress(1, total=2)
        raise RuntimeError("boom")

    r = job.delay()
    worker.run_once(timeout=5)
    assert r.status == "dead"
    assert r.progress["current"] == 1  # kept for post-mortems
    assert r.retry()
    assert r.progress is None


def test_context_does_not_leak_between_tasks(app, worker):
    ids = []

    @app.task
    def job():
        ids.append(current_task_id())

    a, b = job.delay(), job.delay()
    worker.run_once(timeout=5)
    assert sorted(ids) == sorted([a.task_id, b.task_id])
    assert current_task_id() is None
