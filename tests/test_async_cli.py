import asyncio
import json
import threading
import time

import pytest

from ferry import Ferry, SoftTimeLimitExceeded, TaskExpired, TaskFailed, update_progress
from ferry.cli import main
from ferry.worker import Worker

# -- async tasks -------------------------------------------------------------------


def test_async_task_runs_and_returns(app, worker):
    @app.task
    async def fetch(x):
        await asyncio.sleep(0.01)
        return x * 2

    r = fetch.delay(21)
    worker.run_once(timeout=5)
    assert r.get(timeout=1) == 42


def test_async_tasks_run_concurrently(app):
    """Each task gets its own loop in its own thread, so sleeps overlap."""

    @app.task
    async def nap():
        await asyncio.sleep(0.3)
        return 1

    results = [nap.delay() for _ in range(4)]
    start = time.monotonic()
    Worker(app, concurrency=4, poll_interval=0.01).run_once(timeout=5)
    assert [r.get(timeout=1) for r in results] == [1, 1, 1, 1]
    assert time.monotonic() - start < 1.0  # ~0.3s, not 4 x 0.3s


def test_async_task_failure_is_reported(app, worker):
    calls = []

    @app.task(max_retries=0)
    async def boom():
        calls.append(1)
        raise RuntimeError("async kaput")

    r = boom.delay()
    worker.run_once(timeout=5)
    assert calls == [1]
    with pytest.raises(TaskFailed, match="async kaput"):
        r.get(timeout=1)


def test_async_soft_limit_cancels_so_task_can_clean_up(app, worker):
    @app.task(time_limit=10, soft_time_limit=0.2)
    async def slow():
        update_progress(1, total=2, message="started")
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            return "cleaned up"

    start = time.monotonic()
    r = slow.delay()
    worker.run_once(timeout=8)
    assert r.get(timeout=1) == "cleaned up"
    assert time.monotonic() - start < 3  # cut short at ~0.2s, not 5s
    assert r.progress["message"] == "started"


def test_async_soft_limit_uncaught_fails_with_soft_limit_error(app, worker):
    @app.task(max_retries=0, soft_time_limit=0.1)
    async def stuck():
        await asyncio.sleep(5)

    r = stuck.delay()
    worker.run_once(timeout=8)
    with pytest.raises(TaskFailed, match=SoftTimeLimitExceeded.__name__):
        r.get(timeout=1)


def test_async_hard_limit_abandons_task(app, worker):
    @app.task(max_retries=0, time_limit=0.2)
    async def stuck():
        await asyncio.sleep(5)

    r = stuck.delay()
    worker.run_once(timeout=8)
    with pytest.raises(TaskFailed, match="TimeLimitExceeded"):
        r.get(timeout=1)


def test_calling_async_task_directly_returns_coroutine(app):
    @app.task
    async def f():
        return 1

    assert asyncio.run(f()) == 1


# -- aget ----------------------------------------------------------------------------


def test_aget_awaits_result_while_worker_runs(app):
    @app.task
    def add(a, b):
        return a + b

    def run_worker_soon():
        time.sleep(0.1)
        Worker(app, concurrency=1, poll_interval=0.01).run_once(timeout=5)

    async def scenario():
        result = add.delay(2, 3)
        t = threading.Thread(target=run_worker_soon)
        t.start()
        try:
            return await result.aget(timeout=5)
        finally:
            t.join()

    assert asyncio.run(scenario()) == 5


def test_aget_timeout_and_errors(app, worker):
    @app.task
    def never():
        return 1

    async def scenario():
        with pytest.raises(TimeoutError):
            await never.delay().aget(timeout=0.15, poll_interval=0.02)
        stale = never.apply_async(expires=0.05)
        await asyncio.sleep(0.1)
        await asyncio.to_thread(worker.run_once, 2)
        with pytest.raises(TaskExpired):
            await stale.aget(timeout=2)

    asyncio.run(scenario())


# -- inspect / tasks CLI -------------------------------------------------------------


@pytest.fixture()
def db_url(tmp_path):
    return f"sqlite:///{tmp_path}/cli.db"


def test_tasks_lists_and_filters(db_url, capsys):
    app = Ferry("c", broker=db_url)

    @app.task(queue="emails")
    def send(to):
        update_progress(1, total=4, message="sending")
        return to

    @app.task(max_retries=0)
    def boom():
        raise RuntimeError("x")

    ok, bad = send.delay("a@b.c"), boom.delay()
    Worker(app, queues=["default", "emails"], concurrency=2, poll_interval=0.01).run_once(
        timeout=5
    )
    assert main(["tasks", "--broker", db_url]) == 0
    out = capsys.readouterr().out
    assert ok.task_id[:8] in out and bad.task_id[:8] in out
    assert main(["tasks", "--broker", db_url, "--status", "dead"]) == 0
    out = capsys.readouterr().out
    assert bad.task_id[:8] in out and ok.task_id[:8] not in out
    main(["tasks", "--broker", db_url, "--queue", "emails", "--json"])
    rows = json.loads(capsys.readouterr().out)
    assert len(rows) == 1 and rows[0]["status"] == "done"
    assert rows[0]["progress"]["percent"] == 25.0


def test_tasks_empty(db_url, capsys):
    Ferry("c", broker=db_url)
    assert main(["tasks", "--broker", db_url]) == 0
    assert "no tasks" in capsys.readouterr().out


def test_inspect_shows_everything(db_url, capsys):
    app = Ferry("c", broker=db_url)

    @app.task(expires=600)
    def add(a, b=0):
        update_progress(3, total=4, message="adding")
        return a + b

    r = add.delay(1, b=2)
    Worker(app, concurrency=1, poll_interval=0.01).run_once(timeout=5)
    assert main(["inspect", r.task_id, "--broker", db_url]) == 0
    out = capsys.readouterr().out
    assert "done" in out and "adding" in out and "expires_at" in out
    main(["inspect", r.task_id, "--broker", db_url, "--json"])
    detail = json.loads(capsys.readouterr().out)
    assert detail["args"] == [1] and detail["kwargs"] == {"b": 2}
    assert detail["result"] == 3
    assert detail["progress"]["percent"] == 75.0


def test_inspect_unknown_task(db_url, capsys):
    Ferry("c", broker=db_url)
    assert main(["inspect", "nope", "--broker", db_url]) == 1
    assert "no such task" in capsys.readouterr().err
