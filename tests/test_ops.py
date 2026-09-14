"""Operations bundle: revocation, time limits, lifecycle hooks, rate limits.

Each behavior is tested against both brokers (SQLite + fakeredis).
"""

import time

import pytest

from ferry import (
    Ferry,
    SoftTimeLimitExceeded,
    TaskRevoked,
    TimeLimitExceeded,
)
from ferry.broker import parse_rate
from ferry.worker import Worker

fakeredis = pytest.importorskip("fakeredis", reason="fakeredis not installed")

from ferry.redis_broker import RedisBroker  # noqa: E402


@pytest.fixture(params=["sqlite", "redis"])
def any_app(request, tmp_path):
    if request.param == "sqlite":
        return Ferry("test", broker=f"sqlite:///{tmp_path}/ops.db")
    return Ferry("test", broker=RedisBroker(client=fakeredis.FakeRedis(decode_responses=True)))


def run_all(worker, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        n = worker.run_once(timeout=0.3)
        if n == 0:
            return
    raise TimeoutError("worker did not drain")


# -- task revocation ------------------------------------------------------------

def test_revoke_queued_task(any_app):
    app = any_app

    @app.task
    def job():
        return 1

    r = job.delay()
    assert app.broker.revoke(r.task_id) is True
    assert app.broker.get_task(r.task_id)["status"] == "revoked"
    # already terminal: second revoke is a no-op
    assert app.broker.revoke(r.task_id) is False
    assert app.broker.revoke("no-such-task") is False


def test_revoke_blocks_execution(any_app):
    app = any_app
    ran = []

    @app.task
    def job():
        ran.append(1)

    r = job.delay()
    app.broker.revoke(r.task_id)
    w = Worker(app, concurrency=2, poll_interval=0.01)
    assert w.run_once(timeout=1.0) == 0
    assert ran == []
    assert app.broker.stats()["tasks"]["revoked"] == 1


def test_revoke_running_task_fails(any_app):
    app = any_app

    @app.task
    def job():
        return 1

    r = job.delay()
    task = app.broker.claim(["default"], "w1")
    assert task["id"] == r.task_id
    assert app.broker.mark_running(task["id"]) is True
    # already started: cannot revoke
    assert app.broker.revoke(r.task_id) is False
    assert app.broker.get_task(r.task_id)["status"] == "running"


def test_revoked_result_raises(any_app):
    app = any_app

    @app.task
    def job():
        return 1

    r = job.delay()
    app.broker.revoke(r.task_id)
    assert r.ready() is True
    assert r.successful() is False
    with pytest.raises(TaskRevoked):
        r.get(timeout=1.0)


def test_revoke_frees_dedupe_key(any_app):
    app = any_app

    @app.task
    def job():
        return 1

    r1 = job.apply_async(dedupe_key="k1")
    assert app.broker.revoke(r1.task_id) is True
    # revoked is terminal: the key is free for a fresh task
    r2 = job.apply_async(dedupe_key="k1")
    assert r2.task_id != r1.task_id


def test_cli_revoke(any_app, tmp_path, capsys):
    if request_param(any_app) == "redis":
        pytest.skip("CLI revoke needs a real broker URL")
    from ferry.cli import main

    app = any_app

    @app.task
    def job():
        return 1

    r = job.delay()
    url = f"sqlite:///{tmp_path}/ops.db"  # matches the sqlite fixture's database
    assert main(["revoke", r.task_id, "--broker", url]) == 0
    assert "revoked task" in capsys.readouterr().out
    assert app.broker.get_task(r.task_id)["status"] == "revoked"
    assert main(["revoke", r.task_id, "--broker", url]) == 1


def request_param(app):
    return "sqlite" if type(app.broker).__name__ == "SQLiteBroker" else "redis"


# -- time limits ----------------------------------------------------------------

def test_soft_time_limit(any_app):
    app = any_app

    @app.task(max_retries=0, soft_time_limit=0.2, time_limit=30.0)
    def slow():
        for _ in range(200):
            time.sleep(0.05)
        return "never"  # pragma: no cover

    r = slow.delay()
    run_all(Worker(app, concurrency=2, poll_interval=0.01))
    t = app.broker.get_task(r.task_id)
    assert t["status"] == "dead"
    assert "SoftTimeLimitExceeded" in (t["error"] or "")


def test_hard_time_limit_abandons(any_app):
    app = any_app

    @app.task(max_retries=0, time_limit=0.3)
    def stuck():
        time.sleep(30)
        return "never"  # pragma: no cover

    r = stuck.delay()
    w = Worker(app, concurrency=2, poll_interval=0.01)
    start = time.monotonic()
    w.run_once(timeout=10.0)
    elapsed = time.monotonic() - start
    t = app.broker.get_task(r.task_id)
    assert t["status"] == "dead"
    assert "TimeLimitExceeded" in (t["error"] or "")
    assert elapsed < 10, "worker must not wait out the stuck task"


def test_soft_limit_caught_by_task(any_app):
    app = any_app

    @app.task(max_retries=0, soft_time_limit=0.2)
    def careful():
        try:
            for _ in range(200):
                time.sleep(0.05)
        except SoftTimeLimitExceeded:
            return "cleaned up"
        return "finished early"  # pragma: no cover

    r = careful.delay()
    run_all(Worker(app, concurrency=2, poll_interval=0.01))
    t = app.broker.get_task(r.task_id)
    assert t["status"] == "done"
    assert app.broker.decode_result(t) == "cleaned up"


def test_time_limit_override_per_call(any_app):
    app = any_app

    @app.task(max_retries=0, time_limit=30.0)
    def slow():
        time.sleep(30)
        return "never"  # pragma: no cover

    r = slow.apply_async(time_limit=0.2)
    w = Worker(app, concurrency=2, poll_interval=0.01)
    w.run_once(timeout=10.0)
    t = app.broker.get_task(r.task_id)
    assert t["status"] == "dead"
    assert "TimeLimitExceeded" in (t["error"] or "")


def test_time_limit_validation(any_app):
    app = any_app
    with pytest.raises(ValueError, match="soft_time_limit"):

        @app.task(soft_time_limit=10.0, time_limit=5.0)
        def bad():
            pass  # pragma: no cover


def test_time_limit_exceptions_importable():
    assert issubclass(SoftTimeLimitExceeded, Exception)
    assert issubclass(TimeLimitExceeded, Exception)


# -- lifecycle hooks -------------------------------------------------------------

def test_hooks_success(any_app):
    app = any_app
    calls = []

    @app.task(
        on_success=lambda tid, res, el: calls.append(("success", tid, res)),
        on_failure=lambda tid, exc, wr: calls.append(("failure", wr)),
    )
    def job(x):
        return x * 2

    r = job.delay(21)
    run_all(Worker(app, concurrency=2, poll_interval=0.01))
    assert calls == [("success", r.task_id, 42)]


def test_hooks_retry_and_dead(any_app):
    app = any_app
    calls = []

    @app.task(
        max_retries=1,
        retry_backoff_base=0.01,
        retry_backoff_max=0.02,
        on_failure=lambda tid, exc, wr: calls.append(("failure", wr)),
        on_retry=lambda tid, exc, attempt, delay: calls.append(("retry", attempt)),
    )
    def flaky():
        flaky.n = getattr(flaky, "n", 0) + 1
        if flaky.n < 2:
            raise ValueError("boom")
        return "recovered"

    r = flaky.delay()
    deadline = time.monotonic() + 10
    w = Worker(app, concurrency=2, poll_interval=0.01)
    while not app.AsyncResult(r.task_id).ready() and time.monotonic() < deadline:
        w.run_once(timeout=0.5)
        time.sleep(0.02)
    assert ("failure", True) in calls  # first attempt failed, will retry
    assert ("retry", 1) in calls
    assert ("failure", False) not in calls  # recovered: no terminal failure

    calls.clear()

    @app.task(
        max_retries=0,
        on_failure=lambda tid, exc, wr: calls.append(("failure", wr)),
    )
    def doomed():
        raise RuntimeError("always")

    r2 = doomed.delay()
    run_all(Worker(app, concurrency=2, poll_interval=0.01))
    assert app.broker.get_task(r2.task_id)["status"] == "dead"
    assert calls == [("failure", False)]


def test_hook_errors_are_isolated(any_app):
    app = any_app

    @app.task(on_success=lambda *a: 1 / 0)
    def job():
        return "fine"

    r = job.delay()
    run_all(Worker(app, concurrency=2, poll_interval=0.01))
    assert app.broker.get_task(r.task_id)["status"] == "done"


# -- rate limits -----------------------------------------------------------------

def test_parse_rate():
    assert parse_rate("100/s") == (100.0, "100/s")
    assert parse_rate("10/m") == (10 / 60, "10/m")
    assert parse_rate("5/h") == (5 / 3600, "5/h")
    assert parse_rate("1/day") == (1 / 86400, "1/day")
    assert parse_rate(50) == (50.0, "50/s")
    with pytest.raises(ValueError):
        parse_rate("bogus")
    with pytest.raises(ValueError):
        parse_rate("10/fortnight")
    with pytest.raises(ValueError):
        parse_rate("0/s")


def test_rate_limit_crud(any_app):
    b = any_app.broker
    assert b.get_rate_limit("emails") is None
    b.set_rate_limit("emails", "10/m")
    assert b.get_rate_limit("emails") == "10/m"
    assert b.rate_limits() == {"emails": "10/m"}
    b.clear_rate_limit("emails")
    assert b.get_rate_limit("emails") is None
    assert b.rate_limits() == {}


def test_rate_limit_throttles_claims(any_app):
    app = any_app

    @app.task
    def job(x):
        return x

    for i in range(3):
        job.delay(i)
    app.broker.set_rate_limit("default", "1/s")  # burst of 1, then 1/sec
    w = Worker(app, concurrency=2, poll_interval=0.01)
    assert w.run_once(timeout=1.0) == 1  # burst consumed
    assert w.run_once(timeout=0.5) == 0  # throttled: bucket empty
    # after ~1s the bucket refills and claims resume
    time.sleep(1.1)
    assert w.run_once(timeout=2.0) >= 1
    app.broker.clear_rate_limit("default")
    run_all(w)  # remainder drains immediately


def test_cli_rate_limit(any_app, tmp_path, capsys):
    if request_param(any_app) == "redis":
        pytest.skip("CLI rate-limit needs a real broker URL")
    from ferry.cli import main

    url = f"sqlite:///{tmp_path}/ops.db"  # matches the sqlite fixture's database
    assert main(["rate-limit", "emails", "10/m", "--broker", url]) == 0
    assert "10/m" in capsys.readouterr().out
    assert any_app.broker.get_rate_limit("emails") == "10/m"
    assert main(["rate-limit", "emails", "--clear", "--broker", url]) == 0
    assert any_app.broker.get_rate_limit("emails") is None


def test_rate_limit_in_stats(any_app):
    app = any_app

    @app.task(queue="emails")
    def job():
        return 1

    job.delay()
    app.broker.set_rate_limit("emails", "60/m")
    assert app.broker.stats()["rate_limits"] == {"emails": "60/m"}


def test_dashboard_revoke_and_rate_limit(tmp_path):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from ferry.dashboard import create_app

    db = tmp_path / "ops-dash.db"
    client = TestClient(create_app(f"sqlite:///{db}"))
    app = Ferry("x", broker=f"sqlite:///{db}")

    @app.task(queue="emails")
    def job():
        return 1

    r = job.delay()
    resp = client.post(f"/api/tasks/{r.task_id}/revoke")
    assert resp.json() == {"revoked": True}
    assert client.get("/api/tasks?status=revoked").json()[0]["id"] == r.task_id

    resp = client.post(
        "/api/queues/emails/rate-limit", json={"rate": "30/m"}
    )
    assert resp.json() == {"rate": "30/m", "queue": "emails"}
    assert client.get("/api/stats").json()["rate_limits"] == {"emails": "30/m"}
    resp = client.post("/api/queues/emails/rate-limit", json={"rate": None})
    assert resp.json() == {"rate": None, "queue": "emails"}
    assert client.get("/api/stats").json()["rate_limits"] == {}
