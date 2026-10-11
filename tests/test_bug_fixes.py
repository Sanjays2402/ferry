"""Regression tests for bugs found in a code audit. Each test failed before its fix."""

import threading
import time
from datetime import datetime

import pytest

from ferry import Ferry
from ferry.cli import main
from ferry.cron import CronSchedule
from ferry.worker import Worker

# -- cron --------------------------------------------------------------------------


def test_cron_sunday_can_be_written_as_7():
    s = CronSchedule("0 9 * * 7")
    assert s.matches(datetime(2026, 9, 13, 9, 0))  # a Sunday
    assert not s.matches(datetime(2026, 9, 14, 9, 0))  # Monday
    assert CronSchedule("0 9 * * 5-7").matches(datetime(2026, 9, 13, 9, 0))


@pytest.mark.parametrize("expr", [
    "5, * * * *",       # trailing comma used to become a wildcard (every minute!)
    ",5 * * * *",
    "1,,2 * * * *",
    "*/-1 * * * *",     # negative step silently produced an empty schedule
    "*/0 * * * *",
    "30-10 * * * *",    # reversed range silently produced an empty schedule
    "abc * * * *",
])
def test_cron_rejects_malformed_fields(expr):
    with pytest.raises(ValueError):
        CronSchedule(expr)


def test_cron_start_with_step():
    assert CronSchedule("5/20 * * * *")._sets[0] == {5, 25, 45}


def test_cron_day_of_month_or_day_of_week_when_both_restricted():
    """Standard cron: if both fields are restricted, either may match."""
    s = CronSchedule("0 0 1 * 1")  # the 1st of the month OR any Monday
    assert s.matches(datetime(2026, 10, 1, 0, 0))    # Thursday the 1st
    assert s.matches(datetime(2026, 10, 5, 0, 0))    # Monday the 5th
    assert not s.matches(datetime(2026, 10, 6, 0, 0))  # Tuesday the 6th
    # a single restricted field still restricts on its own
    assert not CronSchedule("0 0 1 * *").matches(datetime(2026, 10, 5, 0, 0))
    assert not CronSchedule("0 0 * * 1").matches(datetime(2026, 10, 1, 0, 0))


# -- worker ------------------------------------------------------------------------


def test_unserializable_result_fails_once_instead_of_rerunning(app, worker):
    calls = []

    @app.task(max_retries=3, retry_backoff_base=0.01, retry_backoff_max=0.02)
    def job():
        calls.append(1)
        return object()  # cannot be stored

    r = job.delay()
    worker.run_once(timeout=3)
    time.sleep(0.1)
    worker.run_once(timeout=3)
    assert len(calls) == 1, "a task with an unstorable result must not be re-executed"
    assert r.status == "dead"
    task = app.broker.get_task(r.task_id)
    assert "serialize" in task["error"]


def test_backoff_survives_huge_attempt_counts(app):
    @app.task(max_retries=10**6)
    def job():
        pass

    w = Worker(app)
    delay = w._backoff({"task_name": job.name}, attempts=5000, entry=job)
    assert 0 < delay <= job.retry_backoff_max


def test_shutdown_is_bounded_by_shutdown_timeout(app):
    started, release = threading.Event(), threading.Event()

    @app.task
    def hang():
        started.set()
        release.wait(10)

    hang.delay()
    w = Worker(app, concurrency=1, poll_interval=0.01, shutdown_timeout=0.3)
    t = threading.Thread(target=w.run, daemon=True)
    t.start()
    try:
        assert started.wait(5)
        w.stop()
        t.join(timeout=3)
        assert not t.is_alive(), "worker ignored shutdown_timeout and waited for the hung task"
        assert w.abandoned == 1
    finally:
        release.set()
        t.join(timeout=5)


def test_worker_does_not_claim_more_than_its_concurrency(app):
    """A busy worker must leave the backlog for others (and for higher priorities)."""
    started, release = threading.Event(), threading.Event()

    @app.task
    def slow():
        started.set()
        release.wait(10)

    for _ in range(6):
        slow.delay()
    w = Worker(app, concurrency=2, poll_interval=0.01)
    t = threading.Thread(target=w.run, daemon=True)
    t.start()
    try:
        assert started.wait(5)
        time.sleep(0.4)  # plenty of time for a greedy poll loop to over-claim
        counts = app.broker.stats()["tasks"]
        assert counts["claimed"] + counts["running"] <= 2
        assert counts["queued"] >= 4
    finally:
        release.set()
        w.stop()
        t.join(timeout=10)


# -- canvas ------------------------------------------------------------------------


def test_chain_may_reuse_the_same_signature_object(app, worker):
    @app.task
    def add(x, y):
        return x + y

    @app.task
    def double(x):
        return x * 2

    d = double.s()
    result = app.chain(add.s(1, 1), d, d).apply_async()  # 2 -> 4 -> 8
    assert len(set(result.task_ids)) == 3
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not result.ready():
        worker.run_once(timeout=0.5)
    assert result.get(timeout=2) == 8


def test_in_memory_broker_works_across_worker_threads():
    app = Ferry("mem", broker="sqlite:///:memory:")

    @app.task(max_retries=0)
    def add(a, b):
        return a + b

    results = [add.delay(i, 1) for i in range(5)]
    Worker(app, concurrency=3, poll_interval=0.01).run_once(timeout=5)
    assert [r.get(timeout=2) for r in results] == [1, 2, 3, 4, 5]


def test_two_in_memory_brokers_are_independent():
    a, b = Ferry("a", broker="sqlite:///:memory:"), Ferry("b", broker="sqlite:///:memory:")
    a.broker.enqueue("t", [], {})
    assert a.broker.stats()["tasks"]["queued"] == 1
    assert b.broker.stats()["tasks"]["queued"] == 0


# -- dashboard / CLI input validation ----------------------------------------------


def test_dashboard_rejects_bad_rate_with_400(tmp_path):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from ferry.dashboard import create_app

    client = TestClient(create_app(f"sqlite:///{tmp_path}/d.db"), raise_server_exceptions=False)
    r = client.post("/api/queues/q/rate-limit", json={"rate": "fast"})
    assert r.status_code == 400
    assert "bad rate" in r.json()["detail"]
    assert client.post("/api/queues/q/rate-limit", json={"rate": "5/m"}).json()["rate"] == "5/m"
    assert client.post("/api/queues/q/rate-limit", json={"rate": None}).json()["rate"] is None


def test_cli_bad_rate_is_a_clean_error(tmp_path):
    url = f"sqlite:///{tmp_path}/c.db"
    Ferry("c", broker=url)
    with pytest.raises(SystemExit) as exc:
        main(["rate-limit", "q", "fast", "--broker", url])
    assert "bad rate" in str(exc.value)


# -- redis -------------------------------------------------------------------------


@pytest.fixture()
def redis_broker():
    fakeredis = pytest.importorskip("fakeredis")
    from ferry.redis_broker import RedisBroker

    return RedisBroker(client=fakeredis.FakeRedis(decode_responses=True))


def test_redis_queue_names_may_contain_colons(redis_broker):
    redis_broker.enqueue("t", [], {}, queue="emails:high")
    redis_broker.enqueue("t", [], {}, queue="later:slow",
                         eta=datetime(2999, 1, 1).astimezone())
    names = {q["queue"] for q in redis_broker.stats()["queues"]}
    assert names == {"emails:high", "later:slow"}


def test_redis_list_tasks_returns_the_newest(redis_broker):
    ids = [redis_broker.enqueue("t", [], {}) for _ in range(25)]
    for i, tid in enumerate(ids):  # make creation order unambiguous
        redis_broker._r.hset(redis_broker._k("task", tid), "created_at",
                             f"2026-01-01T00:00:{i:02d}+00:00")
    newest = redis_broker.list_tasks(limit=5)
    assert [t["id"] for t in newest] == ids[::-1][:5]


def test_redis_heartbeat_keeps_original_started_at(redis_broker):
    redis_broker.heartbeat("w1", ["default"], 2, "host")
    first = redis_broker.list_workers()[0]
    time.sleep(0.02)
    redis_broker.heartbeat("w1", ["default"], 2, "host")
    second = redis_broker.list_workers()[0]
    assert second["started_at"] == first["started_at"]
    assert second["last_beat"] >= first["last_beat"]
