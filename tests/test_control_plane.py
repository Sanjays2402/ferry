"""Control-plane features: pause/resume, dedupe keys, result TTL, bulk retry.

Each behavior is tested against both brokers (SQLite + fakeredis).
"""

import time
from datetime import datetime, timedelta, timezone

import pytest

from ferry import Ferry, ResultExpired
from ferry.worker import Worker

fakeredis = pytest.importorskip("fakeredis", reason="fakeredis not installed")

from ferry.redis_broker import RedisBroker  # noqa: E402


@pytest.fixture(params=["sqlite", "redis"])
def any_app(request, tmp_path):
    if request.param == "sqlite":
        return Ferry("test", broker=f"sqlite:///{tmp_path}/ctl.db")
    return Ferry("test", broker=RedisBroker(client=fakeredis.FakeRedis(decode_responses=True)))


def run_all(worker, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        n = worker.run_once(timeout=0.3)
        if n == 0:
            return
    raise TimeoutError("worker did not drain")


# -- queue pause/resume -----------------------------------------------------------

def test_pause_resume_broker(any_app):
    b = any_app.broker
    assert b.paused_queues() == []
    b.pause_queue("emails")
    b.pause_queue("bulk")
    assert b.paused_queues() == ["bulk", "emails"]
    b.resume_queue("emails")
    assert b.paused_queues() == ["bulk"]
    b.resume_queue("bulk")
    assert b.paused_queues() == []


def test_paused_queue_blocks_worker(any_app):
    app = any_app

    @app.task(queue="emails")
    def send(to):
        return to

    app.broker.pause_queue("emails")
    w = Worker(app, queues=["emails"], concurrency=2, poll_interval=0.01)
    r = send.delay("a@b.c")
    assert w.run_once(timeout=1.0) == 0
    assert app.AsyncResult(r.task_id).status == "queued"

    app.broker.resume_queue("emails")
    w2 = Worker(app, queues=["emails"], concurrency=2, poll_interval=0.01)
    run_all(w2)
    assert r.get(timeout=5) == "a@b.c"


def test_pause_does_not_block_other_queues(any_app):
    app = any_app

    @app.task(queue="emails")
    def send(to):
        return to

    @app.task(queue="default")
    def ping():
        return "pong"

    app.broker.pause_queue("emails")
    r1 = send.delay("a@b.c")
    r2 = ping.delay()
    w = Worker(app, queues=["emails", "default"], concurrency=2, poll_interval=0.01)
    run_all(w)
    assert r2.get(timeout=5) == "pong"
    assert app.AsyncResult(r1.task_id).status == "queued"


# -- dedupe keys ------------------------------------------------------------------

def test_dedupe_key_collapses_pending(any_app):
    app = any_app

    @app.task
    def job(x):
        return x * 2

    r1 = job.apply_async(args=(21,), dedupe_key="report:42")
    r2 = job.apply_async(args=(21,), dedupe_key="report:42")
    assert r1.task_id == r2.task_id
    assert app.broker.stats()["tasks"]["queued"] == 1


def test_dedupe_key_frees_after_done(any_app):
    app = any_app

    @app.task
    def job(x):
        return x * 2

    r1 = job.apply_async(args=(21,), dedupe_key="report:42")
    w = Worker(app, concurrency=2, poll_interval=0.01)
    run_all(w)
    assert r1.get(timeout=5) == 42
    r2 = job.apply_async(args=(21,), dedupe_key="report:42")
    assert r2.task_id != r1.task_id


def test_dedupe_key_distinct_keys_enqueue(any_app):
    app = any_app

    @app.task
    def job(x):
        return x

    r1 = job.apply_async(args=(1,), dedupe_key="a")
    r2 = job.apply_async(args=(2,), dedupe_key="b")
    assert r1.task_id != r2.task_id
    assert app.broker.stats()["tasks"]["queued"] == 2


# -- result TTL -------------------------------------------------------------------

def test_expire_results(any_app):
    app = any_app
    b = app.broker

    @app.task
    def job():
        return "payload"

    r = job.delay()
    w = Worker(app, concurrency=2, poll_interval=0.01)
    run_all(w)
    assert r.get(timeout=5) == "payload"

    # make the finished row look old, then expire
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    if hasattr(b, "_connect"):  # sqlite
        b._connect().execute(
            "UPDATE ferry_tasks SET finished_at=? WHERE id=?", (old, r.task_id)
        )
        b._connect().commit()
    else:  # redis
        old_ts = (datetime.now(timezone.utc) - timedelta(hours=2)).timestamp()
        b._r.hset(b._k("task", r.task_id), mapping={"finished_at": old})
        b._r.zadd(b._k("finished"), {f"{r.task_id}:done": old_ts})
    assert b.expire_results(3600) == 1
    with pytest.raises(ResultExpired):
        r.get(timeout=5)
    # row still exists for history
    assert b.get_task(r.task_id)["status"] == "done"


def test_expire_results_keeps_fresh(any_app):
    app = any_app
    b = app.broker

    @app.task
    def job():
        return "fresh"

    r = job.delay()
    w = Worker(app, concurrency=2, poll_interval=0.01)
    run_all(w)
    assert b.expire_results(3600) == 0
    assert r.get(timeout=5) == "fresh"


def test_beat_expires_results_via_app_ttl(tmp_path):
    from ferry.scheduler import Beat

    app = Ferry("test", broker=f"sqlite:///{tmp_path}/ttl.db", result_ttl=3600)

    @app.task
    def job():
        return "x"

    r = job.delay()
    w = Worker(app, concurrency=2, poll_interval=0.01)
    run_all(w)
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    app.broker._connect().execute(
        "UPDATE ferry_tasks SET finished_at=? WHERE id=?", (old, r.task_id)
    )
    app.broker._connect().commit()
    Beat(app).tick_once()
    with pytest.raises(ResultExpired):
        r.get(timeout=5)


# -- bulk retry -------------------------------------------------------------------

def test_retry_dead(any_app):
    app = any_app
    b = app.broker

    @app.task(max_retries=0)
    def boom():
        raise RuntimeError("kaput")

    @app.task(queue="other", max_retries=0)
    def boom2():
        raise RuntimeError("kaput")

    r1 = boom.delay()
    r2 = boom2.delay()
    w = Worker(app, queues=["default", "other"], concurrency=2, poll_interval=0.01)
    run_all(w)
    assert r1.status == "dead" and r2.status == "dead"

    assert b.retry_dead(queue="default") == 1
    assert app.AsyncResult(r1.task_id).status == "queued"
    assert app.AsyncResult(r2.task_id).status == "dead"
    assert b.retry_dead() == 1
    assert app.AsyncResult(r2.task_id).status == "queued"


def test_retry_dead_empty(any_app):
    assert any_app.broker.retry_dead() == 0


# -- stats carries paused queues ----------------------------------------------------

def test_stats_includes_paused(any_app):
    b = any_app.broker
    b.pause_queue("emails")
    assert b.stats()["paused"] == ["emails"]
