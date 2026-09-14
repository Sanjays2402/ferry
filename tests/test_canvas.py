"""Tests for task canvases: chain, group, chord (SQLite + Redis)."""

import time

import pytest

from ferry import Ferry, TaskFailed
from ferry.worker import Worker


def drive_until(worker, result, timeout=15.0):
    """Run worker passes until the canvas result is ready."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result.ready():
            return
        worker.run_once(timeout=0.5)
        time.sleep(0.02)
    raise TimeoutError(f"canvas result not ready after {timeout}s")


@pytest.fixture()
def tasks(app):
    @app.task
    def add(x, y):
        return x + y

    @app.task
    def double(x):
        return x * 2

    @app.task
    def total(xs):
        return sum(xs)

    @app.task
    def combine(results, bonus):
        return sum(results) + bonus

    @app.task(max_retries=0)
    def boom(x):
        raise RuntimeError("boom")

    return {"add": add, "double": double, "total": total,
            "combine": combine, "boom": boom}


# -- chain --------------------------------------------------------------------


def test_chain_three_links(app, worker, tasks):
    res = app.chain(tasks["add"].s(2, 3), tasks["double"].s(), tasks["add"].s(y=100))
    r = res.apply_async()
    drive_until(worker, r)
    # add(2, 3) = 5 -> double(5) = 10 -> add(10, y=100) = 110
    assert r.get(timeout=10) == 110


def test_chain_single_signature(app, worker, tasks):
    r = app.chain(tasks["double"].s(21)).apply_async()
    drive_until(worker, r)
    assert r.get(timeout=10) == 42


def test_chain_immutable_link_ignores_result(app, worker, tasks):
    # .si(7): the previous result is NOT passed; double(7) == 14
    r = app.chain(tasks["add"].s(2, 3), tasks["double"].si(7)).apply_async()
    drive_until(worker, r)
    assert r.get(timeout=10) == 14


def test_chain_stops_on_failure(app, worker, tasks):
    r = app.chain(tasks["boom"].s(1), tasks["double"].s()).apply_async()
    drive_until(worker, app.AsyncResult(r.task_ids[0]))
    with pytest.raises(TaskFailed):
        r.get(timeout=10)
    # the second link was never enqueued
    assert app.broker.get_task(r.task_ids[1]) is None


def test_chain_empty_rejected(app):
    with pytest.raises(ValueError):
        app.chain()


def test_signature_set_options(app, tasks):
    sig = tasks["add"].s(1, 2).set(queue="math", priority=9)
    r = sig.apply_async()
    task = app.broker.get_task(r.task_id)
    assert task["queue"] == "math"
    assert task["priority"] == 9


def test_signature_s_prepends_args(app, tasks):
    sig = tasks["add"].s(2, 3).s(10)
    assert sig.args == (10, 2, 3)


# -- group --------------------------------------------------------------------


def test_group_results_in_order(app, worker, tasks):
    r = app.group(
        tasks["double"].s(1), tasks["double"].s(2), tasks["double"].s(3),
    ).apply_async()
    drive_until(worker, r)
    assert r.get(timeout=10) == [2, 4, 6]


def test_group_failure_propagates(app, worker, tasks):
    r = app.group(tasks["double"].s(1), tasks["boom"].s(2)).apply_async()
    with pytest.raises(TaskFailed):
        drive_until(worker, r)
        r.get(timeout=10)


def test_group_empty(app, worker):
    r = app.group().apply_async()
    assert r.get(timeout=5) == []


# -- chord --------------------------------------------------------------------


def test_chord(app, worker, tasks):
    r = app.chord(
        [tasks["add"].s(1, 2), tasks["add"].s(3, 4), tasks["double"].s(5)],
        tasks["total"].s(),
    ).apply_async()
    drive_until(worker, r)
    assert r.get(timeout=10) == 3 + 7 + 10


def test_chord_body_keeps_own_args(app, worker, tasks):
    # results list is prepended to the body's own args
    r = app.chord(
        [tasks["add"].s(1, 2), tasks["double"].s(5)],
        tasks["combine"].s(bonus=100),
    ).apply_async()
    drive_until(worker, r)
    assert r.get(timeout=10) == (3 + 10) + 100


def test_chord_header_failure_stalls_body(app, worker, tasks):
    r = app.chord(
        [tasks["double"].s(1), tasks["boom"].s(2)],
        tasks["total"].s(),
    ).apply_async()
    drive_until(worker, app.AsyncResult(r.header_ids[1]))
    with pytest.raises(TaskFailed):
        r.get(timeout=10)
    # body never ran: it was never even enqueued
    assert app.broker.get_task(r.body_id) is None


def test_chord_empty_header_rejected(app, tasks):
    with pytest.raises(ValueError):
        app.chord([], tasks["total"].s())


# -- redis --------------------------------------------------------------------

fakeredis = pytest.importorskip("fakeredis")


@pytest.fixture()
def redis_app(tmp_path):
    from ferry.redis_broker import RedisBroker

    client = fakeredis.FakeRedis(decode_responses=True)
    return Ferry("test-redis", broker=RedisBroker(client=client))


@pytest.fixture()
def redis_tasks(redis_app):
    @redis_app.task
    def add(x, y):
        return x + y

    @redis_app.task
    def double(x):
        return x * 2

    @redis_app.task
    def total(xs):
        return sum(xs)

    return {"add": add, "double": double, "total": total}


@pytest.fixture()
def redis_worker(redis_app):
    return Worker(redis_app, concurrency=2, poll_interval=0.01)


def test_redis_chain(redis_app, redis_worker, redis_tasks):
    r = redis_app.chain(
        redis_tasks["add"].s(2, 3), redis_tasks["double"].s()
    ).apply_async()
    drive_until(redis_worker, r)
    assert r.get(timeout=10) == 10


def test_redis_group(redis_app, redis_worker, redis_tasks):
    r = redis_app.group(
        redis_tasks["double"].s(1), redis_tasks["double"].s(2)
    ).apply_async()
    drive_until(redis_worker, r)
    assert r.get(timeout=10) == [2, 4]


def test_redis_chord(redis_app, redis_worker, redis_tasks):
    r = redis_app.chord(
        [redis_tasks["add"].s(1, 2), redis_tasks["double"].s(5)],
        redis_tasks["total"].s(),
    ).apply_async()
    drive_until(redis_worker, r)
    assert r.get(timeout=10) == 13
