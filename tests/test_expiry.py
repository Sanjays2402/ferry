import time
from datetime import datetime, timedelta

import pytest

from ferry import TaskExpired
from ferry.broker import is_expired, resolve_expires
from ferry.worker import Worker


def _make_task(app, calls, **opts):
    @app.task(**opts)
    def job(x=0):
        calls.append(x)
        return x

    return job


def test_expired_task_is_skipped(app, worker):
    calls = []
    job = _make_task(app, calls)
    result = job.apply_async(args=(1,), expires=0.05)
    time.sleep(0.15)
    assert worker.run_once(timeout=2) == 1
    assert calls == []  # never executed
    assert result.status == "expired"
    assert result.ready() and not result.successful()
    with pytest.raises(TaskExpired):
        result.get(timeout=1)


def test_unexpired_task_runs_normally(app, worker):
    calls = []
    job = _make_task(app, calls)
    result = job.apply_async(args=(7,), expires=60)
    worker.run_once(timeout=2)
    assert calls == [7]
    assert result.get(timeout=1) == 7


def test_expires_accepts_datetime(app, worker):
    calls = []
    job = _make_task(app, calls)
    past = job.apply_async(args=(1,), expires=datetime.now() - timedelta(seconds=1))
    future = job.apply_async(args=(2,), expires=datetime.now() + timedelta(hours=1))
    worker.run_once(timeout=2)
    assert past.status == "expired"
    assert future.get(timeout=1) == 2
    assert calls == [2]


def test_task_level_default_and_call_override(app, worker):
    calls = []
    job = _make_task(app, calls, expires=0.05)
    default = job.apply_async(args=(1,))
    overridden = job.apply_async(args=(2,), expires=60)
    assert app.broker.get_task(default.task_id)["expires_at"]
    time.sleep(0.15)
    worker.run_once(timeout=2)
    assert default.status == "expired"
    assert overridden.get(timeout=1) == 2


def test_no_expires_means_never(app):
    calls = []
    job = _make_task(app, calls)
    r = job.delay(1)
    assert app.broker.get_task(r.task_id)["expires_at"] is None


def test_expired_event_and_stats(app, worker):
    seen = []
    app.events.on("task_expired", lambda payload: seen.append(payload["task_id"]))
    job = _make_task(app, [])
    r = job.apply_async(expires=0.05)
    time.sleep(0.15)
    worker.run_once(timeout=2)
    assert seen == [r.task_id]
    assert app.broker.stats()["tasks"]["expired"] == 1


def test_expired_does_not_stall_worker_drain(app, worker):
    """An expired skip must release its in-flight slot, or run_once never drains."""
    calls = []
    job = _make_task(app, calls)
    job.apply_async(args=(1,), expires=0.05)
    job.apply_async(args=(2,))
    time.sleep(0.15)
    start = time.monotonic()
    worker.run_once(timeout=10)
    assert time.monotonic() - start < 5
    assert calls == [2]


def test_expired_chain_link_stops_chain(app, worker):
    calls = []
    first = _make_task(app, calls)

    @app.task
    def second(prev):
        calls.append(("second", prev))
        return prev

    res = app.chain(first.s(1).set(expires=0.05), second.s()).apply_async()
    time.sleep(0.15)
    worker.run_once(timeout=2)
    assert calls == []
    assert app.broker.get_task(res.task_ids[0])["status"] == "expired"


def test_signature_expires_option(app, worker):
    calls = []
    job = _make_task(app, calls)
    r = job.s(5).set(expires=0.05).apply_async()
    time.sleep(0.15)
    worker.run_once(timeout=2)
    assert r.status == "expired"


def test_revoke_still_skips_without_stalling(app, worker):
    """Regression guard: revoked-before-start must also release its slot."""
    calls = []
    job = _make_task(app, calls)
    r = job.delay(1)
    task = app.broker.claim(["default"], "w")
    assert app.broker.revoke(r.task_id)
    w = Worker(app, concurrency=1, poll_interval=0.01)
    w._inflight = 1
    w._execute(task)
    assert w._inflight == 0
    assert calls == []


def test_resolve_expires_validation():
    assert resolve_expires(None) is None
    with pytest.raises(ValueError):
        resolve_expires(0)
    with pytest.raises(ValueError):
        resolve_expires(-5)
    with pytest.raises(TypeError):
        resolve_expires("soon")
    with pytest.raises(TypeError):
        resolve_expires(True)


def test_is_expired_helper():
    assert not is_expired({})
    assert not is_expired({"expires_at": None})
    past = (datetime.now().astimezone() - timedelta(seconds=5)).isoformat()
    future = (datetime.now().astimezone() + timedelta(seconds=60)).isoformat()
    assert is_expired({"expires_at": past})
    assert not is_expired({"expires_at": future})
