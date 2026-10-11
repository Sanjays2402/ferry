"""Tests for the Redis broker, run against fakeredis with real Lua execution."""

from datetime import datetime, timedelta, timezone

import pytest

fakeredis = pytest.importorskip("fakeredis")

from ferry.redis_broker import RedisBroker  # noqa: E402


@pytest.fixture()
def broker():
    client = fakeredis.FakeRedis(decode_responses=True)
    return RedisBroker(client=client)


def test_enqueue_claim_priority(broker):
    broker.enqueue("t.low", [], {}, priority=0)
    broker.enqueue("t.high", [], {}, priority=10)
    assert broker.claim(["default"], "w")["task_name"] == "t.high"
    assert broker.claim(["default"], "w")["task_name"] == "t.low"
    assert broker.claim(["default"], "w") is None


def test_no_double_claim(broker):
    tid = broker.enqueue("t", [], {})
    assert broker.claim(["default"], "w1")["id"] == tid
    assert broker.claim(["default"], "w2") is None


def test_eta_visibility(broker):
    broker.enqueue("t.future", [], {}, eta=datetime.now(timezone.utc) + timedelta(hours=1))
    assert broker.claim(["default"], "w") is None
    tid = broker.enqueue("t.past", [], {}, eta=datetime.now(timezone.utc) - timedelta(seconds=1))
    # the claim Lua script moves due delayed tasks into the ready set
    assert broker.claim(["default"], "w")["id"] == tid


def test_done_and_counters(broker):
    tid = broker.enqueue("t", [], {})
    task = broker.claim(["default"], "w")
    broker.mark_running(task["id"])
    broker.ack_done(task["id"], {"ok": True})
    assert broker.get_task(tid)["status"] == "done"
    assert broker.decode_result(broker.get_task(tid)) == {"ok": True}
    stats = broker.stats()
    assert stats["tasks"]["done"] == 1
    assert stats["tasks"]["queued"] == 0
    assert stats["tasks"]["claimed"] == 0
    assert stats["tasks"]["running"] == 0


def test_retry_then_dead(broker):
    tid = broker.enqueue("t", [], {}, max_retries=0)
    task = broker.claim(["default"], "w")
    assert broker.ack_failed(task["id"], "boom", None) is False
    assert broker.get_task(tid)["status"] == "dead"
    assert broker.stats()["tasks"]["dead"] == 1
    assert broker.retry_task(tid) is True
    assert broker.get_task(tid)["status"] == "queued"


def test_scheduled_id_dedup(broker):
    a = broker.enqueue("t", [], {}, scheduled_id="s@1")
    b = broker.enqueue("t", [], {}, scheduled_id="s@1")
    assert a == b
    assert broker.get_task_by_scheduled_id("s@1")["id"] == a


def test_heartbeat_workers(broker):
    broker.heartbeat("w1", ["default"], 4, "h")
    assert len(broker.list_workers()) == 1
    broker.remove_worker("w1")
    assert broker.list_workers() == []


def test_stale_claim_recovery(broker):
    tid = broker.enqueue("t", [], {})
    broker.claim(["default"], "ghost")  # never heartbeats
    assert broker.requeue_stale_claims(older_than_seconds=-1) == 1
    assert broker.get_task(tid)["status"] == "queued"


def test_purge(broker):
    broker.enqueue("t", [], {})
    broker.enqueue("t", [], {}, queue="emails")
    assert broker.purge() == 2  # no queue filter: purges everything queued
    assert broker.stats()["tasks"]["queued"] == 0

    broker.enqueue("t", [], {})
    broker.enqueue("t", [], {}, queue="emails")
    assert broker.purge(queue="emails") == 1
    assert broker.stats()["tasks"]["queued"] == 1


def test_worker_end_to_end(broker):
    import time

    from ferry import Ferry
    from ferry.worker import Worker

    app = Ferry("redis-e2e", broker=broker)

    @app.task(max_retries=2, retry_backoff_base=0.01, retry_backoff_max=0.05)
    def add(a, b):
        return a + b

    result = add.delay(20, 22)
    worker = Worker(app, concurrency=2, poll_interval=0.01)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not result.ready():
        worker.run_once(timeout=0.5)
        time.sleep(0.02)
    assert result.get(timeout=1) == 42
    assert broker.stats()["tasks"]["done"] == 1


def test_expire_task_and_stats(broker):
    tid = broker.enqueue("t", [], {})
    assert broker.expire_task(tid) is True
    t = broker.get_task(tid)
    assert t["status"] == "expired" and t["finished_at"]
    assert broker.claim(["default"], "w") is None  # removed from the ready set
    stats = broker.stats()
    assert stats["tasks"]["expired"] == 1 and stats["tasks"]["queued"] == 0
    assert broker.expire_task(tid) is False  # already terminal


def test_expire_claimed_but_not_running_task(broker):
    tid = broker.enqueue("t", [], {})
    broker.claim(["default"], "w")
    assert broker.expire_task(tid) is True
    assert broker.mark_running(tid) is False
    assert broker.stats()["tasks"]["claimed"] == 0


def test_cannot_expire_running_task(broker):
    tid = broker.enqueue("t", [], {})
    broker.claim(["default"], "w")
    broker.mark_running(tid)
    assert broker.expire_task(tid) is False


def test_expires_at_roundtrip(broker):
    when = datetime.now(timezone.utc) + timedelta(minutes=5)
    with_deadline = broker.enqueue("t", [], {}, expires=when)
    without = broker.enqueue("t", [], {})
    from ferry.broker import is_expired

    assert not is_expired(broker.get_task(with_deadline))
    assert broker.get_task(without)["expires_at"] == ""
    assert not is_expired(broker.get_task(without))


def test_progress_only_while_running(broker):
    tid = broker.enqueue("t", [], {})
    assert broker.set_progress(tid, {"current": 1}) is False
    assert broker.decode_progress(broker.get_task(tid)) is None
    broker.claim(["default"], "w")
    broker.mark_running(tid)
    assert broker.set_progress(tid, {"current": 3, "total": 4}) is True
    assert broker.decode_progress(broker.get_task(tid)) == {"current": 3, "total": 4}


def test_progress_cleared_on_retry(broker):
    tid = broker.enqueue("t", [], {})
    broker.claim(["default"], "w")
    broker.mark_running(tid)
    broker.set_progress(tid, {"current": 1})
    broker.ack_failed(tid, "boom", None)
    assert broker.decode_progress(broker.get_task(tid)) == {"current": 1}
    assert broker.retry_task(tid)
    assert broker.decode_progress(broker.get_task(tid)) is None


def test_worker_expiry_and_progress_end_to_end(broker):
    import time

    from ferry import Ferry, TaskExpired, update_progress
    from ferry.worker import Worker

    app = Ferry("redis-expiry", broker=broker)
    calls = []

    @app.task
    def job(x):
        calls.append(x)
        update_progress(x, total=10, message="working")
        return x

    stale = job.apply_async(args=(1,), expires=0.05)
    time.sleep(0.15)
    fresh = job.apply_async(args=(5,), expires=60)
    Worker(app, concurrency=2, poll_interval=0.01).run_once(timeout=5)
    assert calls == [5]
    assert stale.status == "expired"
    with pytest.raises(TaskExpired):
        stale.get(timeout=1)
    assert fresh.get(timeout=1) == 5
    assert fresh.progress["percent"] == 50.0
