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
