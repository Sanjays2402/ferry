from datetime import datetime, timedelta, timezone


def test_enqueue_and_claim_roundtrip(app):
    task_id = app.broker.enqueue("m.add", [1, 2], {})
    claimed = app.broker.claim(["default"], "w1")
    assert claimed is not None
    assert claimed["id"] == task_id
    assert claimed["task_name"] == "m.add"
    assert app.broker.decode_args(claimed) == ([1, 2], {})
    # second claim finds nothing: the task is already claimed
    assert app.broker.claim(["default"], "w2") is None


def test_priority_ordering(app):
    app.broker.enqueue("t.low", [], {}, priority=0)
    app.broker.enqueue("t.high", [], {}, priority=10)
    app.broker.enqueue("t.mid", [], {}, priority=5)
    order = [app.broker.claim(["default"], "w")["task_name"] for _ in range(3)]
    assert order == ["t.high", "t.mid", "t.low"]


def test_fifo_within_priority(app):
    first = app.broker.enqueue("t", [], {}, priority=1)
    second = app.broker.enqueue("t", [], {}, priority=1)
    assert app.broker.claim(["default"], "w")["id"] == first
    assert app.broker.claim(["default"], "w")["id"] == second


def test_eta_hides_task_until_due(app):
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    app.broker.enqueue("t.future", [], {}, eta=future)
    assert app.broker.claim(["default"], "w") is None
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    tid = app.broker.enqueue("t.past", [], {}, eta=past)
    assert app.broker.claim(["default"], "w")["id"] == tid


def test_queue_isolation(app):
    app.broker.enqueue("t.a", [], {}, queue="emails")
    assert app.broker.claim(["default"], "w") is None
    assert app.broker.claim(["emails"], "w")["task_name"] == "t.a"


def test_ack_done_stores_result(app):
    tid = app.broker.enqueue("t", [], {})
    task = app.broker.claim(["default"], "w")
    app.broker.mark_running(task["id"])
    app.broker.ack_done(task["id"], {"ok": True})
    stored = app.broker.get_task(tid)
    assert stored["status"] == "done"
    assert app.broker.decode_result(stored) == {"ok": True}


def test_retry_requeues_and_dead_after_exhaustion(app):
    tid = app.broker.enqueue("t", [], {}, max_retries=1)
    task = app.broker.claim(["default"], "w")
    soon = datetime.now(timezone.utc) + timedelta(seconds=60)
    assert app.broker.ack_failed(task["id"], "boom", soon) is True
    stored = app.broker.get_task(tid)
    assert stored["status"] == "queued" and stored["attempts"] == 1
    # not visible before the backoff eta
    assert app.broker.claim(["default"], "w") is None
    # force visibility by clearing the eta, then fail the final attempt
    app.broker._connect().execute("UPDATE ferry_tasks SET eta=NULL WHERE id=?", (tid,))
    app.broker._connect().commit()
    task = app.broker.claim(["default"], "w")
    assert app.broker.ack_failed(task["id"], "boom again", None) is False
    assert app.broker.get_task(tid)["status"] == "dead"


def test_scheduled_id_dedup(app):
    a = app.broker.enqueue("t", [], {}, scheduled_id="nightly@20260913")
    b = app.broker.enqueue("t", [], {}, scheduled_id="nightly@20260913")
    assert a == b
    c = app.broker.enqueue("t", [], {}, scheduled_id="nightly@20260914")
    assert c != a


def test_stats_and_throughput(app):
    app.broker.enqueue("t", [], {})
    app.broker.enqueue("t", [], {}, queue="emails")
    stats = app.broker.stats()
    assert stats["tasks"]["queued"] == 2
    assert {q["queue"] for q in stats["queues"]} == {"default", "emails"}
    assert app.broker.throughput(minutes=60) == []


def test_retry_task_and_purge(app):
    tid = app.broker.enqueue("t", [], {})
    task = app.broker.claim(["default"], "w")
    app.broker.ack_failed(task["id"], "x", None)
    assert app.broker.retry_task(tid) is True
    assert app.broker.get_task(tid)["status"] == "queued"
    assert app.broker.purge() == 1
    assert app.broker.stats()["tasks"]["queued"] == 0


def test_heartbeat_and_workers(app):
    app.broker.heartbeat("w1", ["default"], 4, "host1")
    workers = app.broker.list_workers()
    assert len(workers) == 1 and workers[0]["worker_id"] == "w1"
    app.broker.remove_worker("w1")
    assert app.broker.list_workers() == []
