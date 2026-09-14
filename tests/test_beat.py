from datetime import datetime, timezone

from ferry.scheduler import Beat


def test_periodic_enqueue_and_dedup(app):
    ran = []

    @app.periodic("* * * * *")
    def every_minute():
        ran.append(1)

    beat = Beat(app)
    now = datetime(2026, 9, 13, 10, 5, tzinfo=timezone.utc)
    assert beat.tick_once(now) == 1
    assert beat.tick_once(now) == 0  # idempotent: same slot never enqueues twice
    assert len(app.broker.list_tasks()) == 1
    # even after the first instance finishes, the slot must not run again
    task = app.broker.list_tasks()[0]
    app.broker.ack_done(task["id"], "ok")
    assert beat.tick_once(now) == 0
    assert len(app.broker.list_tasks()) == 1
    later = datetime(2026, 9, 13, 10, 6, tzinfo=timezone.utc)
    assert beat.tick_once(later) == 1
    assert len(app.broker.list_tasks()) == 2


def test_periodic_skips_non_matching(app):
    @app.periodic("0 0 * * *")
    def midnight():
        pass

    beat = Beat(app)
    noon = datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc)
    assert beat.tick_once(noon) == 0
    assert app.broker.list_tasks() == []


def test_stale_claim_recovery(app):
    tid = app.broker.enqueue("t", [], {})
    claimed = app.broker.claim(["default"], "ghost-worker")  # never heartbeats
    assert claimed["id"] == tid
    recovered = app.broker.requeue_stale_claims(older_than_seconds=-1)
    assert recovered == 1
    assert app.broker.get_task(tid)["status"] == "queued"
