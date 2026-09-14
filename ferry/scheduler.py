"""Beat: the scheduler process.

Every tick it:
1. Enqueues due periodic tasks (cron schedules registered via ``@app.periodic``).
2. Requeues tasks claimed by workers that stopped heartbeating (crash recovery).

Run one beat per broker. It is idempotent: periodic tasks carry a
``scheduled_id`` (schedule slot), so even two beats can't double-enqueue.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from datetime import datetime, timezone

from .app import Ferry

log = logging.getLogger("ferry.beat")


class Beat:
    def __init__(self, app: Ferry, tick: float = 5.0, stale_after: float = 120.0):
        self.app = app
        self.tick = tick
        self.stale_after = stale_after
        self._stop = threading.Event()

    def run(self) -> None:
        def _handle(signum, frame):
            log.info("beat received signal %d, stopping…", signum)
            self._stop.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _handle)
            except ValueError:
                pass
        log.info("beat started: %d periodic schedules", len(self.app.periodic_schedules))
        while not self._stop.is_set():
            try:
                self.tick_once()
            except Exception:
                log.exception("beat tick failed")
            self._stop.wait(self.tick)

    def stop(self) -> None:
        self._stop.set()

    def tick_once(self, now: datetime | None = None) -> int:
        """Run one scheduling pass. Returns the number of tasks enqueued."""
        now = now or datetime.now(timezone.utc)
        enqueued = 0
        for schedule, task_name, options in self.app.periodic_schedules:
            if not schedule.matches(now.replace(second=0, microsecond=0)):
                continue
            slot = now.strftime("%Y%m%d%H%M")
            scheduled_id = f"{task_name}@{slot}"
            # one run per slot, ever: skip if this slot already ran or is pending
            if self.app.broker.get_task_by_scheduled_id(scheduled_id) is not None:
                continue
            task = self.app.registry[task_name]
            task_id = self.app.broker.enqueue(
                task_name,
                (),
                {},
                queue=options.get("queue", task.queue),
                priority=options.get("priority", task.priority),
                max_retries=options.get("max_retries", task.max_retries),
                scheduled_id=scheduled_id,
            )
            log.info("beat enqueued periodic %s (slot %s) as %s", task_name, slot, task_id[:8])
            enqueued += 1
        recovered = self.app.broker.requeue_stale_claims(self.stale_after)
        if recovered:
            log.warning("beat recovered %d stale task(s) from dead workers", recovered)
        return enqueued
