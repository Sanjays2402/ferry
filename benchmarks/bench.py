"""Ferry benchmarks: end-to-end task throughput on the SQLite broker.

Measures wall-clock tasks/second for tiny (no-op-ish) tasks with a warm
worker, which is the number that matters for queue overhead (not your task's
own runtime).

    python benchmarks/bench.py [--tasks N] [--concurrency C]

Results on the author's machine are recorded in README.md.
"""

from __future__ import annotations

import argparse
import tempfile
import time

from ferry import Ferry
from ferry.worker import Worker


def bench(tasks: int, concurrency: int) -> dict:
    tmp = tempfile.mkdtemp()
    app = Ferry("bench", broker=f"sqlite:///{tmp}/bench.db")

    @app.task
    def noop(i: int) -> int:
        return i

    results = [noop.delay(i) for i in range(tasks)]
    worker = Worker(app, concurrency=concurrency, poll_interval=0.001)

    start = time.perf_counter()
    deadline = start + 300
    pending = set(results)
    while pending and time.perf_counter() < deadline:
        worker.run_once(timeout=1.0)
        pending = {r for r in pending if not r.ready()}
    elapsed = time.perf_counter() - start
    assert not pending, f"{len(pending)} tasks never finished"
    # sanity: every result round-trips
    assert sum(r.get() for r in results) == sum(range(tasks))
    return {"tasks": tasks, "concurrency": concurrency,
            "seconds": round(elapsed, 2), "per_second": round(tasks / elapsed)}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--tasks", type=int, default=2000)
    p.add_argument("--concurrency", type=int, default=8)
    args = p.parse_args()
    r = bench(args.tasks, args.concurrency)
    print(f"{r['tasks']} tasks, concurrency={r['concurrency']}: "
          f"{r['seconds']}s  ->  {r['per_second']:,} tasks/sec")


if __name__ == "__main__":
    main()
