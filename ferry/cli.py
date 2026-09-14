"""Command-line interface: ``ferry worker``, ``ferry beat``, ``ferry dashboard``.

Every command takes an app reference ``module:app`` and a broker URL, e.g.::

    ferry worker myapp:app --broker sqlite:///ferry.db --concurrency 8
    ferry beat myapp:app
    ferry dashboard --port 8000
"""

from __future__ import annotations

import argparse
import importlib
import logging
import sys


def load_app(ref: str):
    if ":" in ref:
        module_name, attr = ref.split(":", 1)
    else:
        module_name, attr = ref, "app"
    sys.path.insert(0, "")
    module = importlib.import_module(module_name)
    app = getattr(module, attr)
    return app


def cmd_worker(args) -> int:
    from .worker import Worker

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    app = load_app(args.app)
    if args.broker:
        from .broker import open_broker

        app.broker = open_broker(args.broker)
    Worker(
        app,
        queues=args.queues,
        concurrency=args.concurrency,
        worker_id=args.worker_id,
    ).run()
    return 0


def cmd_beat(args) -> int:
    from .scheduler import Beat

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    app = load_app(args.app)
    Beat(app).run()
    return 0


def cmd_dashboard(args) -> int:
    from .dashboard import run_dashboard

    print(f"ferry dashboard on http://{args.host}:{args.port}")
    run_dashboard(args.broker, host=args.host, port=args.port)
    return 0


def cmd_stats(args) -> int:
    from .broker import open_broker

    stats = open_broker(args.broker).stats()
    t = stats["tasks"]
    print(f"queued={t['queued']} active={t['claimed'] + t['running']} "
          f"done={t['done']} dead={t['dead'] + t['failed']} workers={len(stats['workers'])}")
    for q in stats["queues"]:
        print(f"  queue {q['queue']!r}: queued={q['queued']} active={q['active']} dead={q['dead']}")
    return 0


def cmd_purge(args) -> int:
    from .broker import open_broker

    n = open_broker(args.broker).purge(queue=args.queue)
    print(f"purged {n} queued task(s)" + (f" from {args.queue!r}" if args.queue else ""))
    return 0


def cmd_pause(args) -> int:
    from .broker import open_broker

    open_broker(args.broker).pause_queue(args.queue)
    print(f"paused queue {args.queue!r}: workers will skip it until resumed")
    return 0


def cmd_resume(args) -> int:
    from .broker import open_broker

    open_broker(args.broker).resume_queue(args.queue)
    print(f"resumed queue {args.queue!r}")
    return 0


def cmd_retry_dead(args) -> int:
    from .broker import open_broker

    n = open_broker(args.broker).retry_dead(queue=args.queue)
    print(f"requeued {n} failed/dead task(s)" + (f" from {args.queue!r}" if args.queue else ""))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ferry", description="Ferry — lightweight distributed task queue"
    )
    sub = p.add_subparsers(dest="command", required=True)

    w = sub.add_parser("worker", help="run a worker")
    w.add_argument("app", help="app reference, e.g. myapp:app")
    w.add_argument("--broker", default=None)
    w.add_argument("--queues", nargs="+", default=["default"])
    w.add_argument("--concurrency", type=int, default=4)
    w.add_argument("--worker-id", default=None)
    w.set_defaults(func=cmd_worker)

    b = sub.add_parser("beat", help="run the scheduler (one per broker)")
    b.add_argument("app", help="app reference, e.g. myapp:app")
    b.set_defaults(func=cmd_beat)

    d = sub.add_parser("dashboard", help="run the live dashboard")
    d.add_argument("--broker", default="sqlite:///ferry.db")
    d.add_argument("--host", default="127.0.0.1")
    d.add_argument("--port", type=int, default=8000)
    d.set_defaults(func=cmd_dashboard)

    s = sub.add_parser("stats", help="print broker stats")
    s.add_argument("--broker", default="sqlite:///ferry.db")
    s.set_defaults(func=cmd_stats)

    pu = sub.add_parser("purge", help="delete queued tasks")
    pu.add_argument("--broker", default="sqlite:///ferry.db")
    pu.add_argument("--queue", default=None)
    pu.set_defaults(func=cmd_purge)

    pa = sub.add_parser("pause", help="pause a queue (workers skip it)")
    pa.add_argument("queue")
    pa.add_argument("--broker", default="sqlite:///ferry.db")
    pa.set_defaults(func=cmd_pause)

    re = sub.add_parser("resume", help="resume a paused queue")
    re.add_argument("queue")
    re.add_argument("--broker", default="sqlite:///ferry.db")
    re.set_defaults(func=cmd_resume)

    rd = sub.add_parser("retry-dead", help="requeue all failed/dead tasks")
    rd.add_argument("--broker", default="sqlite:///ferry.db")
    rd.add_argument("--queue", default=None)
    rd.set_defaults(func=cmd_retry_dead)

    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
