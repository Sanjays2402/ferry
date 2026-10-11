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
import os
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
    worker = Worker(
        app,
        queues=args.queues,
        concurrency=args.concurrency,
        worker_id=args.worker_id,
    )
    worker.run()
    if worker.abandoned:
        # tasks that outlived shutdown_timeout are still running in non-daemon
        # pool threads, which would keep the interpreter alive: exit for real
        logging.shutdown()
        os._exit(1)
    return 0


def cmd_beat(args) -> int:
    from .scheduler import Beat

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    app = load_app(args.app)
    Beat(app).run()
    return 0


def cmd_dashboard(args) -> int:
    from .dashboard import run_dashboard

    # --token wins; the env var keeps the secret out of the process list
    token = args.token if args.token is not None else os.environ.get("FERRY_DASHBOARD_TOKEN")
    if token is not None and not token:
        raise SystemExit("dashboard: the token must not be empty")
    if token is None and args.host not in ("127.0.0.1", "localhost", "::1"):
        print(
            f"warning: dashboard on {args.host} has no authentication; "
            "pass --token or set FERRY_DASHBOARD_TOKEN",
            file=sys.stderr,
        )
    print(f"ferry dashboard on http://{args.host}:{args.port}"
          + (" (token required)" if token else ""))
    run_dashboard(args.broker, host=args.host, port=args.port, token=token)
    return 0


def cmd_stats(args) -> int:
    from .broker import open_broker

    stats = open_broker(args.broker).stats()
    if args.json:
        import json

        print(json.dumps(stats, indent=2, default=str))
        return 0
    t = stats["tasks"]
    print(f"queued={t['queued']} active={t['claimed'] + t['running']} "
          f"done={t['done']} dead={t['dead'] + t['failed']} workers={len(stats['workers'])}")
    for q in stats["queues"]:
        print(f"  queue {q['queue']!r}: queued={q['queued']} active={q['active']} dead={q['dead']}")
    return 0


def _progress_text(broker, task: dict) -> str:
    p = broker.decode_progress(task)
    if not p:
        return "—"
    text = f"{p['percent']}%" if p.get("percent") is not None else str(p["current"])
    return text + (f" {p['message']}" if p.get("message") else "")


def cmd_tasks(args) -> int:
    from .broker import open_broker

    broker = open_broker(args.broker)
    rows = broker.list_tasks(status=args.status, queue=args.queue, limit=args.limit)
    if args.json:
        import json

        keys = ("id", "task_name", "queue", "status", "attempts", "max_retries",
                "created_at", "finished_at", "expires_at")
        out = [{k: r.get(k) or None for k in keys} | {"progress": broker.decode_progress(r)}
               for r in rows]
        print(json.dumps(out, indent=2, default=str))
        return 0
    if not rows:
        print("no tasks")
        return 0
    for r in rows:
        print(f"{r['id'][:8]}  {r['status']:<8} {r['queue']:<12} {r['task_name']}  "
              f"{r['attempts']}/{r['max_retries']}  {_progress_text(broker, r)}")
    return 0


def cmd_inspect(args) -> int:
    from .broker import open_broker

    broker = open_broker(args.broker)
    task = broker.get_task(args.task_id)
    if task is None:
        print(f"no such task: {args.task_id}", file=sys.stderr)
        return 1
    targs, tkwargs = broker.decode_args(task)
    detail = {
        **{k: (v if v not in ("", None) else None) for k, v in task.items()
           if k not in ("args", "kwargs", "result", "progress", "chain")},
        "args": targs,
        "kwargs": tkwargs,
        "result": broker.decode_result(task),
        "progress": broker.decode_progress(task),
    }
    if args.json:
        import json

        print(json.dumps(detail, indent=2, default=str))
        return 0
    width = max(len(k) for k in detail)
    for key, value in detail.items():
        if value is None:
            continue
        print(f"{key:<{width}}  {value}")
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


def cmd_revoke(args) -> int:
    from .broker import open_broker

    ok = open_broker(args.broker).revoke(args.task_id)
    print(f"revoked task {args.task_id}" if ok else
          f"task {args.task_id} could not be revoked (already started, finished, or unknown)")
    return 0 if ok else 1


def cmd_rate_limit(args) -> int:
    from .broker import open_broker

    broker = open_broker(args.broker)
    if not args.clear and args.rate is None:
        raise SystemExit("rate-limit: pass a rate like '100/s', or use --clear")
    if args.clear:
        broker.clear_rate_limit(args.queue)
        print(f"cleared rate limit on queue {args.queue!r}")
    else:
        try:
            broker.set_rate_limit(args.queue, args.rate)
        except ValueError as exc:
            raise SystemExit(f"rate-limit: {exc}") from None
        print(f"rate limit on queue {args.queue!r}: {broker.get_rate_limit(args.queue)}")
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
    d.add_argument("--token", default=None,
                   help="require this token (Bearer, or Basic-auth password); "
                        "falls back to $FERRY_DASHBOARD_TOKEN")
    d.set_defaults(func=cmd_dashboard)

    s = sub.add_parser("stats", help="print broker stats")
    s.add_argument("--broker", default="sqlite:///ferry.db")
    s.add_argument("--json", action="store_true",
                   help="emit machine-readable JSON instead of the human-readable summary")
    s.set_defaults(func=cmd_stats)

    ts = sub.add_parser("tasks", help="list recent tasks")
    ts.add_argument("--broker", default="sqlite:///ferry.db")
    ts.add_argument("--status", default=None, help="filter, e.g. running, dead, expired")
    ts.add_argument("--queue", default=None)
    ts.add_argument("--limit", type=int, default=20)
    ts.add_argument("--json", action="store_true", help="emit JSON instead of text")
    ts.set_defaults(func=cmd_tasks)

    ins = sub.add_parser("inspect", help="show one task: state, args, result, progress, error")
    ins.add_argument("task_id")
    ins.add_argument("--broker", default="sqlite:///ferry.db")
    ins.add_argument("--json", action="store_true", help="emit JSON instead of text")
    ins.set_defaults(func=cmd_inspect)

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

    rv = sub.add_parser("revoke", help="cancel a task that hasn't started yet")
    rv.add_argument("task_id")
    rv.add_argument("--broker", default="sqlite:///ferry.db")
    rv.set_defaults(func=cmd_revoke)

    rl = sub.add_parser("rate-limit", help="cap a queue's claim rate (e.g. '100/s', '10/m')")
    rl.add_argument("queue")
    rl.add_argument("rate", nargs="?", default=None,
                    help="rate like '100/s', '10/m', '5/h' (omit with --clear)")
    rl.add_argument("--clear", action="store_true", help="remove the rate limit")
    rl.add_argument("--broker", default="sqlite:///ferry.db")
    rl.set_defaults(func=cmd_rate_limit)

    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
