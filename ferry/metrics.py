"""Prometheus metrics: render a broker snapshot in the text exposition format.

No client library needed. The dashboard serves this at ``GET /metrics``; you
can also call :func:`render_metrics` yourself (e.g. from your own web app)::

    from ferry.metrics import render_metrics
    body = render_metrics(app.broker.stats())
"""

from __future__ import annotations

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _escape(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(**labels: object) -> str:
    if not labels:
        return ""
    return "{" + ",".join(f'{k}="{_escape(v)}"' for k, v in labels.items()) + "}"


def render_metrics(stats: dict) -> str:
    """Render ``broker.stats()`` as Prometheus text exposition format."""
    out: list[str] = []

    def family(name: str, help_text: str, samples: list[tuple[str, float]]) -> None:
        out.append(f"# HELP {name} {help_text}")
        out.append(f"# TYPE {name} gauge")
        out.extend(f"{name}{labels} {value}" for labels, value in samples)

    family(
        "ferry_tasks",
        "Number of tasks by status.",
        [(_labels(status=s), n) for s, n in sorted(stats["tasks"].items())],
    )
    queues = stats["queues"]
    family(
        "ferry_queue_tasks",
        "Number of tasks per queue by state (queued, active, dead).",
        [
            (_labels(queue=q["queue"], state=state), q[state] or 0)
            for q in queues
            for state in ("queued", "active", "dead")
        ],
    )
    paused = set(stats.get("paused", []))
    known = sorted(paused | {q["queue"] for q in queues})
    family(
        "ferry_queue_paused",
        "1 if the queue is paused (workers skip it), else 0.",
        [(_labels(queue=name), int(name in paused)) for name in known],
    )
    family(
        "ferry_rate_limited_queues",
        "Number of queues with a rate limit configured.",
        [("", len(stats.get("rate_limits", {})))],
    )
    workers = stats["workers"]
    family("ferry_workers", "Number of registered workers.", [("", len(workers))])
    family(
        "ferry_worker_concurrency",
        "Total concurrency across registered workers.",
        [("", sum(int(w["concurrency"]) for w in workers))],
    )
    return "\n".join(out) + "\n"
