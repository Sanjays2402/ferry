"""Ferry quickstart: enqueue work, run a worker, watch the dashboard.

Run a worker in one terminal::

    python examples/quickstart.py worker

Enqueue jobs from another::

    python examples/quickstart.py enqueue

Watch it live::

    ferry dashboard --broker sqlite:///ferry.db
"""

import sys
import time

from ferry import Ferry

app = Ferry("quickstart", broker="sqlite:///ferry.db")


@app.task(queue="emails", max_retries=3)
def send_email(to: str, subject: str) -> str:
    time.sleep(0.2)  # pretend to talk to an SMTP server
    return f"sent {subject!r} to {to}"


@app.task(queue="default", priority=10)
def urgent_ping() -> str:
    return "pong"


@app.periodic("*/1 * * * *", queue="maintenance")
def cleanup() -> str:
    return "cleaned up"


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "enqueue"
    if mode == "worker":
        from ferry.worker import Worker

        Worker(app, queues=["default", "emails", "maintenance"], concurrency=4).run()
    elif mode == "beat":
        from ferry.scheduler import Beat

        Beat(app).run()
    else:
        for i in range(20):
            send_email.delay(f"user{i}@example.com", subject=f"welcome #{i}")
        urgent_ping.delay()
        print("enqueued 20 emails + 1 urgent ping")


if __name__ == "__main__":
    main()
