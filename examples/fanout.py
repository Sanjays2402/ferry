"""Fan-out / fan-in with Ferry: process a batch of items in parallel, then aggregate.

``process_batch`` fans out one task per item onto the ``images`` queue;
``aggregate`` is scheduled with a countdown so it runs after the batch lands.
In production you'd chain this with a real barrier — this example shows the
pattern with nothing but the core API.
"""

import sys
import time
from datetime import datetime, timezone

from ferry import Ferry

app = Ferry("fanout", broker="sqlite:///ferry.db")


@app.task(queue="images", max_retries=2)
def resize(image: str, width: int) -> dict:
    time.sleep(0.1)
    return {"image": image, "width": width, "done_at": datetime.now(timezone.utc).isoformat()}


@app.task(queue="default")
def aggregate(batch_id: str, expected: int) -> dict:
    done = [
        t for t in app.broker.list_tasks(status="done", queue="images", limit=1000)
        if t["task_name"].endswith("resize")
    ]
    return {"batch_id": batch_id, "resized": len(done), "expected": expected}


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "enqueue"
    if mode == "worker":
        from ferry.worker import Worker

        Worker(app, queues=["images", "default"], concurrency=8).run()
    else:
        images = [f"photo_{i:03d}.jpg" for i in range(50)]
        for img in images:
            resize.delay(img, width=800)
        aggregate.apply_async(args=("batch-1", len(images)), countdown=10)
        print(f"fanned out {len(images)} resizes; aggregate runs in ~10s")


if __name__ == "__main__":
    main()
