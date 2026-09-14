import time

import pytest

from ferry import Ferry
from ferry.worker import Worker


@pytest.fixture()
def app(tmp_path):
    return Ferry("test", broker=f"sqlite:///{tmp_path}/test.db")


@pytest.fixture()
def worker(app):
    return Worker(app, concurrency=2, poll_interval=0.01)


def run_until(worker, result, timeout=15.0):
    """Drive a worker until the result is ready (retries included)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result.ready():
            return
        worker.run_once(timeout=0.5)
        time.sleep(0.02)
    raise TimeoutError(f"task {result.task_id} not ready after {timeout}s")
