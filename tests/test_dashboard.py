import pytest

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from ferry.dashboard import create_app  # noqa: E402


@pytest.fixture()
def client(tmp_path):
    app = create_app(f"sqlite:///{tmp_path}/dash.db")
    return TestClient(app)


def test_stats_empty(client):
    r = client.get("/api/stats")
    assert r.status_code == 200
    assert r.json()["tasks"]["queued"] == 0


def test_tasks_and_retry_flow(client, tmp_path):
    from ferry import Ferry

    ferry = Ferry("x", broker=f"sqlite:///{tmp_path}/dash.db")

    @ferry.task
    def job():
        return 1

    result = job.delay()
    tasks = client.get("/api/tasks").json()
    assert len(tasks) == 1 and tasks[0]["task_name"] == job.name

    # simulate a failure straight to dead, then retry via the API
    ferry.broker.ack_failed(result.task_id, "boom", None)
    r = client.post(f"/api/tasks/{result.task_id}/retry")
    assert r.json() == {"retried": True}
    assert client.get("/api/tasks?status=queued").json()[0]["id"] == result.task_id


def test_throughput_and_workers(client):
    assert client.get("/api/throughput").json() == []
    assert client.get("/api/workers").json() == []


def test_purge(client, tmp_path):
    from ferry import Ferry

    ferry = Ferry("x", broker=f"sqlite:///{tmp_path}/dash.db")

    @ferry.task
    def job():
        return 1

    job.delay()
    job.delay()
    r = client.post("/api/tasks/purge")
    assert r.json() == {"purged": 2}
