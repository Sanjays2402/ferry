import base64

import pytest

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from ferry import Ferry  # noqa: E402
from ferry.cli import build_parser  # noqa: E402
from ferry.dashboard import _authorized, create_app  # noqa: E402
from ferry.metrics import render_metrics  # noqa: E402

TOKEN = "s3cret-token"


def basic(user: str, password: str) -> dict:
    raw = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {raw}"}


def bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture()
def db(tmp_path):
    return f"sqlite:///{tmp_path}/dash.db"


@pytest.fixture()
def secured(db):
    return TestClient(create_app(db, token=TOKEN))


# -- metrics ---------------------------------------------------------------------


def test_metrics_exposition(db):
    ferry = Ferry("x", broker=db)

    @ferry.task(queue="emails")
    def send():
        return 1

    send.delay()
    send.delay()
    ferry.broker.pause_queue("emails")
    ferry.broker.heartbeat("w1", ["emails"], 4, "host")
    client = TestClient(create_app(db))
    r = client.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain; version=0.0.4")
    body = r.text
    assert "# TYPE ferry_tasks gauge" in body
    assert 'ferry_tasks{status="queued"} 2' in body
    assert 'ferry_tasks{status="expired"} 0' in body
    assert 'ferry_queue_tasks{queue="emails",state="queued"} 2' in body
    assert 'ferry_queue_paused{queue="emails"} 1' in body
    assert "ferry_workers 1" in body
    assert "ferry_worker_concurrency 4" in body
    assert body.endswith("\n")


def test_metrics_label_escaping():
    stats = {
        "tasks": {"queued": 0},
        "queues": [{"queue": 'we"ird\\q\nname', "queued": 1, "active": None, "dead": 0}],
        "workers": [],
        "paused": [],
        "rate_limits": {},
    }
    body = render_metrics(stats)
    assert 'queue="we\\"ird\\\\q\\nname"' in body
    assert 'state="active"} 0' in body  # None counts render as 0


def test_metrics_empty_broker(db):
    body = TestClient(create_app(db)).get("/metrics").text
    assert "ferry_workers 0" in body
    assert "ferry_rate_limited_queues 0" in body


# -- auth ------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["/", "/app.js", "/style.css", "/api/stats", "/api/tasks",
                                  "/api/workers", "/metrics"])
def test_everything_requires_token(secured, path):
    r = secured.get(path)
    assert r.status_code == 401
    assert r.headers["www-authenticate"].startswith("Basic")


def test_mutating_endpoints_require_token(secured):
    assert secured.post("/api/tasks/purge").status_code == 401
    assert secured.post("/api/queues/q/pause").status_code == 401
    assert secured.post("/api/tasks/retry-dead").status_code == 401


def test_bearer_token_grants_access(secured):
    assert secured.get("/api/stats", headers=bearer(TOKEN)).status_code == 200
    assert secured.get("/metrics", headers=bearer(TOKEN)).status_code == 200
    assert secured.post("/api/tasks/purge", headers=bearer(TOKEN)).json() == {"purged": 0}


def test_basic_auth_password_is_the_token(secured):
    assert secured.get("/", headers=basic("anyone", TOKEN)).status_code == 200
    assert secured.get("/", headers=basic("", TOKEN)).status_code == 200
    assert secured.get("/", headers=basic("admin", "wrong")).status_code == 401


def test_wrong_and_malformed_credentials_rejected(secured):
    assert secured.get("/api/stats", headers=bearer("nope")).status_code == 401
    assert secured.get("/api/stats", headers=bearer("")).status_code == 401
    for header in ("Basic !!!notbase64", "Digest abc", TOKEN):
        assert secured.get("/api/stats", headers={"Authorization": header}).status_code == 401
    # token must not be accepted from the query string (it would leak into logs)
    assert secured.get(f"/api/stats?token={TOKEN}").status_code == 401


def test_non_ascii_credentials_do_not_crash(secured):
    raw = {"Authorization": "Bearer tökén".encode("latin-1")}  # httpx only sends bytes here
    assert secured.get("/api/stats", headers=raw).status_code == 401
    assert secured.get("/api/stats", headers=basic("ü", "pässwörd")).status_code == 401


def test_open_when_no_token(db):
    client = TestClient(create_app(db))
    assert client.get("/api/stats").status_code == 200
    assert client.get("/api/stats", headers=bearer("whatever")).status_code == 200


def test_empty_token_rejected(db):
    with pytest.raises(ValueError):
        create_app(db, token="")


def test_websocket_requires_token(db):
    from starlette.websockets import WebSocketDisconnect

    client = TestClient(create_app(db, token=TOKEN))
    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/ws"):
            pass
    assert exc.value.code == 1008
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws", headers=bearer("wrong")):
            pass
    with client.websocket_connect("/ws", headers=bearer(TOKEN)) as ws:
        assert ws.receive_json()["type"] == "stats"
    with client.websocket_connect("/ws", headers=basic("u", TOKEN)) as ws:
        assert ws.receive_json()["type"] == "stats"


def test_websocket_cookie_set_on_ui_load_and_scoped_to_ws(db):
    client = TestClient(create_app(db, token=TOKEN))
    r = client.get("/", headers=basic("u", TOKEN))
    cookie = r.headers["set-cookie"]
    assert cookie.startswith("ferry_ws=")
    assert "HttpOnly" in cookie and "Path=/ws" in cookie and "SameSite=strict" in cookie
    assert TOKEN not in cookie  # derived value, never the token itself
    assert "set-cookie" not in client.get("/api/stats", headers=bearer(TOKEN)).headers
    assert "set-cookie" not in TestClient(create_app(db)).get("/").headers  # no auth, no cookie


def test_websocket_accepts_cookie_but_http_routes_do_not(db):
    from starlette.websockets import WebSocketDisconnect

    client = TestClient(create_app(db, token=TOKEN))
    value = client.get("/", headers=basic("u", TOKEN)).cookies["ferry_ws"]
    fresh = TestClient(create_app(db, token=TOKEN))
    with fresh.websocket_connect("/ws", headers={"Cookie": f"ferry_ws={value}"}) as ws:
        assert ws.receive_json()["type"] == "stats"
    with pytest.raises(WebSocketDisconnect):
        with fresh.websocket_connect("/ws", headers={"Cookie": "ferry_ws=forged"}):
            pass
    # the cookie must never authenticate HTTP routes (no CSRF surface)
    cookie = {"Cookie": f"ferry_ws={value}"}
    assert fresh.get("/api/stats", headers=cookie).status_code == 401
    assert fresh.post("/api/tasks/purge", headers=cookie).status_code == 401


def test_websocket_open_without_token(db):
    with TestClient(create_app(db)).websocket_connect("/ws") as ws:
        assert ws.receive_json()["type"] == "stats"


def test_authorized_helper():
    assert _authorized("t", "Bearer t")
    assert _authorized("t", "bearer t")  # scheme is case-insensitive
    assert not _authorized("t", None)
    assert not _authorized("t", "")
    assert not _authorized("t", "Bearer")


# -- cli -------------------------------------------------------------------------


def test_cli_token_option():
    parser = build_parser()
    assert parser.parse_args(["dashboard"]).token is None
    assert parser.parse_args(["dashboard", "--token", "abc"]).token == "abc"


def test_cli_uses_env_token(monkeypatch):
    from ferry import cli

    captured = {}
    monkeypatch.setattr(
        "ferry.dashboard.run_dashboard",
        lambda broker, host, port, token: captured.update(token=token, host=host),
    )
    monkeypatch.setenv("FERRY_DASHBOARD_TOKEN", "from-env")
    cli.main(["dashboard"])
    assert captured["token"] == "from-env"
    cli.main(["dashboard", "--token", "explicit"])
    assert captured["token"] == "explicit"  # flag beats env


def test_cli_rejects_empty_token(monkeypatch):
    from ferry import cli

    monkeypatch.delenv("FERRY_DASHBOARD_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        cli.main(["dashboard", "--token", ""])


def test_cli_warns_on_open_public_bind(monkeypatch, capsys):
    from ferry import cli

    monkeypatch.delenv("FERRY_DASHBOARD_TOKEN", raising=False)
    monkeypatch.setattr("ferry.dashboard.run_dashboard", lambda *a, **k: None)
    cli.main(["dashboard", "--host", "0.0.0.0"])
    assert "no authentication" in capsys.readouterr().err
    cli.main(["dashboard", "--host", "0.0.0.0", "--token", "x"])
    assert "no authentication" not in capsys.readouterr().err


# -- progress / expiry through the dashboard API -------------------------------------


def test_dashboard_exposes_progress_and_expires_at(db):
    from ferry.worker import Worker

    ferry = Ferry("x", broker=db)

    @ferry.task(expires=600, max_retries=0)
    def job():
        from ferry import update_progress

        update_progress(1, total=4, message="warming up")
        raise RuntimeError("stop here")  # keep it from finishing cleanly

    ferry_task = job.delay()
    Worker(ferry, concurrency=1, poll_interval=0.01).run_once(timeout=3)
    client = TestClient(create_app(db))
    detail = client.get(f"/api/tasks/{ferry_task.task_id}").json()
    assert detail["progress"]["percent"] == 25.0
    assert detail["progress"]["message"] == "warming up"
    assert detail["expires_at"]
    row = next(t for t in client.get("/api/tasks").json() if t["id"] == ferry_task.task_id)
    assert row["progress"]["current"] == 1
