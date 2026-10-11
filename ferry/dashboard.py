"""Live dashboard: FastAPI app serving a real-time view of the broker.

Endpoints:
    GET /                  the dashboard UI
    GET /metrics           Prometheus text-format metrics (queue depth, workers, …)
    GET /api/stats         task counts, per-queue depth, workers, paused queues
    GET /api/tasks         recent tasks (?status=, ?queue=, ?limit=)
    GET /api/tasks/{id}    full task detail (decoded args/result/error)
    GET /api/workers       known workers and their heartbeats
    GET /api/throughput    finished tasks per minute (for the chart)
    POST /api/tasks/{id}/retry   requeue a failed/dead task
    POST /api/tasks/{id}/revoke  cancel a task that hasn't started yet
    POST /api/tasks/retry-dead   requeue all failed/dead tasks (?queue=)
    POST /api/tasks/purge        delete queued tasks (?queue=)
    POST /api/queues/{queue}/pause    pause a queue (workers skip it)
    POST /api/queues/{queue}/resume   resume a paused queue
    POST /api/queues/{queue}/rate-limit  set/clear a queue's claim rate
    WS  /ws                pushes a stats snapshot every second

Run with ``ferry dashboard --broker sqlite:///ferry.db`` (requires the
``dashboard`` extra: ``pip install "ferry[dashboard]"``).

Authentication: pass ``token=...`` (CLI: ``--token`` or the
``FERRY_DASHBOARD_TOKEN`` environment variable) to require it on every
request, including the UI, ``/metrics`` and the WebSocket. Clients send it as
``Authorization: Bearer <token>`` (scripts, Prometheus) or as the password of
HTTP Basic auth with any username (browsers prompt for it natively). Without
a token the dashboard is open — keep it on 127.0.0.1 or behind a proxy.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import json
import os

from .broker import open_broker
from .metrics import CONTENT_TYPE as METRICS_CONTENT_TYPE
from .metrics import render_metrics

# Imported at module level (not inside create_app): with postponed annotations
# FastAPI resolves handler annotations like ``websocket: WebSocket`` from the
# module globals, and a function-local import left the /ws parameter looking
# like a required query field, so every websocket handshake was rejected.
try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.responses import FileResponse, JSONResponse, Response
except ImportError:  # pragma: no cover - the dashboard extra is optional
    FastAPI = None

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _token_matches(expected: str, supplied: str) -> bool:
    return hmac.compare_digest(expected.encode("utf-8"), supplied.encode("utf-8"))


def _authorized(token: str, header: str | None) -> bool:
    """Check an ``Authorization`` header against the dashboard token."""
    if not header:
        return False
    scheme, _, value = header.partition(" ")
    value = value.strip()
    scheme = scheme.lower()
    if scheme == "bearer":
        return _token_matches(token, value)
    if scheme == "basic":
        try:
            decoded = base64.b64decode(value, validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            return False
        _user, _, password = decoded.partition(":")
        return _token_matches(token, password)
    return False


_WS_COOKIE = "ferry_ws"


def _ws_cookie_value(token: str) -> str:
    """Derived value for the websocket cookie (never the token itself)."""
    return hmac.new(token.encode("utf-8"), b"ferry-dashboard-ws", "sha256").hexdigest()


def create_app(broker_url: str = "sqlite:///ferry.db", token: str | None = None):
    if FastAPI is None:  # pragma: no cover
        raise RuntimeError(
            'the dashboard needs the "dashboard" extra: pip install "ferry[dashboard]"'
        )
    if token is not None and not token:
        raise ValueError("dashboard token must not be empty")
    broker = open_broker(broker_url)
    app = FastAPI(title="Ferry dashboard", docs_url=None, redoc_url=None)

    if token is not None:

        @app.middleware("http")
        async def require_token(request, call_next):
            if _authorized(token, request.headers.get("authorization")):
                response = await call_next(request)
                if request.url.path == "/":
                    # Browsers don't reliably send cached Basic credentials on a
                    # WebSocket handshake. Hand the page a cookie that is valid
                    # for /ws only (SameSite=Strict, HttpOnly) so the live feed
                    # works; it is never accepted by any HTTP route.
                    response.set_cookie(
                        _WS_COOKIE, _ws_cookie_value(token),
                        httponly=True, samesite="strict", path="/ws",
                    )
                return response
            return JSONResponse(
                {"detail": "authentication required"},
                status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="Ferry dashboard", charset="UTF-8"'},
            )

    @app.get("/metrics")
    def metrics():
        return Response(render_metrics(broker.stats()), media_type=METRICS_CONTENT_TYPE)

    @app.get("/")
    def index():
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    @app.get("/app.js")
    def app_js():
        return FileResponse(os.path.join(STATIC_DIR, "app.js"))

    @app.get("/style.css")
    def style_css():
        return FileResponse(os.path.join(STATIC_DIR, "style.css"))

    @app.get("/api/stats")
    def stats():
        return JSONResponse(broker.stats())

    @app.get("/api/tasks")
    def tasks(status: str | None = None, queue: str | None = None, limit: int = 100):
        limit = max(1, min(limit, 500))
        rows = broker.list_tasks(status=status, queue=queue, limit=limit)
        for r in rows:
            r.pop("args", None)
            r.pop("kwargs", None)
            r["progress"] = broker.decode_progress(r)
            if r.get("result") and len(r["result"]) > 500:
                r["result"] = r["result"][:500] + "…"
            if r.get("error") and len(r["error"]) > 500:
                r["error"] = r["error"][:500] + "…"
        return JSONResponse(rows)

    @app.get("/api/tasks/{task_id}")
    def task_detail(task_id: str):
        t = broker.get_task(task_id)
        if t is None:
            return JSONResponse({"detail": "unknown task"}, status_code=404)
        args, kwargs = broker.decode_args(t)
        t["args_decoded"] = args
        t["kwargs_decoded"] = kwargs
        t["result_decoded"] = broker.decode_result(t)
        t["progress"] = broker.decode_progress(t)
        return JSONResponse(t)

    @app.get("/api/workers")
    def workers():
        return JSONResponse(broker.list_workers())

    @app.get("/api/throughput")
    def throughput(minutes: int = 60):
        return JSONResponse(broker.throughput(minutes=max(1, min(minutes, 1440))))

    @app.post("/api/tasks/{task_id}/retry")
    def retry(task_id: str):
        ok = broker.retry_task(task_id)
        return JSONResponse({"retried": ok})

    @app.post("/api/tasks/{task_id}/revoke")
    def revoke(task_id: str):
        ok = broker.revoke(task_id)
        return JSONResponse({"revoked": ok})

    @app.post("/api/tasks/retry-dead")
    def retry_dead(queue: str | None = None):
        n = broker.retry_dead(queue=queue)
        return JSONResponse({"retried": n})

    @app.post("/api/tasks/purge")
    def purge(queue: str | None = None):
        n = broker.purge(queue=queue)
        return JSONResponse({"purged": n})

    @app.post("/api/queues/{queue}/pause")
    def pause_queue(queue: str):
        broker.pause_queue(queue)
        return JSONResponse({"paused": True, "queue": queue})

    @app.post("/api/queues/{queue}/resume")
    def resume_queue(queue: str):
        broker.resume_queue(queue)
        return JSONResponse({"paused": False, "queue": queue})

    @app.post("/api/queues/{queue}/rate-limit")
    def rate_limit(queue: str, payload: dict):
        rate = payload.get("rate")
        if rate:
            broker.set_rate_limit(queue, rate)
            return JSONResponse({"rate": broker.get_rate_limit(queue), "queue": queue})
        broker.clear_rate_limit(queue)
        return JSONResponse({"rate": None, "queue": queue})

    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        # HTTP middleware doesn't see websocket handshakes, so check here
        if token is not None and not (
            _authorized(token, websocket.headers.get("authorization"))
            or _token_matches(
                _ws_cookie_value(token), websocket.cookies.get(_WS_COOKIE, "")
            )
        ):
            await websocket.close(code=1008)  # policy violation
            return
        await websocket.accept()
        try:
            while True:
                await websocket.send_text(json.dumps({"type": "stats", "data": broker.stats()}))
                await asyncio.sleep(1.0)
        except WebSocketDisconnect:
            pass

    return app


def run_dashboard(
    broker_url: str, host: str = "127.0.0.1", port: int = 8000, token: str | None = None
):
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            'the dashboard needs the "dashboard" extra: pip install "ferry[dashboard]"'
        ) from exc
    uvicorn.run(create_app(broker_url, token=token), host=host, port=port, log_level="warning")
