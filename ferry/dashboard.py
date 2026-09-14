"""Live dashboard: FastAPI app serving a real-time view of the broker.

Endpoints:
    GET /                  the dashboard UI
    GET /api/stats         task counts, per-queue depth, workers, paused queues
    GET /api/tasks         recent tasks (?status=, ?queue=, ?limit=)
    GET /api/tasks/{id}    full task detail (decoded args/result/error)
    GET /api/workers       known workers and their heartbeats
    GET /api/throughput    finished tasks per minute (for the chart)
    POST /api/tasks/{id}/retry   requeue a failed/dead task
    POST /api/tasks/retry-dead   requeue all failed/dead tasks (?queue=)
    POST /api/tasks/purge        delete queued tasks (?queue=)
    POST /api/queues/{queue}/pause    pause a queue (workers skip it)
    POST /api/queues/{queue}/resume   resume a paused queue
    WS  /ws                pushes a stats snapshot every second

Run with ``ferry dashboard --broker sqlite:///ferry.db`` (requires the
``dashboard`` extra: ``pip install "ferry[dashboard]"``).
"""

from __future__ import annotations

import asyncio
import json
import os

from .broker import open_broker

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def create_app(broker_url: str = "sqlite:///ferry.db"):
    try:
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        from fastapi.responses import FileResponse, JSONResponse
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            'the dashboard needs the "dashboard" extra: pip install "ferry[dashboard]"'
        ) from exc

    broker = open_broker(broker_url)
    app = FastAPI(title="Ferry dashboard", docs_url=None, redoc_url=None)

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

    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        await websocket.accept()
        try:
            while True:
                await websocket.send_text(json.dumps({"type": "stats", "data": broker.stats()}))
                await asyncio.sleep(1.0)
        except WebSocketDisconnect:
            pass

    return app


def run_dashboard(broker_url: str, host: str = "127.0.0.1", port: int = 8000):
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            'the dashboard needs the "dashboard" extra: pip install "ferry[dashboard]"'
        ) from exc
    uvicorn.run(create_app(broker_url), host=host, port=port, log_level="warning")
