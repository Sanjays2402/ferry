"""Broker: durable task storage and atomic claiming.

The default broker is SQLite — zero infrastructure, WAL mode for concurrent
readers/writers, and atomic ``UPDATE ... RETURNING`` claims so two workers can
never pick up the same task. A Redis broker can be added behind the same
interface (see ``ferry/redis_broker.py`` when the ``redis`` extra is installed).
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import datetime, timezone

from .serialization import dumps, loads

_STATUSES = ("queued", "claimed", "running", "done", "failed", "dead")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ferry_tasks (
    id           TEXT PRIMARY KEY,
    queue        TEXT NOT NULL,
    task_name    TEXT NOT NULL,
    args         TEXT NOT NULL,
    kwargs       TEXT NOT NULL,
    priority     INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'queued',
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_retries  INTEGER NOT NULL DEFAULT 3,
    eta          TEXT,                       -- not visible before this (ISO-8601 UTC)
    scheduled_id TEXT,                       -- dedup key for periodic tasks
    result       TEXT,
    error        TEXT,
    worker_id    TEXT,
    created_at   TEXT NOT NULL,
    claimed_at   TEXT,
    finished_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_ferry_claim
    ON ferry_tasks (status, queue, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_ferry_lookup ON ferry_tasks (status, created_at);
CREATE TABLE IF NOT EXISTS ferry_workers (
    worker_id   TEXT PRIMARY KEY,
    queues      TEXT NOT NULL,
    concurrency INTEGER NOT NULL,
    last_beat   TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    hostname    TEXT
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class SQLiteBroker:
    """Durable broker backed by a SQLite database file (or ``:memory:``)."""

    def __init__(self, path: str = "ferry.db", timeout: float = 30.0):
        self.path = path
        self._local = threading.local()
        self._timeout = timeout
        self._init_db()

    # -- connection handling -------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=self._timeout, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        conn.executescript(_SCHEMA)
        conn.commit()

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # -- producing ------------------------------------------------------------
    def enqueue(
        self,
        task_name: str,
        args: tuple = (),
        kwargs: dict | None = None,
        *,
        queue: str = "default",
        priority: int = 0,
        max_retries: int = 3,
        eta: datetime | None = None,
        scheduled_id: str | None = None,
    ) -> str:
        if scheduled_id is not None:
            # periodic tasks: at most one pending instance per schedule slot
            row = self._connect().execute(
                "SELECT id FROM ferry_tasks WHERE scheduled_id = ? "
                "AND status IN ('queued','claimed','running')",
                (scheduled_id,),
            ).fetchone()
            if row:
                return row["id"]
        task_id = uuid.uuid4().hex
        self._connect().execute(
            """INSERT INTO ferry_tasks
               (id, queue, task_name, args, kwargs, priority,
                max_retries, eta, scheduled_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                task_id,
                queue,
                task_name,
                dumps(list(args)),
                dumps(kwargs or {}),
                priority,
                max_retries,
                eta.astimezone(timezone.utc).isoformat() if eta else None,
                scheduled_id,
                _utcnow(),
            ),
        )
        self._connect().commit()
        return task_id

    # -- consuming ------------------------------------------------------------
    def claim(self, queues: list[str], worker_id: str) -> dict | None:
        """Atomically claim the highest-priority visible task. Returns None if empty."""
        now = _utcnow()
        placeholders = ",".join("?" for _ in queues)
        row = self._connect().execute(
            f"""UPDATE ferry_tasks SET status='claimed', worker_id=?, claimed_at=?
                WHERE id = (
                    SELECT id FROM ferry_tasks
                    WHERE status='queued' AND queue IN ({placeholders})
                      AND (eta IS NULL OR eta <= ?)
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                )
                RETURNING *""",
            (worker_id, now, *queues, now),
        ).fetchone()
        self._connect().commit()
        return dict(row) if row else None

    def mark_running(self, task_id: str) -> None:
        self._connect().execute(
            "UPDATE ferry_tasks SET status='running' WHERE id=?", (task_id,)
        )
        self._connect().commit()

    def ack_done(self, task_id: str, result) -> None:
        self._connect().execute(
            "UPDATE ferry_tasks SET status='done', result=?, finished_at=? WHERE id=?",
            (dumps(result), _utcnow(), task_id),
        )
        self._connect().commit()

    def ack_failed(self, task_id: str, error: str, retry_at: datetime | None) -> bool:
        """Record a failure. Returns True if the task was requeued for retry."""
        conn = self._connect()
        if retry_at is None:
            conn.execute(
                "UPDATE ferry_tasks SET status='dead', error=?, finished_at=? WHERE id=?",
                (error, _utcnow(), task_id),
            )
            conn.commit()
            return False
        conn.execute(
            """UPDATE ferry_tasks
               SET status='queued', attempts=attempts+1, error=?,
                   eta=?, worker_id=NULL, claimed_at=NULL
               WHERE id=?""",
            (error, retry_at.astimezone(timezone.utc).isoformat(), task_id),
        )
        conn.commit()
        return True

    def requeue_stale_claims(self, older_than_seconds: float = 120.0) -> int:
        """Return tasks claimed by workers that stopped heartbeating to the queue."""
        from datetime import timedelta

        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat()
        cur = self._connect().execute(
            """UPDATE ferry_tasks SET status='queued', worker_id=NULL, claimed_at=NULL
               WHERE status IN ('claimed','running')
                 AND (claimed_at IS NULL OR claimed_at < ?)
                 AND (worker_id IS NULL OR worker_id NOT IN
                      (SELECT worker_id FROM ferry_workers WHERE last_beat >= ?))
               RETURNING id""",
            (cutoff, cutoff),
        )
        n = len(cur.fetchall())
        self._connect().commit()
        return n

    # -- workers ---------------------------------------------------------------
    def heartbeat(self, worker_id: str, queues: list[str], concurrency: int, hostname: str) -> None:
        self._connect().execute(
            """INSERT INTO ferry_workers
               (worker_id, queues, concurrency, last_beat, started_at, hostname)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(worker_id) DO UPDATE SET
                 queues=excluded.queues, concurrency=excluded.concurrency,
                 last_beat=excluded.last_beat, hostname=excluded.hostname""",
            (worker_id, ",".join(queues), concurrency, _utcnow(), _utcnow(), hostname),
        )
        self._connect().commit()

    def remove_worker(self, worker_id: str) -> None:
        self._connect().execute("DELETE FROM ferry_workers WHERE worker_id=?", (worker_id,))
        self._connect().commit()

    def list_workers(self) -> list[dict]:
        return [
            dict(r)
            for r in self._connect().execute(
                "SELECT * FROM ferry_workers ORDER BY started_at"
            ).fetchall()
        ]

    # -- inspection --------------------------------------------------------------
    def get_task(self, task_id: str) -> dict | None:
        row = self._connect().execute(
            "SELECT * FROM ferry_tasks WHERE id=?", (task_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_task_by_scheduled_id(self, scheduled_id: str) -> dict | None:
        """Find any task (any status) enqueued for a periodic schedule slot."""
        row = self._connect().execute(
            "SELECT * FROM ferry_tasks WHERE scheduled_id=? ORDER BY created_at DESC LIMIT 1",
            (scheduled_id,),
        ).fetchone()
        return dict(row) if row else None

    def list_tasks(
        self, status: str | None = None, queue: str | None = None, limit: int = 100
    ) -> list[dict]:
        q = "SELECT * FROM ferry_tasks"
        clauses, params = [], []
        if status:
            clauses.append("status=?")
            params.append(status)
        if queue:
            clauses.append("queue=?")
            params.append(queue)
        if clauses:
            q += " WHERE " + " AND ".join(clauses)
        q += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in self._connect().execute(q, params).fetchall()]

    def stats(self) -> dict:
        rows = self._connect().execute(
            "SELECT status, COUNT(*) AS n FROM ferry_tasks GROUP BY status"
        ).fetchall()
        counts = {s: 0 for s in _STATUSES}
        counts.update({r["status"]: r["n"] for r in rows})
        queues = [
            dict(r)
            for r in self._connect().execute(
                """SELECT queue,
                          SUM(status='queued') AS queued,
                          SUM(status IN ('claimed','running')) AS active,
                          SUM(status='dead') AS dead
                   FROM ferry_tasks GROUP BY queue"""
            ).fetchall()
        ]
        workers = self.list_workers()
        return {"tasks": counts, "queues": queues, "workers": workers}

    def throughput(self, minutes: int = 60) -> list[dict]:
        """Finished tasks per minute for the last ``minutes`` minutes (for charts)."""
        from datetime import timedelta

        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        rows = self._connect().execute(
            """SELECT substr(finished_at, 1, 16) AS minute, status, COUNT(*) AS n
               FROM ferry_tasks
               WHERE finished_at >= ? AND status IN ('done','failed','dead')
               GROUP BY minute, status ORDER BY minute""",
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]

    def retry_task(self, task_id: str) -> bool:
        cur = self._connect().execute(
            """UPDATE ferry_tasks
               SET status='queued', attempts=0, error=NULL, eta=NULL,
                   worker_id=NULL, claimed_at=NULL, finished_at=NULL
               WHERE id=? AND status IN ('failed','dead')""",
            (task_id,),
        )
        self._connect().commit()
        return cur.rowcount > 0

    def purge(self, queue: str | None = None, status: str = "queued") -> int:
        q = "DELETE FROM ferry_tasks WHERE status=?"
        params: list = [status]
        if queue:
            q += " AND queue=?"
            params.append(queue)
        cur = self._connect().execute(q, params)
        self._connect().commit()
        return cur.rowcount

    # -- payload helpers ---------------------------------------------------------
    @staticmethod
    def decode_args(task: dict) -> tuple[list, dict]:
        return _decode_args(task)

    @staticmethod
    def decode_result(task: dict):
        return _decode_result(task)


def _decode_args(task: dict) -> tuple[list, dict]:
    return loads(task["args"]), loads(task["kwargs"])


def _decode_result(task: dict):
    return loads(task["result"]) if task["result"] else None


def open_broker(url: str) -> SQLiteBroker:
    """Open a broker from a URL: ``sqlite:///path/to.db`` or ``sqlite:///:memory:``."""
    if url == "sqlite:///:memory:":
        return SQLiteBroker(":memory:")
    if url.startswith("sqlite:///"):
        return SQLiteBroker(url[len("sqlite:///"):])
    if url.startswith("redis://"):
        from .redis_broker import RedisBroker

        return RedisBroker(url)
    raise ValueError(f"unsupported broker URL: {url!r}")


__all__ = ["SQLiteBroker", "open_broker"]
