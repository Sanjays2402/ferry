"""Broker: durable task storage and atomic claiming.

The default broker is SQLite — zero infrastructure, WAL mode for concurrent
readers/writers, and atomic ``UPDATE ... RETURNING`` claims so two workers can
never pick up the same task. A Redis broker can be added behind the same
interface (see ``ferry/redis_broker.py`` when the ``redis`` extra is installed).
"""

from __future__ import annotations

import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone

from .serialization import dumps, loads

_STATUSES = ("queued", "claimed", "running", "done", "failed", "dead", "revoked")

_RATE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*/\s*([a-z]+)\s*$", re.IGNORECASE)
_RATE_UNITS = {
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
}


def parse_rate(rate: str | float) -> tuple[float, str]:
    """Parse a rate like ``"10/m"`` or ``100`` (per second) into
    ``(tasks_per_second, canonical_string)``."""
    if isinstance(rate, (int, float)):
        if rate <= 0:
            raise ValueError("rate must be positive")
        return float(rate), f"{rate:g}/s"
    m = _RATE_RE.match(str(rate))
    if not m:
        raise ValueError(f"bad rate {rate!r}: expected like '100/s', '10/m', '5/h'")
    n, unit = float(m.group(1)), m.group(2).lower()
    if unit not in _RATE_UNITS:
        raise ValueError(f"bad rate {rate!r}: unknown unit {m.group(2)!r}")
    if n <= 0:
        raise ValueError("rate must be positive")
    return n / _RATE_UNITS[unit], f"{n:g}/{unit}"

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
CREATE TABLE IF NOT EXISTS ferry_chords (
    chord_id  TEXT PRIMARY KEY,
    remaining INTEGER NOT NULL,   -- header tasks still outstanding
    body      TEXT NOT NULL,      -- serialized callback signature (JSON)
    task_ids  TEXT NOT NULL       -- header task ids in order (JSON list)
);
CREATE TABLE IF NOT EXISTS ferry_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
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
        # migrations for databases created by older Ferry versions
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(ferry_tasks)")}
        for coldef in (
            "chain TEXT",
            "chord_id TEXT",
            "chord_index INTEGER",
            "dedupe_key TEXT",
            "result_expired INTEGER NOT NULL DEFAULT 0",
            "time_limit REAL",
            "soft_time_limit REAL",
        ):
            if coldef.split()[0] not in cols:
                conn.execute(f"ALTER TABLE ferry_tasks ADD COLUMN {coldef}")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ferry_dedupe ON ferry_tasks (dedupe_key, status)"
        )
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
        task_id: str | None = None,
        chain: str | list | None = None,
        chord_id: str | None = None,
        chord_index: int | None = None,
        dedupe_key: str | None = None,
        time_limit: float | None = None,
        soft_time_limit: float | None = None,
    ) -> str:
        """Enqueue a task. ``task_id`` lets callers pre-assign ids (used by
        canvases); ``chain`` is a JSON list of serialized signatures to run
        after this task succeeds; ``chord_id``/``chord_index`` attach the task
        to a chord barrier. ``dedupe_key`` collapses duplicates: if a task with
        the same key is still pending (queued/claimed/running), its id is
        returned instead of enqueuing a new task. ``time_limit`` /
        ``soft_time_limit`` bound execution time (seconds; enforced by workers)."""
        conn = self._connect()
        if scheduled_id is not None:
            # periodic tasks: at most one pending instance per schedule slot
            row = conn.execute(
                "SELECT id FROM ferry_tasks WHERE scheduled_id = ? "
                "AND status IN ('queued','claimed','running')",
                (scheduled_id,),
            ).fetchone()
            if row:
                return row["id"]
        if dedupe_key is not None:
            row = conn.execute(
                "SELECT id FROM ferry_tasks WHERE dedupe_key = ? "
                "AND status IN ('queued','claimed','running')",
                (dedupe_key,),
            ).fetchone()
            if row:
                return row["id"]
        if task_id is None:
            task_id = uuid.uuid4().hex
        if chain is not None and not isinstance(chain, str):
            chain = dumps(chain)
        conn.execute(
            """INSERT INTO ferry_tasks
               (id, queue, task_name, args, kwargs, priority,
                max_retries, eta, scheduled_id, created_at,
                chain, chord_id, chord_index, dedupe_key,
                time_limit, soft_time_limit)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                chain,
                chord_id,
                chord_index,
                dedupe_key,
                time_limit,
                soft_time_limit,
            ),
        )
        conn.commit()
        return task_id

    # -- consuming ------------------------------------------------------------
    def claim(self, queues: list[str], worker_id: str) -> dict | None:
        """Atomically claim the highest-priority visible task. Returns None if empty.

        Queues with a rate limit (see :meth:`set_rate_limit`) are skipped when
        their token bucket is empty; a token is consumed for the claimed task's
        queue. The check-and-consume runs inside the claim transaction, so the
        limit holds across any number of workers."""
        if not queues:
            return None
        conn = self._connect()
        now = _utcnow()
        now_ts = time.time()
        # Rate limiting: consume one token per rate-limited queue up front and
        # drop queues whose bucket is empty. Tokens for queues that don't
        # produce the claimed task are refunded below, so the limit is exact
        # even with many workers (writers serialize in SQLite).
        allowed = [q for q in queues if self._rl_consume(q, now_ts)]
        row = None
        if allowed:
            placeholders = ",".join("?" for _ in allowed)
            row = conn.execute(
                f"""UPDATE ferry_tasks SET status='claimed', worker_id=?, claimed_at=?
                    WHERE id = (
                        SELECT id FROM ferry_tasks
                        WHERE status='queued' AND queue IN ({placeholders})
                          AND (eta IS NULL OR eta <= ?)
                        ORDER BY priority DESC, created_at ASC
                        LIMIT 1
                    )
                    RETURNING *""",
                (worker_id, now, *allowed, now),
            ).fetchone()
        winner = row["queue"] if row is not None else None
        for q in allowed:
            if q != winner:
                self._rl_refund(q)
        conn.commit()
        return dict(row) if row else None

    def mark_running(self, task_id: str) -> bool:
        """Move a claimed task to running. Returns False if the task is no
        longer claimed (e.g. it was revoked between claim and start) — the
        worker must then skip execution."""
        cur = self._connect().execute(
            "UPDATE ferry_tasks SET status='running' WHERE id=? AND status='claimed'",
            (task_id,),
        )
        self._connect().commit()
        return cur.rowcount > 0

    def revoke(self, task_id: str) -> bool:
        """Cancel a task that hasn't started yet. The task moves to the
        terminal ``revoked`` state; returns False if it already started,
        finished, or doesn't exist."""
        cur = self._connect().execute(
            """UPDATE ferry_tasks SET status='revoked', finished_at=?,
                   worker_id=NULL, claimed_at=NULL
               WHERE id=? AND status IN ('queued','claimed')""",
            (_utcnow(), task_id),
        )
        self._connect().commit()
        return cur.rowcount > 0

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

    # -- chords ------------------------------------------------------------------
    def chord_init(self, chord_id: str, body_json: str, task_ids_json: str) -> None:
        """Register a chord barrier: ``task_ids_json`` is the header task ids in
        order, ``body_json`` the serialized callback signature."""
        import json as _json

        self._connect().execute(
            "INSERT OR IGNORE INTO ferry_chords (chord_id, remaining, body, task_ids)"
            " VALUES (?, ?, ?, ?)",
            (chord_id, len(_json.loads(task_ids_json)), body_json, task_ids_json),
        )
        self._connect().commit()

    def chord_task_done(self, chord_id: str) -> dict | None:
        """Atomically count one header task as finished. When the last header
        finishes, the barrier is removed and the body payload is returned so the
        caller can enqueue the callback exactly once."""
        conn = self._connect()
        row = conn.execute(
            """UPDATE ferry_chords SET remaining = remaining - 1
               WHERE chord_id = ?
               RETURNING remaining, body, task_ids""",
            (chord_id,),
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        remaining = row["remaining"]
        if remaining <= 0:
            conn.execute("DELETE FROM ferry_chords WHERE chord_id = ?", (chord_id,))
        conn.commit()
        return {"remaining": remaining, "body": row["body"], "task_ids": row["task_ids"]}

    # -- control plane: queue pause/resume ---------------------------------------
    def pause_queue(self, queue: str) -> None:
        """Pause a queue: workers skip it until :meth:`resume_queue` is called."""
        self._connect().execute(
            "INSERT OR REPLACE INTO ferry_meta (key, value) VALUES (?, '1')",
            (f"queue_paused:{queue}",),
        )
        self._connect().commit()

    def resume_queue(self, queue: str) -> None:
        self._connect().execute(
            "DELETE FROM ferry_meta WHERE key = ?", (f"queue_paused:{queue}",)
        )
        self._connect().commit()

    def paused_queues(self) -> list[str]:
        prefix = "queue_paused:"
        return sorted(
            r["key"][len(prefix):]
            for r in self._connect().execute(
                "SELECT key FROM ferry_meta WHERE key LIKE 'queue_paused:%'"
            ).fetchall()
        )

    # -- control plane: per-queue rate limits --------------------------------------
    def set_rate_limit(self, queue: str, rate: str | float) -> None:
        """Cap a queue at ``rate`` (e.g. ``"100/s"``, ``"10/m"``, ``"5/h"``).
        Workers collectively never claim faster than this; excess tasks wait.
        The bucket starts full, so a short burst up to one second's worth is
        allowed."""
        per_sec, original = parse_rate(rate)
        self._connect().execute(
            "INSERT OR REPLACE INTO ferry_meta (key, value) VALUES (?, ?)",
            (f"queue_rl:{queue}", f"{per_sec}|{original}"),
        )
        self._connect().commit()

    def get_rate_limit(self, queue: str) -> str | None:
        """The configured rate string for a queue, or None if unlimited."""
        row = self._connect().execute(
            "SELECT value FROM ferry_meta WHERE key=?", (f"queue_rl:{queue}",)
        ).fetchone()
        return row["value"].split("|", 1)[1] if row else None

    def clear_rate_limit(self, queue: str) -> None:
        self._connect().execute(
            "DELETE FROM ferry_meta WHERE key IN (?, ?)",
            (f"queue_rl:{queue}", f"queue_rl_bucket:{queue}"),
        )
        self._connect().commit()

    def rate_limits(self) -> dict[str, str]:
        prefix = "queue_rl:"
        out = {}
        for r in self._connect().execute(
            "SELECT key, value FROM ferry_meta WHERE key LIKE 'queue_rl:%'"
        ).fetchall():
            if r["key"].startswith("queue_rl_bucket:"):
                continue
            out[r["key"][len(prefix):]] = r["value"].split("|", 1)[1]
        return out

    def _rl_config(self, queue: str) -> tuple[float, str] | None:
        row = self._connect().execute(
            "SELECT value FROM ferry_meta WHERE key=?", (f"queue_rl:{queue}",)
        ).fetchone()
        if row is None:
            return None
        per_sec, _, original = row["value"].partition("|")
        return float(per_sec), original

    def _rl_consume(self, queue: str, now_ts: float) -> bool:
        """Token-bucket consume. True if the queue may claim right now."""
        cfg = self._rl_config(queue)
        if cfg is None:
            return True
        per_sec, _ = cfg
        burst = max(1.0, per_sec)
        key = f"queue_rl_bucket:{queue}"
        conn = self._connect()
        row = conn.execute("SELECT value FROM ferry_meta WHERE key=?", (key,)).fetchone()
        if row is None:
            tokens, ts = burst, now_ts
        else:
            tokens, ts = (float(v) for v in row["value"].split("|"))
        tokens = min(burst, tokens + (now_ts - ts) * per_sec)
        ok = tokens >= 1.0
        if ok:
            tokens -= 1.0
        conn.execute(
            "INSERT OR REPLACE INTO ferry_meta (key, value) VALUES (?, ?)",
            (key, f"{tokens}|{now_ts}"),
        )
        return ok

    def _rl_refund(self, queue: str) -> None:
        """Return one token (for queues that consumed but didn't claim)."""
        cfg = self._rl_config(queue)
        if cfg is None:
            return
        per_sec, _ = cfg
        burst = max(1.0, per_sec)
        key = f"queue_rl_bucket:{queue}"
        conn = self._connect()
        row = conn.execute("SELECT value FROM ferry_meta WHERE key=?", (key,)).fetchone()
        if row is None:
            return
        tokens, ts = row["value"].split("|")
        conn.execute(
            "INSERT OR REPLACE INTO ferry_meta (key, value) VALUES (?, ?)",
            (key, f"{min(burst, float(tokens) + 1.0)}|{ts}"),
        )

    # -- control plane: result expiry --------------------------------------------
    def expire_results(self, older_than_seconds: float) -> int:
        """Drop result payloads of done tasks older than ``older_than_seconds``.

        The task rows stay (for history); ``result_expired`` is set so
        ``AsyncResult.get()`` raises :class:`ResultExpired` instead of
        returning a stale payload."""
        from datetime import timedelta

        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat()
        cur = self._connect().execute(
            """UPDATE ferry_tasks SET result=NULL, result_expired=1
               WHERE status='done' AND result IS NOT NULL
                 AND result_expired=0 AND finished_at < ?""",
            (cutoff,),
        )
        n = cur.rowcount
        self._connect().commit()
        return n

    # -- control plane: bulk retry -------------------------------------------------
    def retry_dead(self, queue: str | None = None) -> int:
        """Requeue every failed/dead task (optionally limited to one queue)."""
        q = """UPDATE ferry_tasks
               SET status='queued', attempts=0, error=NULL, eta=NULL,
                   worker_id=NULL, claimed_at=NULL, finished_at=NULL
               WHERE status IN ('failed','dead')"""
        params: list = []
        if queue:
            q += " AND queue=?"
            params.append(queue)
        cur = self._connect().execute(q, params)
        self._connect().commit()
        return cur.rowcount

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
        return {
            "tasks": counts,
            "queues": queues,
            "workers": workers,
            "paused": self.paused_queues(),
            "rate_limits": self.rate_limits(),
        }

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
