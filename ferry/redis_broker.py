"""Redis broker: the same interface as :class:`SQLiteBroker`, backed by Redis.

Data layout (all keys prefixed with ``ferry:``):
- ``q:{queue}``            sorted set of ready tasks, score = -priority,
                           member = ``{seq:020d}:{task_id}`` (seq breaks ties FIFO)
- ``delayed:{queue}``       sorted set of not-yet-visible tasks, score = eta unix time
- ``task:{id}``            hash with all task fields
- ``sched``                hash scheduled_id -> task_id (periodic dedup)
- ``workers``              hash worker_id -> JSON heartbeat payload
- ``finished``             sorted set of finished task ids, score = finish unix time
- ``counts``               hash status -> count (for O(1) stats)
- ``seq``                  global sequence for FIFO tie-breaking

Claiming runs as a single Lua script: move due delayed tasks into the ready
set, then pop the highest-priority member. The script executes atomically, so
any number of workers share the broker with zero double-execution.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone

from .broker import _decode_args, _decode_result

_PREFIX = "ferry:"

_CLAIM_LUA = """
local ready = KEYS[1]
local delayed = KEYS[2]
local now = tonumber(ARGV[1])
local worker_id = ARGV[2]
local claimed_at = ARGV[3]
local per_sec = tonumber(ARGV[4]) or 0

-- move due delayed tasks into the ready set
local due = redis.call('ZRANGEBYSCORE', delayed, 0, now, 'LIMIT', 0, 500)
for i, tid in ipairs(due) do
  local pr = tonumber(redis.call('HGET', '""" + _PREFIX + """task:' .. tid, 'priority')) or 0
  local seq = redis.call('INCR', '""" + _PREFIX + """seq')
  redis.call('ZREM', delayed, tid)
  redis.call('ZADD', ready, -pr, string.format('%020d', seq) .. ':' .. tid)
end

local members = redis.call('ZRANGE', ready, 0, 0)
if #members == 0 then
  return nil
end

-- per-queue rate limit: token bucket checked atomically with the claim
if per_sec > 0 then
  local burst = tonumber(ARGV[5]) or per_sec
  local bucket = KEYS[3]
  local data = redis.call('HMGET', bucket, 'tokens', 'ts')
  local tokens = tonumber(data[1])
  if tokens == nil then tokens = burst end
  local ts = tonumber(data[2])
  if ts == nil then ts = now end
  tokens = math.min(burst, tokens + (now - ts) * per_sec)
  if tokens < 1 then
    redis.call('HSET', bucket, 'tokens', tokens, 'ts', now)
    return nil
  end
  redis.call('HSET', bucket, 'tokens', tokens - 1, 'ts', now)
end

-- pop the highest-priority ready task
redis.call('ZREM', ready, members[1])
local tid = string.sub(members[1], 22)
redis.call('HSET', '""" + _PREFIX + """task:' .. tid,
           'status', 'claimed', 'worker_id', worker_id, 'claimed_at', claimed_at)
redis.call('HINCRBY', '""" + _PREFIX + """counts', 'queued', -1)
redis.call('HINCRBY', '""" + _PREFIX + """counts', 'claimed', 1)
return tid
"""


_MARK_RUNNING_LUA = """
if redis.call('HGET', KEYS[1], 'status') == 'claimed' then
  redis.call('HSET', KEYS[1], 'status', 'running')
  redis.call('HINCRBY', '""" + _PREFIX + """counts', 'claimed', -1)
  redis.call('HINCRBY', '""" + _PREFIX + """counts', 'running', 1)
  return 1
end
return 0
"""


_CHORD_DONE_LUA = """
local rem = redis.call('HINCRBY', KEYS[1], 'remaining', -1)
if rem == 0 then
  local body = redis.call('HGET', KEYS[1], 'body')
  local tids = redis.call('HGET', KEYS[1], 'task_ids')
  redis.call('DEL', KEYS[1])
  return {rem, body, tids}
end
return {rem}
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class RedisBroker:
    """Broker backed by Redis. ``url`` like ``redis://localhost:6379/0``."""

    def __init__(self, url: str = "redis://localhost:6379/0", client=None):
        if client is not None:
            self._r = client
        else:
            try:
                import redis
            except ImportError as exc:
                raise RuntimeError(
                    'the redis broker needs the "redis" extra: pip install "ferry[redis]"'
                ) from exc
            self._r = redis.Redis.from_url(url, decode_responses=True)
        self._claim = self._r.register_script(_CLAIM_LUA)
        self._chord_done = self._r.register_script(_CHORD_DONE_LUA)
        self._mark_running = self._r.register_script(_MARK_RUNNING_LUA)

    # -- key helpers -----------------------------------------------------------
    def _k(self, *parts: str) -> str:
        return _PREFIX + ":".join(parts)

    # -- producing --------------------------------------------------------------
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
        from .serialization import dumps

        if scheduled_id is not None:
            existing = self._r.hget(self._k("sched"), scheduled_id)
            if existing:
                return existing
        if dedupe_key is not None:
            existing = self._r.hget(self._k("dedupe"), dedupe_key)
            if existing:
                t = self.get_task(existing)
                if t and t["status"] in ("queued", "claimed", "running"):
                    return existing
                # stale mapping for a finished task: drop it and enqueue fresh
                self._r.hdel(self._k("dedupe"), dedupe_key)
        if task_id is None:
            task_id = uuid.uuid4().hex
        if chain is not None and not isinstance(chain, str):
            chain = dumps(chain)
        eta_ts = eta.astimezone(timezone.utc).timestamp() if eta else 0
        pipe = self._r.pipeline()
        pipe.hset(
            self._k("task", task_id),
            mapping={
                "id": task_id,
                "queue": queue,
                "task_name": task_name,
                "args": dumps(list(args)),
                "kwargs": dumps(kwargs or {}),
                "priority": priority,
                "status": "queued",
                "attempts": 0,
                "max_retries": max_retries,
                "eta": eta_ts,
                "scheduled_id": scheduled_id or "",
                "result": "",
                "error": "",
                "worker_id": "",
                "created_at": _utcnow(),
                "claimed_at": "",
                "finished_at": "",
                "chain": chain or "",
                "chord_id": chord_id or "",
                "chord_index": "" if chord_index is None else chord_index,
                "dedupe_key": dedupe_key or "",
                "result_expired": 0,
                "time_limit": "" if time_limit is None else time_limit,
                "soft_time_limit": "" if soft_time_limit is None else soft_time_limit,
            },
        )
        if eta_ts and eta_ts > time.time():
            pipe.zadd(self._k("delayed", queue), {task_id: eta_ts})
        else:
            seq = self._r.incr(self._k("seq"))
            pipe.zadd(self._k("q", queue), {f"{seq:020d}:{task_id}": -priority})
        pipe.hincrby(self._k("counts"), "queued", 1)
        if scheduled_id:
            pipe.hset(self._k("sched"), scheduled_id, task_id)
        if dedupe_key:
            pipe.hset(self._k("dedupe"), dedupe_key, task_id)
        pipe.execute()
        return task_id

    # -- consuming ---------------------------------------------------------------
    def claim(self, queues: list[str], worker_id: str) -> dict | None:
        """Claim the highest-priority visible task. Queues with a rate limit
        (see :meth:`set_rate_limit`) are skipped while their token bucket is
        empty — enforced atomically inside the claim script."""
        now = time.time()
        rate_cfg = self._r.hgetall(self._k("rate_limits"))
        for queue in queues:
            per_sec = 0.0
            cfg = rate_cfg.get(queue)
            if cfg:
                per_sec = float(cfg.split("|", 1)[0])
            task_id = self._claim(
                keys=[self._k("q", queue), self._k("delayed", queue),
                      self._k("ratelimit", queue)],
                args=[now, worker_id, _utcnow(), per_sec, max(1.0, per_sec)],
            )
            if task_id:
                return self.get_task(task_id)
        return None

    def mark_running(self, task_id: str) -> bool:
        """Move a claimed task to running. Returns False if the task is no
        longer claimed (e.g. revoked between claim and start)."""
        return bool(self._mark_running(keys=[self._k("task", task_id)]))

    def revoke(self, task_id: str) -> bool:
        """Cancel a task that hasn't started yet. Returns False if it already
        started, finished, or doesn't exist."""
        t = self.get_task(task_id)
        if not t or t["status"] not in ("queued", "claimed"):
            return False
        members = self._r.zrange(self._k("q", t["queue"]), 0, -1)
        victim = next((m for m in members if m.endswith(":" + task_id)), None)
        pipe = self._r.pipeline()
        if victim:
            pipe.zrem(self._k("q", t["queue"]), victim)
        pipe.zrem(self._k("delayed", t["queue"]), task_id)
        dk = self._r.hget(self._k("task", task_id), "dedupe_key")
        if dk:
            pipe.hdel(self._k("dedupe"), dk)
        pipe.hset(
            self._k("task", task_id),
            mapping={"finished_at": _utcnow(), "worker_id": "", "claimed_at": ""},
        )
        pipe.execute()
        self._set_status(task_id, "revoked")
        self._r.zadd(self._k("finished"), {f"{task_id}:revoked": time.time()})
        return True

    def _set_status(self, task_id: str, new: str) -> str:
        """Set a task's status, adjusting counters from its actual previous status."""
        key = self._k("task", task_id)
        prev = self._r.hget(key, "status") or "queued"
        pipe = self._r.pipeline()
        pipe.hset(key, mapping={"status": new})
        if prev != new:
            pipe.hincrby(self._k("counts"), prev, -1)
            pipe.hincrby(self._k("counts"), new, 1)
        pipe.execute()
        return prev

    def ack_done(self, task_id: str, result) -> None:
        from .serialization import dumps

        key = self._k("task", task_id)
        dk = self._r.hget(key, "dedupe_key")
        pipe = self._r.pipeline()
        pipe.hset(key, mapping={"result": dumps(result), "finished_at": _utcnow()})
        if dk:
            # dedupe only collapses pending duplicates; a finished task frees its key
            pipe.hdel(self._k("dedupe"), dk)
        pipe.execute()
        self._set_status(task_id, "done")
        self._r.zadd(self._k("finished"), {f"{task_id}:done": time.time()})

    def ack_failed(self, task_id: str, error: str, retry_at: datetime | None) -> bool:
        task = self.get_task(task_id)
        if task is None:
            return False
        if retry_at is None:
            key = self._k("task", task_id)
            dk = self._r.hget(key, "dedupe_key")
            pipe = self._r.pipeline()
            pipe.hset(key, mapping={"error": error, "finished_at": _utcnow()})
            if dk:
                pipe.hdel(self._k("dedupe"), dk)
            pipe.execute()
            self._set_status(task_id, "dead")
            self._r.zadd(self._k("finished"), {f"{task_id}:dead": time.time()})
            return False
        eta_ts = retry_at.astimezone(timezone.utc).timestamp()
        self._r.hset(
            self._k("task", task_id),
            mapping={
                "attempts": int(task["attempts"]) + 1,
                "error": error,
                "eta": eta_ts,
                "worker_id": "",
                "claimed_at": "",
            },
        )
        self._set_status(task_id, "queued")
        self._r.zadd(self._k("delayed", task["queue"]), {task_id: eta_ts})
        return True

    def requeue_stale_claims(self, older_than_seconds: float = 120.0) -> int:
        # Redis-side: tasks claimed by workers with no recent heartbeat go back.
        # Heartbeats live in the workers hash with a unix timestamp.
        cutoff = time.time() - older_than_seconds
        live = set()
        for worker_id, payload in self._r.hgetall(self._k("workers")).items():
            try:
                if json.loads(payload)["ts"] >= cutoff:
                    live.add(worker_id)
            except (KeyError, ValueError):
                pass
        recovered = 0
        for key in self._r.scan_iter(match=self._k("task", "*")):
            t = self._r.hgetall(key)
            if t.get("status") in ("claimed", "running") and t.get("worker_id") not in live:
                pipe = self._r.pipeline()
                pipe.hset(key, mapping={"status": "queued", "worker_id": "", "claimed_at": ""})
                seq = self._r.incr(self._k("seq"))
                pipe.zadd(
                    self._k("q", t["queue"]), {f"{seq:020d}:{t['id']}": -int(t["priority"])}
                )
                pipe.hincrby(self._k("counts"), t["status"], -1)
                pipe.hincrby(self._k("counts"), "queued", 1)
                pipe.execute()
                recovered += 1
        return recovered

    # -- chords --------------------------------------------------------------------
    def chord_init(self, chord_id: str, body_json: str, task_ids_json: str) -> None:
        """Register a chord barrier: ``task_ids_json`` is the header task ids in
        order, ``body_json`` the serialized callback signature."""
        self._r.hset(
            self._k("chord", chord_id),
            mapping={
                "remaining": len(json.loads(task_ids_json)),
                "body": body_json,
                "task_ids": task_ids_json,
            },
        )

    def chord_task_done(self, chord_id: str) -> dict | None:
        """Atomically count one header task as finished. When the last header
        finishes, the barrier is removed and the body payload is returned so the
        caller can enqueue the callback exactly once."""
        res = self._chord_done(keys=[self._k("chord", chord_id)])
        if not res:
            return None
        out: dict = {"remaining": int(res[0])}
        if len(res) == 3:
            out["body"] = res[1]
            out["task_ids"] = res[2]
        return out

    # -- control plane: queue pause/resume ---------------------------------------
    def pause_queue(self, queue: str) -> None:
        """Pause a queue: workers skip it until :meth:`resume_queue` is called."""
        self._r.hset(self._k("paused"), queue, "1")

    def resume_queue(self, queue: str) -> None:
        self._r.hdel(self._k("paused"), queue)

    def paused_queues(self) -> list[str]:
        return sorted(self._r.hkeys(self._k("paused")))

    # -- control plane: per-queue rate limits --------------------------------------
    def set_rate_limit(self, queue: str, rate: str | float) -> None:
        """Cap a queue at ``rate`` (e.g. ``"100/s"``, ``"10/m"``, ``"5/h"``).
        Enforced atomically inside the claim script, so the limit holds across
        any number of workers."""
        from .broker import parse_rate

        per_sec, original = parse_rate(rate)
        self._r.hset(self._k("rate_limits"), queue, f"{per_sec}|{original}")

    def get_rate_limit(self, queue: str) -> str | None:
        cfg = self._r.hget(self._k("rate_limits"), queue)
        return cfg.split("|", 1)[1] if cfg else None

    def clear_rate_limit(self, queue: str) -> None:
        self._r.hdel(self._k("rate_limits"), queue)
        self._r.delete(self._k("ratelimit", queue))

    def rate_limits(self) -> dict[str, str]:
        return {
            q: cfg.split("|", 1)[1]
            for q, cfg in self._r.hgetall(self._k("rate_limits")).items()
        }

    # -- control plane: result expiry --------------------------------------------
    def expire_results(self, older_than_seconds: float) -> int:
        """Drop result payloads of done tasks older than ``older_than_seconds``.

        Rows stay for history; ``result_expired`` is set so ``AsyncResult.get()``
        raises :class:`ResultExpired` instead of returning a stale payload."""
        cutoff = time.time() - older_than_seconds
        n = 0
        for member in self._r.zrangebyscore(self._k("finished"), 0, cutoff):
            task_id, status = member.rsplit(":", 1)
            if status != "done":
                continue
            key = self._k("task", task_id)
            if self._r.hget(key, "result"):
                self._r.hset(key, mapping={"result": "", "result_expired": 1})
                n += 1
        return n

    # -- control plane: bulk retry -------------------------------------------------
    def retry_dead(self, queue: str | None = None) -> int:
        """Requeue every failed/dead task (optionally limited to one queue)."""
        n = 0
        for t in self.list_tasks(limit=100000):
            if t["status"] not in ("failed", "dead"):
                continue
            if queue and t["queue"] != queue:
                continue
            if self.retry_task(t["id"]):
                n += 1
        return n

    # -- workers -----------------------------------------------------------------
    def heartbeat(self, worker_id: str, queues: list[str], concurrency: int, hostname: str) -> None:
        self._r.hset(
            self._k("workers"),
            worker_id,
            json.dumps(
                {
                    "worker_id": worker_id,
                    "queues": ",".join(queues),
                    "concurrency": concurrency,
                    "hostname": hostname,
                    "ts": time.time(),
                    "last_beat": _utcnow(),
                    "started_at": _utcnow(),
                }
            ),
        )

    def remove_worker(self, worker_id: str) -> None:
        self._r.hdel(self._k("workers"), worker_id)

    def list_workers(self) -> list[dict]:
        workers = []
        for payload in self._r.hgetall(self._k("workers")).values():
            w = json.loads(payload)
            workers.append(
                {
                    "worker_id": w["worker_id"],
                    "queues": w["queues"],
                    "concurrency": w["concurrency"],
                    "last_beat": w["last_beat"],
                    "started_at": w["started_at"],
                    "hostname": w["hostname"],
                }
            )
        return sorted(workers, key=lambda w: w["started_at"])

    # -- inspection ----------------------------------------------------------------
    def get_task(self, task_id: str) -> dict | None:
        t = self._r.hgetall(self._k("task", task_id))
        if not t:
            return None
        t["priority"] = int(t["priority"])
        t["attempts"] = int(t["attempts"])
        t["max_retries"] = int(t["max_retries"])
        ci = t.get("chord_index")
        t["chord_index"] = int(ci) if ci not in (None, "") else None
        t["result_expired"] = t.get("result_expired") in ("1", 1, True)
        for f in ("time_limit", "soft_time_limit"):
            v = t.get(f)
            t[f] = float(v) if v not in (None, "") else None
        return t

    def get_task_by_scheduled_id(self, scheduled_id: str) -> dict | None:
        task_id = self._r.hget(self._k("sched"), scheduled_id)
        return self.get_task(task_id) if task_id else None

    def list_tasks(
        self, status: str | None = None, queue: str | None = None, limit: int = 100
    ) -> list[dict]:
        out = []
        for key in self._r.scan_iter(match=self._k("task", "*")):
            t = self._r.hgetall(key)
            if status and t.get("status") != status:
                continue
            if queue and t.get("queue") != queue:
                continue
            out.append(self.get_task(t["id"]))
            if len(out) >= limit:
                break
        return sorted(out, key=lambda t: t["created_at"], reverse=True)[:limit]

    def stats(self) -> dict:
        counts = {s: 0 for s in ("queued", "claimed", "running", "done",
                                 "failed", "dead", "revoked")}
        for s, n in self._r.hgetall(self._k("counts")).items():
            counts[s] = int(n)
        queues: dict[str, dict] = {}
        for key in self._r.scan_iter(match=self._k("q", "*")):
            qname = key.split(":")[-1]
            q = queues.setdefault(qname, {"queue": qname, "queued": 0, "active": 0, "dead": 0})
            q["queued"] += self._r.zcard(key)
        for key in self._r.scan_iter(match=self._k("delayed", "*")):
            qname = key.split(":")[-1]
            q = queues.setdefault(qname, {"queue": qname, "queued": 0, "active": 0, "dead": 0})
            q["queued"] += self._r.zcard(key)
        # active/dead per queue need a scan; cheap enough for a dashboard
        for t in self.list_tasks(limit=100000):
            q = queues.setdefault(
                t["queue"], {"queue": t["queue"], "queued": 0, "active": 0, "dead": 0}
            )
            if t["status"] in ("claimed", "running"):
                q["active"] += 1
            elif t["status"] == "dead":
                q["dead"] += 1
        return {"tasks": counts, "queues": sorted(queues.values(), key=lambda q: q["queue"]),
                "workers": self.list_workers(), "paused": self.paused_queues(),
                "rate_limits": self.rate_limits()}

    def throughput(self, minutes: int = 60) -> list[dict]:
        cutoff = time.time() - minutes * 60
        rows = []
        for member in self._r.zrangebyscore(self._k("finished"), cutoff, "+inf"):
            _task_id, status = member.rsplit(":", 1)
            score = self._r.zscore(self._k("finished"), member)
            minute = datetime.fromtimestamp(score, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M")
            rows.append({"minute": minute, "status": status, "n": 1})
        # aggregate per minute+status like the SQLite version
        agg: dict[tuple[str, str], int] = {}
        for r in rows:
            agg[(r["minute"], r["status"])] = agg.get((r["minute"], r["status"]), 0) + 1
        return [
            {"minute": m, "status": s, "n": n}
            for (m, s), n in sorted(agg.items())
        ]

    def retry_task(self, task_id: str) -> bool:
        t = self.get_task(task_id)
        if not t or t["status"] not in ("failed", "dead"):
            return False
        pipe = self._r.pipeline()
        pipe.hset(
            self._k("task", task_id),
            mapping={"status": "queued", "attempts": 0, "error": "",
                     "eta": 0, "worker_id": "", "claimed_at": "", "finished_at": ""},
        )
        seq = self._r.incr(self._k("seq"))
        pipe.zadd(self._k("q", t["queue"]), {f"{seq:020d}:{task_id}": -int(t["priority"])})
        pipe.hincrby(self._k("counts"), t["status"], -1)
        pipe.hincrby(self._k("counts"), "queued", 1)
        pipe.execute()
        return True

    def purge(self, queue: str | None = None, status: str = "queued") -> int:
        n = 0
        for t in self.list_tasks(status=status, queue=queue, limit=100000):
            # ready-set members are "{seq}:{task_id}"; find the exact member
            members = self._r.zrange(self._k("q", t["queue"]), 0, -1)
            victim = next((m for m in members if m.endswith(":" + t["id"])), None)
            pipe = self._r.pipeline()
            pipe.delete(self._k("task", t["id"]))
            if victim:
                pipe.zrem(self._k("q", t["queue"]), victim)
            pipe.zrem(self._k("delayed", t["queue"]), t["id"])
            pipe.hincrby(self._k("counts"), status, -1)
            pipe.execute()
            n += 1
        return n

    # -- payload helpers (shared semantics with SQLiteBroker) -----------------------
    @staticmethod
    def decode_args(task: dict) -> tuple[list, dict]:
        return _decode_args(task)

    @staticmethod
    def decode_result(task: dict):
        return _decode_result(task)


__all__ = ["RedisBroker"]
