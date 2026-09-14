"""Task canvases: Celery-style workflow primitives built on the broker.

- ``chain``: run tasks in sequence, feeding each result into the next.
- ``group``: run tasks in parallel, collect their results in order.
- ``chord``: run a group, then a callback with the list of results.

Signatures are created with ``Task.s(...)`` (mutable: the previous result is
prepended to args) or ``Task.si(...)`` (immutable: the result is ignored).
Use ``.set(queue=..., priority=..., countdown=..., ...)`` to tune options.

How it works: a chain link is stored as JSON on the task row; when a worker
acks the task done it enqueues the next link. A chord uses an atomic
decrement barrier (``broker.chord_init`` / ``broker.chord_task_done``) so the
callback fires exactly once, on whichever worker finishes the last header task.
Both brokers implement the same semantics, so canvases work identically on
SQLite and Redis.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from .results import AsyncResult
from .serialization import dumps, loads


class Signature:
    """A task invocation waiting to happen: name + args + kwargs + options."""

    def __init__(
        self,
        app,
        task_name: str,
        args: tuple = (),
        kwargs: dict | None = None,
        options: dict | None = None,
        immutable: bool = False,
        task_id: str | None = None,
    ):
        self.app = app
        self.task_name = task_name
        self.args = tuple(args)
        self.kwargs = dict(kwargs or {})
        self.options = dict(options or {})
        self.immutable = immutable
        self.task_id = task_id

    # -- building ------------------------------------------------------------
    def s(self, *args, **kwargs) -> Signature:
        """Clone with ``args`` prepended and ``kwargs`` merged (new wins)."""
        return Signature(
            self.app,
            self.task_name,
            tuple(args) + self.args,
            {**self.kwargs, **kwargs},
            dict(self.options),
            self.immutable,
        )

    def si(self, *args, **kwargs) -> Signature:
        """Immutable clone: a chain result is *not* passed to this task."""
        clone = self.s(*args, **kwargs)
        clone.immutable = True
        return clone

    def set(self, **options) -> Signature:
        """Clone with broker options updated (queue, priority, countdown, …)."""
        clone = self.clone()
        clone.options.update(options)
        return clone

    def clone(self) -> Signature:
        return Signature(
            self.app,
            self.task_name,
            self.args,
            dict(self.kwargs),
            dict(self.options),
            self.immutable,
        )

    # -- running --------------------------------------------------------------
    def apply_async(self) -> AsyncResult:
        """Enqueue this single signature, returning its ``AsyncResult``."""
        task_id = _enqueue_sig(self.app.broker, self.to_dict(), self.args)
        self.app.events.emit(
            "task_enqueued", {"task_id": task_id, "task_name": self.task_name}
        )
        return AsyncResult(self.app.broker, task_id)

    # -- serialization ---------------------------------------------------------
    def to_dict(self) -> dict:
        return {
            "task": self.task_name,
            "args": list(self.args),
            "kwargs": dict(self.kwargs),
            "options": dict(self.options),
            "immutable": self.immutable,
            "task_id": self.task_id,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"Signature({self.task_name!r}, args={self.args!r}, "
            f"kwargs={self.kwargs!r}, immutable={self.immutable})"
        )


def _enqueue_sig(
    broker,
    sig: dict,
    args: tuple | list,
    *,
    chain: str | None = None,
    chord_id: str | None = None,
    chord_index: int | None = None,
    default_queue: str = "default",
) -> str:
    """Enqueue one serialized signature, resolving countdown/eta options."""
    opts = sig.get("options", {})
    eta = opts.get("eta")
    if opts.get("countdown") is not None:
        eta = datetime.now(timezone.utc) + timedelta(seconds=opts["countdown"])
    return broker.enqueue(
        sig["task"],
        tuple(args),
        dict(sig.get("kwargs", {})),
        task_id=sig.get("task_id"),
        queue=opts.get("queue") or default_queue,
        priority=opts.get("priority", 0),
        max_retries=opts.get("max_retries", 3),
        eta=eta,
        chain=chain,
        chord_id=chord_id,
        chord_index=chord_index,
    )


def _new_ids(n: int) -> list[str]:
    return [uuid.uuid4().hex for _ in range(n)]


# -- chain ---------------------------------------------------------------------


class Chain:
    """``app.chain(s1, s2, ...)``: s1 runs, its result feeds s2, and so on."""

    def __init__(self, app, sigs: list[Signature]):
        if not sigs:
            raise ValueError("chain() needs at least one signature")
        self.app = app
        self.sigs = list(sigs)

    def apply_async(self) -> ChainResult:
        broker = self.app.broker
        ids = _new_ids(len(self.sigs))
        for sig, tid in zip(self.sigs, ids):
            sig.task_id = tid
        first, rest = self.sigs[0], self.sigs[1:]
        chain_json = dumps([s.to_dict() for s in rest]) if rest else None
        _enqueue_sig(broker, first.to_dict(), first.args, chain=chain_json)
        self.app.events.emit(
            "task_enqueued", {"task_id": ids[0], "task_name": first.task_name}
        )
        return ChainResult(broker, ids)


class ChainResult:
    """Handle on a running chain. ``get()`` waits link by link and returns the
    final result; the first failed link raises ``TaskFailed``."""

    def __init__(self, broker, task_ids: list[str]):
        self.broker = broker
        self.task_ids = list(task_ids)

    @property
    def task_id(self) -> str:
        return self.task_ids[-1]

    def get(self, timeout: float | None = None, poll_interval: float = 0.05):
        result = None
        for tid in self.task_ids:
            result = AsyncResult(self.broker, tid).get(
                timeout=timeout, poll_interval=poll_interval
            )
        return result

    def ready(self) -> bool:
        return all(AsyncResult(self.broker, tid).ready() for tid in self.task_ids)

    def successful(self) -> bool:
        return all(AsyncResult(self.broker, tid).successful() for tid in self.task_ids)

    def __repr__(self) -> str:  # pragma: no cover
        return f"ChainResult({self.task_ids!r})"


# -- group ---------------------------------------------------------------------


class Group:
    """``app.group(s1, s2, ...)``: run signatures in parallel."""

    def __init__(self, app, sigs: list[Signature]):
        self.app = app
        self.sigs = list(sigs)

    def apply_async(self) -> GroupResult:
        broker = self.app.broker
        ids = _new_ids(len(self.sigs))
        for sig, tid in zip(self.sigs, ids):
            sig.task_id = tid
            _enqueue_sig(broker, sig.to_dict(), sig.args)
            self.app.events.emit(
                "task_enqueued", {"task_id": tid, "task_name": sig.task_name}
            )
        return GroupResult(broker, ids)


class GroupResult:
    """Handle on a running group. ``get()`` returns results in signature order;
    the first failed task raises ``TaskFailed``."""

    def __init__(self, broker, task_ids: list[str]):
        self.broker = broker
        self.task_ids = list(task_ids)

    def get(
        self, timeout: float | None = None, poll_interval: float = 0.05
    ) -> list:
        return [
            AsyncResult(self.broker, tid).get(timeout=timeout, poll_interval=poll_interval)
            for tid in self.task_ids
        ]

    def ready(self) -> bool:
        return all(AsyncResult(self.broker, tid).ready() for tid in self.task_ids)

    def successful(self) -> bool:
        return all(AsyncResult(self.broker, tid).successful() for tid in self.task_ids)

    def __repr__(self) -> str:  # pragma: no cover
        return f"GroupResult({self.task_ids!r})"


# -- chord ---------------------------------------------------------------------


class Chord:
    """``app.chord([s1, s2], callback)``: run the header in parallel, then call
    ``callback`` once with the list of header results.

    The callback fires when every header task *succeeds*. If a header task
    dies, the chord stalls and the callback never runs (matching Celery's
    default behavior).
    """

    def __init__(self, app, header: list[Signature], body: Signature):
        if not header:
            raise ValueError("chord() needs a non-empty header")
        self.app = app
        self.header = list(header)
        self.body = body

    def apply_async(self) -> ChordResult:
        broker = self.app.broker
        chord_id = uuid.uuid4().hex
        header_ids = _new_ids(len(self.header))
        body_id = uuid.uuid4().hex
        body_dict = self.body.to_dict()
        body_dict["task_id"] = body_id
        broker.chord_init(chord_id, dumps(body_dict), dumps(header_ids))
        for i, (sig, tid) in enumerate(zip(self.header, header_ids)):
            sig_dict = sig.to_dict()
            sig_dict["task_id"] = tid
            _enqueue_sig(
                broker, sig_dict, sig.args, chord_id=chord_id, chord_index=i
            )
            self.app.events.emit(
                "task_enqueued", {"task_id": tid, "task_name": sig.task_name}
            )
        return ChordResult(broker, body_id, header_ids)


class ChordResult:
    """Handle on a running chord. ``get()`` first waits for the header (raising
    ``TaskFailed`` if any header task fails), then returns the callback result."""

    def __init__(self, broker, body_id: str, header_ids: list[str]):
        self.broker = broker
        self.body_id = body_id
        self.header_ids = list(header_ids)

    @property
    def task_id(self) -> str:
        return self.body_id

    def get(self, timeout: float | None = None, poll_interval: float = 0.05):
        for tid in self.header_ids:
            AsyncResult(self.broker, tid).get(
                timeout=timeout, poll_interval=poll_interval
            )
        return AsyncResult(self.broker, self.body_id).get(
            timeout=timeout, poll_interval=poll_interval
        )

    def ready(self) -> bool:
        return AsyncResult(self.broker, self.body_id).ready()

    def successful(self) -> bool:
        return AsyncResult(self.broker, self.body_id).successful()

    def __repr__(self) -> str:  # pragma: no cover
        return f"ChordResult(body={self.body_id!r}, header={self.header_ids!r})"


# -- worker integration ----------------------------------------------------------


def fire_continuations(broker, task: dict, result) -> list[tuple[str, str]]:
    """Enqueue whatever follows a successfully finished task.

    Called by the worker right after ``ack_done``. Returns ``[(task_id,
    task_name)]`` for everything enqueued (chain links, chord callbacks).
    """
    fired: list[tuple[str, str]] = []

    chain_raw = task.get("chain") or None
    if chain_raw:
        sigs = loads(chain_raw)
        if sigs:
            nxt, rest = sigs[0], sigs[1:]
            args = () if nxt.get("immutable") else (result,)
            tid = _enqueue_sig(
                broker,
                nxt,
                (*args, *nxt.get("args", [])),
                chain=dumps(rest) if rest else None,
                default_queue=task["queue"],
            )
            fired.append((tid, nxt["task"]))

    chord_id = task.get("chord_id") or None
    if chord_id:
        state = broker.chord_task_done(chord_id)
        if state is not None and state["remaining"] == 0:
            body = loads(state["body"])
            header_ids = loads(state["task_ids"])
            results = [
                broker.decode_result(broker.get_task(tid)) for tid in header_ids
            ]
            tid = _enqueue_sig(
                broker,
                body,
                (results, *body.get("args", [])),
                default_queue=task["queue"],
            )
            fired.append((tid, body["task"]))

    return fired


__all__ = [
    "Chain",
    "ChainResult",
    "Chord",
    "ChordResult",
    "Group",
    "GroupResult",
    "Signature",
    "fire_continuations",
]
