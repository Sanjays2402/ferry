"""JSON serialization for task arguments and results.

Strict enough to catch mistakes early, flexible enough for real payloads:
supports datetime, date, UUID, bytes, sets and nested combinations of them.
Anything else raises a TypeError with a helpful message instead of silently
producing garbage.
"""

from __future__ import annotations

import base64
import json
from datetime import date, datetime
from uuid import UUID

_TYPE_MARKERS = {
    datetime: "__datetime__",
    date: "__date__",
    UUID: "__uuid__",
    bytes: "__bytes__",
    set: "__set__",
    frozenset: "__frozenset__",
}


class _Encoder(json.JSONEncoder):
    def default(self, o):
        marker = _TYPE_MARKERS.get(type(o))
        if marker == "__datetime__":
            return {"__datetime__": o.isoformat()}
        if marker == "__date__":
            return {"__date__": o.isoformat()}
        if marker == "__uuid__":
            return {"__uuid__": str(o)}
        if marker == "__bytes__":
            return {"__bytes__": base64.b64encode(o).decode("ascii")}
        if marker == "__set__":
            return {"__set__": [self.default(i) if not _is_plain(i) else i for i in o]}
        if marker == "__frozenset__":
            return {"__frozenset__": [self.default(i) if not _is_plain(i) else i for i in o]}
        raise TypeError(
            f"ferry cannot serialize {type(o).__name__!r} objects. "
            "Task arguments must be JSON-compatible (or datetime/date/UUID/bytes/set)."
        )


def _is_plain(o) -> bool:
    return o is None or isinstance(o, (bool, int, float, str, list, dict))


def _decode(o):
    if isinstance(o, dict) and len(o) == 1:
        key = next(iter(o))
        if key == "__datetime__":
            return datetime.fromisoformat(o[key])
        if key == "__date__":
            return date.fromisoformat(o[key])
        if key == "__uuid__":
            return UUID(o[key])
        if key == "__bytes__":
            return base64.b64decode(o[key])
        if key == "__set__":
            return {_decode(i) for i in o[key]}
        if key == "__frozenset__":
            return frozenset(_decode(i) for i in o[key])
    if isinstance(o, dict):
        return {k: _decode(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_decode(i) for i in o]
    return o


def dumps(obj) -> str:
    return json.dumps(obj, cls=_Encoder, separators=(",", ":"))


def loads(s: str):
    return _decode(json.loads(s))
