"""Tiny in-process event bus.

Workers emit lifecycle events (enqueued/started/succeeded/failed/dead);
subscribe to observe, log, or forward them to your own metrics pipeline.

Events do not cross process boundaries — for the multi-process view, use the
dashboard, which reads the broker directly.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable


class EventBus:
    def __init__(self):
        self._subscribers: dict[str, list[Callable[[dict], None]]] = defaultdict(list)

    def on(self, event: str, handler: Callable[[dict], None]) -> Callable[[dict], None]:
        self._subscribers[event].append(handler)
        return handler

    def off(self, event: str, handler: Callable[[dict], None]) -> None:
        try:
            self._subscribers[event].remove(handler)
        except ValueError:
            pass

    def emit(self, event: str, payload: dict) -> None:
        for handler in list(self._subscribers.get(event, ())):
            try:
                handler(payload)
            except Exception:
                # event handlers must never break task execution
                pass
