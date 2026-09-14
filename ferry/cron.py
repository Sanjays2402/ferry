"""Minimal cron expression parser for periodic tasks.

Supports the standard 5-field format (minute hour day-of-month month day-of-week)
with ``*``, ``*/n``, ``a,b``, ``a-b`` and ``a-b/n`` in every field.
"""

from __future__ import annotations

from datetime import datetime

_FIELD_RANGES = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 6)]  # Sun == 0


def _parse_field(field: str, lo: int, hi: int) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        step = 1
        if "/" in part:
            part, step_s = part.split("/", 1)
            step = int(step_s)
        if part == "*" or part == "":
            start, end = lo, hi
        elif "-" in part:
            start_s, end_s = part.split("-", 1)
            start, end = int(start_s), int(end_s)
        else:
            start = end = int(part)
        if not (lo <= start <= hi and lo <= end <= hi):
            raise ValueError(f"cron field {field!r} out of range {lo}-{hi}")
        values.update(range(start, end + 1, step))
    return values


class CronSchedule:
    """A parsed cron expression. ``next_after(dt)`` returns the next matching datetime."""

    def __init__(self, expression: str):
        fields = expression.split()
        if len(fields) != 5:
            raise ValueError(
                f"cron expression must have 5 fields, got {len(fields)}: {expression!r}"
            )
        self.expression = expression
        self._sets = [
            _parse_field(f, lo, hi) for f, (lo, hi) in zip(fields, _FIELD_RANGES)
        ]

    def matches(self, dt: datetime) -> bool:
        # cron day-of-week: Python Monday==0, cron Sunday==0
        dow = (dt.weekday() + 1) % 7
        parts = (dt.minute, dt.hour, dt.day, dt.month, dow)
        return all(p in s for p, s in zip(parts, self._sets))

    def next_after(self, dt: datetime) -> datetime:
        """Next datetime strictly after ``dt`` matching the schedule (minute resolution)."""
        from datetime import timedelta

        candidate = dt.replace(second=0, microsecond=0)
        # brute force is fine: at most ~525k minute steps per year, schedules hit far sooner
        for _ in range(525_600 * 2):
            candidate += timedelta(minutes=1)
            if self.matches(candidate):
                return candidate
        raise ValueError(f"no matching time found for cron {self.expression!r}")

    def __repr__(self) -> str:  # pragma: no cover
        return f"CronSchedule({self.expression!r})"
