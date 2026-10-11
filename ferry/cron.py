"""Minimal cron expression parser for periodic tasks.

Supports the standard 5-field format (minute hour day-of-month month day-of-week)
with ``*``, ``*/n``, ``a,b``, ``a-b``, ``a-b/n`` and ``a/n`` in every field.
Day-of-week accepts 0-7 (both 0 and 7 are Sunday). As in standard cron, when
*both* day-of-month and day-of-week are restricted (neither starts with ``*``)
a day matches if **either** field matches: ``0 0 1 * 1`` runs on the 1st of the
month and on every Monday.
"""

from __future__ import annotations

from datetime import datetime

_FIELD_RANGES = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]  # day-of-week: 0 and 7 == Sunday


def _number(text: str, field: str) -> int:
    try:
        return int(text)
    except ValueError:
        raise ValueError(f"bad number {text!r} in cron field {field!r}") from None


def _parse_field(field: str, lo: int, hi: int) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        if not part:
            raise ValueError(f"empty item in cron field {field!r}")
        step = 1
        has_step = "/" in part
        if has_step:
            part, step_s = part.split("/", 1)
            step = _number(step_s, field)
            if step < 1:
                raise ValueError(f"cron step must be at least 1 in field {field!r}")
        if part == "*":
            start, end = lo, hi
        elif not part:
            raise ValueError(f"cron field {field!r} is missing a value before '/'")
        elif "-" in part:
            start_s, end_s = part.split("-", 1)
            start, end = _number(start_s, field), _number(end_s, field)
            if start > end:
                raise ValueError(f"cron range {part!r} in field {field!r} is reversed")
        else:
            start = _number(part, field)
            end = hi if has_step else start  # "5/20" means "5-max/20"
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
        if 7 in self._sets[4]:  # Sunday may be written 0 or 7
            self._sets[4] = (self._sets[4] - {7}) | {0}
        # standard cron: day-of-month OR day-of-week unless one of them is a wildcard
        self._day_or = not (fields[2].startswith("*") or fields[4].startswith("*"))

    def matches(self, dt: datetime) -> bool:
        # cron day-of-week: Python Monday==0, cron Sunday==0
        dow = (dt.weekday() + 1) % 7
        minute, hour, dom, month, dow_set = self._sets
        if dt.minute not in minute or dt.hour not in hour or dt.month not in month:
            return False
        dom_ok, dow_ok = dt.day in dom, dow in dow_set
        return (dom_ok or dow_ok) if self._day_or else (dom_ok and dow_ok)

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
