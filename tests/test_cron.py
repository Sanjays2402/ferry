from datetime import datetime

import pytest

from ferry.cron import CronSchedule


def test_every_minute():
    s = CronSchedule("* * * * *")
    assert s.next_after(datetime(2026, 9, 13, 10, 0, 30)) == datetime(2026, 9, 13, 10, 1)


def test_step():
    s = CronSchedule("*/15 * * * *")
    assert s.next_after(datetime(2026, 9, 13, 10, 7)) == datetime(2026, 9, 13, 10, 15)
    assert s.next_after(datetime(2026, 9, 13, 10, 15)) == datetime(2026, 9, 13, 10, 30)


def test_daily_midnight():
    s = CronSchedule("0 0 * * *")
    assert s.next_after(datetime(2026, 9, 13, 10, 0)) == datetime(2026, 9, 14, 0, 0)


def test_lists_and_ranges():
    s = CronSchedule("0 9-17 * * 1-5")
    assert s.matches(datetime(2026, 9, 14, 9, 0))  # a Monday
    assert not s.matches(datetime(2026, 9, 14, 8, 0))
    assert not s.matches(datetime(2026, 9, 13, 12, 0))  # a Sunday


def test_invalid():
    with pytest.raises(ValueError):
        CronSchedule("* * * *")
    with pytest.raises(ValueError):
        CronSchedule("61 * * * *")
