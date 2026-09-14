from datetime import date, datetime, timezone
from uuid import UUID

import pytest

from ferry.serialization import dumps, loads


@pytest.mark.parametrize(
    "value",
    [
        42,
        3.14,
        "hello",
        True,
        None,
        [1, 2, {"a": [3]}],
        {"k": "v", "n": [1, 2]},
        datetime(2026, 9, 13, 12, 30, tzinfo=timezone.utc),
        date(2026, 9, 13),
        UUID("12345678-1234-5678-1234-567812345678"),
        b"\x00\x01binary",
        {1, 2, 3},
        frozenset("ab"),
        {"nested": {"when": datetime(2026, 1, 1), "ids": {UUID(int=0)}}},
    ],
)
def test_roundtrip(value):
    assert loads(dumps(value)) == value


def test_unserializable_raises_helpful_error():
    class Custom:
        pass

    with pytest.raises(TypeError, match="cannot serialize"):
        dumps(Custom())
