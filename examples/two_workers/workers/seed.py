"""The stream both workers read, and the rows the projection should end up with.

The backlog is deliberately longer than a demo needs to *show* the behaviour: the
second worker only has to be caught waiting once, and a stream that a first
worker can finish before a second process has even started cannot catch it. Two
hundred events keep the first worker inside its pass long enough to be contended.
"""

from typing import Any

STREAM = "two-workers:bookings"
CONSUMER = "bookings-projection"

#: How many events the two workers share.
BACKLOG = 200

#: How many events are appended after the workers have caught up. The resume
#: check reads these and must read *only* these.
TAIL = 5


def booking(n: int) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "form_type": "BOOKING",
        "key": f"bk-{n:04d}",
        "amount": f"{n}.00",
    }


def backlog() -> list[dict[str, Any]]:
    return [booking(n) for n in range(BACKLOG)]


def tail() -> list[dict[str, Any]]:
    return [booking(n) for n in range(BACKLOG, BACKLOG + TAIL)]
