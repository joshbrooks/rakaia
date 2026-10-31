"""Two handlers a fingerprint cannot see into, and one that wraps a function it can.

`reads_database` reaches the live model manager instead of an injected reader, so
its behaviour depends on the database and an edit behind `OrderSummary` would go
unnoticed. `reads_clock` reads a clock object, and an edit to that object's `now`
is invisible for the same reason. Both are the shapes
`RAKAIA_STRICT_HANDLER_FINGERPRINTS` exists to refuse.

`make_closure` is the closure factory the demo contrasts with `functools.partial`:
its wrapper calls a captured callable directly, so the fingerprint stops at the
wrapper's own text.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from django.utils import timezone

from orders.models import OrderSummary


class SystemClock:
    """A clock as a dependency object rather than a module-level function."""

    def now(self) -> datetime:
        return timezone.now()


CLOCK = SystemClock()


def reads_database(event: dict) -> None:
    """Impure: it queries the projection's table instead of taking a reader."""
    OrderSummary.objects.filter(order_id=event.get("order_id")).exists()


def reads_clock(event: dict) -> dict[str, Any]:  # noqa: ARG001
    """Impure: the clock is an object whose behaviour is not in the fingerprint."""
    return {"recorded_at": CLOCK.now().isoformat()}


def make_closure(bound: Callable[[dict], Any]) -> Callable[[dict], Any]:
    """A closure that captures an already-bound callable and just calls it."""

    def _handler(event: dict) -> Any:
        return bound(event)

    return _handler
