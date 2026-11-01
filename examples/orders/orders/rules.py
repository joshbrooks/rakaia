"""Handler logic the hermeticity demo registers twice.

Kept in its own module because the point is what the *fingerprint* covers: a
policy value (`MINOR_UNITS`) read by this function is part of its digest, so an
edit to the policy is drift in every handler that wraps it. The two wirings
live in the demo command.
"""

from __future__ import annotations

from decimal import Decimal

MINOR_UNITS = Decimal(100)


def rate_value(event: dict, *, rates: dict[str, Decimal]) -> int:
    """Turn a currency amount into minor units using an injected rate table."""
    return int(Decimal(str(event["amount"])) * rates[event["currency"]] * MINOR_UNITS)
