"""The rules a progress row is checked against before anything is appended.

Pure and model-free: a row in, a tuple of reason codes out, empty when the row is
acceptable. Codes rather than sentences, because an outcome stores codes — a
rendered message is where a field value leaks, and a code is countable.

The check deliberately does not stop at the first breach. A form arrives as a
document of rows and a user fixing it wants every problem in one pass, which is
also why the record for one row carries several reasons rather than several
records carrying one each.
"""

from __future__ import annotations

from typing import Any

KNOWN_OUTPUTS = ("WATER", "SANITATION", "ROAD")
"""The outputs a progress row may report against, as the reference data would."""


def check_row(row: dict[str, Any]) -> tuple[str, ...]:
    """Reason codes for everything wrong with `row`; empty when it is acceptable."""
    reasons: list[str] = []
    percent = row.get("percent")
    if not isinstance(percent, int) or not 0 <= percent <= 100:
        reasons.append("percent_out_of_range")
    if row.get("output") not in KNOWN_OUTPUTS:
        reasons.append("unknown_output")
    return tuple(reasons)
