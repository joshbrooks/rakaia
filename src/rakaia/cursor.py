"""
Stream cursor calculation for CDN cache collapsing.

Implements interval-based cursor generation to prevent infinite CDN cache loops
while enabling request collapsing. See Protocol Section 8.1.

The mechanism works by:
1. Dividing time into fixed intervals (default 20 seconds)
2. Computing interval number from an epoch (October 9, 2024)
3. Returning cursor values that change at interval boundaries
4. Ensuring monotonic cursor progression (never going backwards)
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass

# Default epoch: October 9, 2024 00:00:00 UTC (as Unix timestamp)
# datetime(2024, 10, 9, 0, 0, 0, tzinfo=timezone.utc).timestamp()
DEFAULT_CURSOR_EPOCH: float = 1728432000.0

# Default interval duration in seconds
DEFAULT_CURSOR_INTERVAL_SECONDS: int = 20

# Jitter range in seconds (per protocol spec: 1-3600)
MIN_JITTER_SECONDS: int = 1
MAX_JITTER_SECONDS: int = 3600


@dataclass
class CursorOptions:
    """Configuration for cursor calculation."""

    interval_seconds: int = DEFAULT_CURSOR_INTERVAL_SECONDS
    epoch: float = DEFAULT_CURSOR_EPOCH


def calculate_cursor(options: CursorOptions | None = None) -> str:
    """
    Calculate the current cursor value based on time intervals.

    Returns the interval number as a decimal string.
    """
    opts = options or CursorOptions()
    now = time.time()
    interval_number = int((now - opts.epoch) / opts.interval_seconds)
    return str(interval_number)


def generate_response_cursor(
    client_cursor: str | None,
    options: CursorOptions | None = None,
) -> str:
    """
    Generate a cursor for a response, ensuring monotonic progression.

    Algorithm:
    - If no client cursor: return current interval
    - If client cursor < current interval: return current interval
    - If client cursor >= current interval: return client cursor + jitter

    This guarantees monotonic cursor progression and prevents A→B→A cycles.
    """
    opts = options or CursorOptions()
    current_cursor = calculate_cursor(opts)
    current_interval = int(current_cursor)

    if not client_cursor:
        return current_cursor

    try:
        client_interval = int(client_cursor)
    except ValueError:
        return current_cursor

    if client_interval < current_interval:
        return current_cursor

    # Client cursor is at or ahead of current interval - add jitter
    jitter_seconds = random.randint(MIN_JITTER_SECONDS, MAX_JITTER_SECONDS)
    jitter_intervals = max(1, jitter_seconds // opts.interval_seconds)
    return str(client_interval + jitter_intervals)
