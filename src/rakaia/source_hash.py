"""
Source-hash utility for drift detection on versioned handlers and upcasters.

We hash the function's source body so that edits to a "frozen" handler are
detectable at replay time. The hash is captured at registration; later
re-resolution compares it against the live function's hash and warns (or
raises) on mismatch.
"""

from __future__ import annotations

import hashlib
import inspect
import textwrap
from typing import Any


def hash_function_source(fn: Any) -> str:
    """
    Return a SHA-256 hex digest of the function's source body.

    Leading whitespace is normalised (textwrap.dedent) so that nested
    `def`s and decorator placement don't affect the hash.
    """
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError) as exc:
        raise ValueError(
            f"Cannot capture source for {fn!r}: {exc}. "
            "Versioned handlers must be defined in importable source files."
        ) from exc
    normalised = textwrap.dedent(src).encode("utf-8")
    return hashlib.sha256(normalised).hexdigest()
