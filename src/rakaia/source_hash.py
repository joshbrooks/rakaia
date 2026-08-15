"""
Source-hash utility for drift detection on versioned handlers and upcasters.

We hash the function's source body so that edits to a "frozen" handler are
detectable at replay time. The hash is captured at registration; later
re-resolution compares it against the live function's hash and warns (or
raises) on mismatch.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import textwrap
from typing import Any


def unwrap_handler(fn: Any) -> Any:
    """The function a callable's identity and source should be taken from.

    A `functools.partial` is the supported way to bind a dependency into a
    handler — a stage-0 handler is called `fn(event)`, so there is nowhere else
    to put one. Unwrapping it means `dotted_path` and `source_hash` describe the
    *wrapped* function, which is where the logic lives:

    * the recorded path stays importable, so `rehydrate()` can resolve it;
    * the hash tracks edits to the real body, so drift detection still works;
    * rebinding a different dependency is correctly *not* a logic change.

    Nested partials unwrap all the way down. Anything else is returned as-is.
    """
    while isinstance(fn, functools.partial):
        fn = fn.func
    return fn


def is_importable(fn: Any) -> bool:
    """Whether this callable's qualified name could be imported back.

    False for a closure or a lambda defined inside a function — its qualname
    carries a `<locals>` segment, and `rehydrate()` imports the module part of
    the recorded `dotted_path`, which for such a name is not a module.
    """
    qualname = getattr(unwrap_handler(fn), "__qualname__", "")
    return "<locals>" not in qualname


def hash_function_source(fn: Any) -> str:
    """
    Return a SHA-256 hex digest of the function's source body.

    Leading whitespace is normalised (textwrap.dedent) so that nested
    `def`s and decorator placement don't affect the hash.

    A `functools.partial` is unwrapped first (see `unwrap_handler`) — it used to
    be refused outright with a message claiming the handler was not in an
    importable source file, which was untrue and pushed callers towards a
    closure, whose hash covers only the wrapper and silently disables drift
    detection for whatever it calls.
    """
    fn = unwrap_handler(fn)
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError) as exc:
        raise ValueError(
            f"Cannot capture source for {fn!r}: {exc}. "
            "Versioned handlers must be defined in importable source files."
        ) from exc
    normalised = textwrap.dedent(src).encode("utf-8")
    return hashlib.sha256(normalised).hexdigest()
