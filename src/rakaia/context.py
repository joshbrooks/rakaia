"""
Ambient provenance for the event envelope.

`django-pghistory` captures *who* made a change by stamping a request's user
onto every DB write via `HistoryMiddleware`. rakaia's equivalent is a
contextvar: set ambient envelope metadata (an actor, a request path, an
import-batch id) for a block, and every `append` inside that block merges it
into the appended message's `metadata` — transparently, without threading it
through each call site.

    from rakaia.context import provenance

    with provenance(user=request.user.pk, url=request.path):
        store.append(path, data, AppendOptions(label="update"))
        # -> message.metadata == {"user": ..., "url": ...}

Explicit `metadata` passed to `append` takes precedence over the ambient values.
Because a reader only ever sees committed events, provenance stays a pure
per-append annotation.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from contextvars import ContextVar
from typing import Any

_provenance: ContextVar[dict[str, Any] | None] = ContextVar(
    "rakaia_provenance", default=None
)


@contextlib.contextmanager
def provenance(**fields: Any) -> Iterator[None]:
    """Set ambient envelope metadata for appends within this block.

    Nested blocks merge over the enclosing one; the previous value is restored
    on exit.
    """
    current = _provenance.get() or {}
    token = _provenance.set({**current, **fields})
    try:
        yield
    finally:
        _provenance.reset(token)


def get_provenance() -> dict[str, Any]:
    """The current ambient provenance (a copy; empty dict if none is set)."""
    return dict(_provenance.get() or {})


def merge_provenance(explicit: dict[str, Any] | None) -> dict[str, Any] | None:
    """Merge ambient provenance *under* `explicit` metadata (explicit wins).

    Returns None when both are empty, so a plain append with no provenance and
    no explicit metadata keeps the no-envelope default (`metadata=None`).
    """
    ambient = _provenance.get() or {}
    if not ambient and not explicit:
        return None
    return {**ambient, **(explicit or {})}
