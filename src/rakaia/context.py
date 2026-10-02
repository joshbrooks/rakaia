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
from collections.abc import Iterator, Mapping
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

    ``correlation`` is also the correlation id of every append in the block that
    gives none, so it is checked here, on entry: a value that is not a string of
    1 to 128 characters raises `ValueError` before anything inside the block
    runs, rather than at an append, or inside a model's ``post_save`` after the
    row it audits has already been saved.
    """
    from .types import check_correlation_id

    if "correlation" in fields:
        check_correlation_id(fields["correlation"])
    current = _provenance.get() or {}
    token = _provenance.set({**current, **fields})
    try:
        yield
    finally:
        _provenance.reset(token)


def get_provenance() -> dict[str, Any]:
    """The current ambient provenance (a copy; empty dict if none is set)."""
    return dict(_provenance.get() or {})


def merge_provenance(explicit: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Merge ambient provenance *under* `explicit` metadata (explicit wins).

    Returns None when both are empty, so a plain append with no provenance and
    no explicit metadata keeps the no-envelope default (`metadata=None`).
    """
    ambient = _provenance.get() or {}
    if not ambient and not explicit:
        return None
    return {**ambient, **(explicit or {})}


def correlation_for(explicit: str | None) -> str | None:
    """The correlation id an append records: `explicit`, else the ambient one.

    An explicit value is checked here, because `AppendOptions` checks its field
    only when it is built and a caller can set it afterwards. The ambient value
    was checked when its `provenance` block opened; checking it again guards
    only code that sets the private contextvar directly.
    """
    from .types import check_correlation_id

    if explicit is not None:
        return check_correlation_id(explicit)
    return check_correlation_id(get_provenance().get("correlation"))


def tags_and_correlation(options: Any) -> tuple[tuple[str, ...], str | None]:
    """The tags and correlation id an append with `options` records, checked.

    Every store calls this before it writes, so a bad value refuses the append,
    or the whole batch, with nothing written. Stores differ only in an item the
    batch would refuse anyway, such as one after a close: the in-memory and
    JSONL stores check it too, and the Django store skips it. `options` may be None or any object with the
    `AppendOptions` fields.
    """
    from .types import clean_tags

    return (
        clean_tags(getattr(options, "tags", ())),
        correlation_for(getattr(options, "correlation_id", None)),
    )
