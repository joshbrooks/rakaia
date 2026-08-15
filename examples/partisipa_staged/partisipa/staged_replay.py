"""Prototype of the *staged replay* feature proposed in issue #7 (feature #1).

This lives in the example, not in rakaia core — a spike to validate the shape
before a core API lands. It contrasts two ways of deriving projections from one
event stream:

* ``naive_replay`` — the "signals today" model: one pass in stream order,
  applying each event's effects immediately. A dependent form processed before
  its reference entity exists links to nothing. This reproduces Partisipa's
  need for reactive re-save signals and ``_task_*_backfill_project_ids``.

* ``staged_replay`` — the proposal: handlers are grouped by *stage*; each stage
  is applied in full before the next begins, and later stages read the earlier
  stages' output through rakaia's read-only ``DjangoProjectionReader``. Arrival
  order in the stream stops mattering, because the whole reference registry
  (stage 0) is built before any dependent projection (stage 1) runs.

Both share the same decode + upcast + dispatch path, so the only variable under
test is *when* effects become visible. Determinism holds because the reader only
reads committed projections, themselves a pure function of the log.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from django_rakaia import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from rakaia import upcast

Handler = Callable[[dict[str, Any], DjangoProjectionReader], Any]
HandlerSpec = tuple[str, Handler]  # (form_type, fn)


def _events(store: Any, stream_path: str) -> list[dict[str, Any]]:
    """Decode every event in the stream, upcast to the current schema."""
    messages, _ = store.read(stream_path)
    return [upcast(json.loads(m.data), stream_path) for m in messages]


def _dispatch(
    event: dict[str, Any],
    handlers: list[HandlerSpec],
    refs: DjangoProjectionReader,
) -> list:
    """Run the handlers whose form_type matches this event; collect Effects."""
    effects = []
    for form_type, fn in handlers:
        if event.get("form_type") == form_type:
            effects.append(fn(event, refs))
    return effects


def naive_replay(store: Any, stream_path: str, handlers: list[HandlerSpec]) -> None:
    """One pass, stream order, effects applied per event (signal-like)."""
    executor = DjangoExecutor()
    refs = DjangoProjectionReader()
    for event in _events(store, stream_path):
        effects = _dispatch(event, handlers, refs)
        if effects:
            executor.apply(effects)  # visible to *later* events only


def staged_replay(
    store: Any,
    stream_path: str,
    staged_handlers: dict[int, list[HandlerSpec]],
) -> None:
    """Stages applied in order; each stage's reader sees all prior stages."""
    events = _events(store, stream_path)
    for stage in sorted(staged_handlers):
        refs = DjangoProjectionReader()  # sees everything committed by earlier stages
        batch = []
        for event in events:
            batch.extend(_dispatch(event, staged_handlers[stage], refs))
        if batch:
            DjangoExecutor().apply(batch)  # commit before the next stage
