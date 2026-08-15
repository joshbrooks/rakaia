"""Staged replay with per-stage aggregates — the orchestrator for this spike.

Like ``examples/partisipa_staged``, this lives in the example, not rakaia core;
it prototypes the shape so we can prove it before a core API lands. It extends
the staged idea in two ways this workflow needs:

* a stage may have **reduce steps** (`fn(refs) -> [Effect]`) that run once against
  the prior stages' projections — used for the ``Balance`` aggregate; and
* later stages read earlier ones through rakaia's read-only
  ``DjangoProjectionReader``, so the ``CycleClose`` guard is a pure function of
  the log.

Determinism holds because every stage's inputs are committed projections, which
are themselves pure functions of the stream.
"""

from __future__ import annotations

import json
from typing import Any

from django_rakaia import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from rakaia import upcast


def _events(store: Any, stream_path: str) -> list[dict[str, Any]]:
    messages, _ = store.read(stream_path)
    return [upcast(json.loads(m.data), stream_path) for m in messages]


def staged_replay(
    store: Any,
    stream_path: str,
    stages: dict[int, dict[str, Any]],
) -> None:
    """Apply each stage in order; later stages see all earlier stages' output.

    Within a stage, per-event handlers are applied *per event* in stream order
    — a fold — so two events that update the same row (last-write-wins, e.g. a
    corrected PROGRESS) don't collide inside one batch. Per-stage ``reduce``
    steps then run once against the committed projections.
    """
    events = _events(store, stream_path)
    executor = DjangoExecutor()
    for stage in sorted(stages):
        spec = stages[stage]
        refs = DjangoProjectionReader()
        for event in events:
            for form_type, fn in spec.get("events", []):
                if event.get("form_type") == form_type:
                    executor.apply([fn(event, refs)])  # fold; last write wins
        reduce_batch = []
        for reduce_fn in spec.get("reduce", []):
            reduce_batch.extend(reduce_fn(refs))
        if reduce_batch:
            executor.apply(reduce_batch)  # commit before the next stage
