"""Multi-stream merge replay — the orchestrator for this spike (issue #7 #2).

Like the earlier spikes this lives in the example, not rakaia core. It adds the
one capability the subproject view needs: consuming **several streams** in one
deterministic total order.

The merge itself is easy — each stream is already ordered, so it's a k-way merge.
The subtle part is the **order key**: the merged order must be a pure function of
the streams' contents, or projections aren't reproducible. So we sort by a
declared key (the envelope ``ts`` from the pghistory-retirement spike) with a
deterministic tiebreak ``(ts, stream_path, offset)`` — two events with the same
timestamp always resolve the same way, regardless of which order the streams are
passed in.

Once merged, the event list is fed to exactly the same staged replay the
single-stream baseline uses (`staged_replay_events`), so the *only* variable the
parity check measures is single-stream vs merged.
"""

from __future__ import annotations

import json
from typing import Any

from django_rakaia import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from rakaia import upcast


def read_events(store: Any, stream_path: str) -> list[dict[str, Any]]:
    """Decode + upcast every event in one stream, in append order."""
    messages, _ = store.read(stream_path)
    return [upcast(json.loads(m.data), stream_path) for m in messages]


def merge_streams(
    store: Any,
    stream_paths: list[str],
    order_key: str = "ts",
) -> list[dict[str, Any]]:
    """Merge N ordered streams into one deterministic total order.

    Sort key is ``(event[order_key], stream_path, offset)``: the declared order
    key first, then a stable tiebreak so equal-timestamp events across streams
    resolve identically on every replay and independently of the order in which
    ``stream_paths`` is given.
    """
    tagged: list[tuple[tuple[Any, str, int], dict[str, Any]]] = []
    for path in stream_paths:
        for offset, event in enumerate(read_events(store, path)):
            tagged.append(((event[order_key], path, offset), event))
    tagged.sort(key=lambda item: item[0])
    return [event for _, event in tagged]


def staged_replay_events(
    events: list[dict[str, Any]],
    stages: dict[int, dict[str, Any]],
) -> None:
    """Staged replay over an already-ordered event list (source-agnostic).

    Per-event handlers fold in order (last write wins, no batch collision);
    per-stage ``reduce`` steps run once against the committed projections. This
    is shared verbatim between the single-stream baseline and the merged run.
    """
    executor = DjangoExecutor()
    for stage in sorted(stages):
        spec = stages[stage]
        refs = DjangoProjectionReader()
        for event in events:
            for form_type, fn in spec.get("events", []):
                if event.get("form_type") == form_type:
                    effect = fn(event, refs)
                    if effect is not None:  # handlers may opt out per event
                        executor.apply([effect])
        reduce_batch = []
        for reduce_fn in spec.get("reduce", []):
            reduce_batch.extend(reduce_fn(refs))
        if reduce_batch:
            executor.apply(reduce_batch)
