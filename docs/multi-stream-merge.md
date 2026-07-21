# Multi-stream merge replay

> Status: **in core** ([issue #7](https://github.com/joshbrooks/rakaia/issues/7)
> feature #2). Prototyped in [`examples/partisipa_merge`](../examples/partisipa_merge);
> now shipped as `rakaia.merge_replay`. This page is the design; the section below
> shows the real API.

## Using it

```python
from rakaia.replay import merge_replay, ENVELOPE_TS
from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader

merge_replay(
    store,
    ["forms/sf", "forms/tf", "forms/ff"],   # several streams
    DjangoExecutor(),
    order_key=ENVELOPE_TS,                   # merge on the first-class envelope ts
    reader=DjangoProjectionReader(),        # required iff staged / reducers
)
```

`merge_replay` decodes and upcasts every event of every stream, tags each with
`(sort_value, stream_path, offset)`, sorts, and feeds the merged sequence through
the **same staged handler + reducer pipeline as `replay()`**. The
`(stream_path, offset)` tiebreak makes the order a pure function of the streams'
contents and **independent of the order `stream_paths` is passed**. By default
each event routes by its **source stream path** (so per-stream upcasters and
`match_field` content routing both work); pass `event_match` to route every merged
event by one string. Merged `seq` is the event's position in the merged order.

### Two order-key sources — pick the envelope, not the payload

`order_key` chooses **what** the merge sorts on, and there are two deliberately
distinct sources:

- **`ENVELOPE_TS`** (recommended) sorts on the first-class **envelope timestamp**,
  `StreamMessage.event_ts` — the producer's *logical* event time, set once at
  append via `AppendOptions(event_ts=…)` and defaulting to the append time when
  unset. This is what makes a backfill deterministic: a one-time import can stamp
  each event's *original historical* time as its `event_ts` even though every row
  is appended at ≈`now`, so replay orders by when the event *happened*, not when
  it was loaded.
- **a string** (default `"ts"`, kept for backward compatibility) sorts on a field
  read out of the **decoded payload body** (`event["ts"]`). This is *not* the
  transport timestamp and *not* the envelope — it is whatever the producer wrote
  into the JSON. A missing key raises. Historically merge only supported this, so
  producers had to duplicate a timestamp into the payload; `ENVELOPE_TS` removes
  that duplication.

> Note the transport timestamp (`StreamMessage.timestamp`, set to append time by
> the store) is **never** a merge key. Ordering on it would collapse a backfill to
> ≈`now` and lose historical order. See [the event envelope](event-envelope.md#the-envelope-timestamp).

## The problem: one projection, many streams

`replay()` reads a single `stream_path`. But a subproject view spans several
forms that, in real Partisipa, live in **separate typed pipelines** — SF, TF, FF —
each with its own ingestion, schema evolution, retention, and offset sequence.
Today they're stitched downstream in SQL views / Metabase. To own that in rakaia,
replay has to consume **N streams in one deterministic total order**.

## Why not just one stream

Keeping the streams separate at write time is deliberate: independent producers
and cadence, per-family upcasters, independent retention/access, and independent
offsets (which the change_id/watermark sync depends on). Merging at **read time**
keeps those concerns decoupled while still deriving one projection from all of
them.

## The proposal: merge by a declared order key

Each stream is already ordered (append-only), so the merge is a k-way merge. The
design decision that matters is the **order key**, constrained by one rule:

> The merged order must be a pure function of the streams' contents — stable
> across replays — or projections aren't reproducible.

`merge_streams` sorts by `(event[order_key], stream_path, offset)`:

```python
def merge_streams(store, stream_paths, order_key="ts"):
    tagged = []
    for path in stream_paths:
        for offset, event in enumerate(read_events(store, path)):
            tagged.append(((event[order_key], path, offset), event))
    tagged.sort(key=lambda item: item[0])       # deterministic total order
    return [event for _, event in tagged]
```

- **`order_key`** — the declared merge key. The **envelope timestamp**
  (`ENVELOPE_TS` → `StreamMessage.event_ts`) is its natural home: a producer-set
  logical time, so #12 and #2 compose. A plain-string key reading a payload field
  is also supported (see "Two order-key sources" above).
- **`(stream_path, offset)`** — the tiebreak. Two events with the same timestamp
  in different streams resolve identically every time, **independent of the order
  `stream_paths` is passed**. Without it, equal-timestamp events could swap
  between runs and silently change a last-write-wins field.

Merge and staged replay are **orthogonal**: the merge decides cross-*stream*
order; [staged replay](staged-replay.md) decides cross-*form* dependency order.
The example feeds the merged list to the same `staged_replay_events` the
single-stream baseline uses, so the only variable is the event source.

## Correctness corners

- **Determinism / tiebreak** — proven by passing the paths in different orders and
  getting the identical sequence; a `ts`-only sort fails this.
- **Non-monotonic keys** — a backdated correction is still ordered deterministically,
  but "position in merged order" ≠ "arrival order." For a rebuild-from-scratch
  projection with idempotent effects that's fine.
- **`seq` stability** — a per-event **audit log** must *not* key by merged position:
  a backdated append would renumber later rows. Key it by `(stream_path, offset)`,
  which is stable under merge. (This is the subtle interaction with the #12 history
  read-model.)
- **Resume** — a merged replay's cursor is a **vector of per-stream offsets**, not a
  single number; incremental/tailing merge needs a watermark per stream — the
  tie-in to the change_id/watermark sync quick win.

## What making it first-class requires

The spike carries the merge and the source-agnostic orchestrator as example code.
Promoting to core means:

1. `replay()` (or a `merge_replay()`) accepting **several stream paths** plus a
   declared `order_key`, with the `(order_key, stream_path, offset)` tiebreak.
2. A **k-way heap** merge for the streaming case (same order, without materializing
   every stream).
3. A documented rule that per-event history keys by `(stream, offset)`, not merged
   position.

## Relationship to the other spikes

- **Staged replay** (#7 #1) supplies the stages + `refs` the merged events feed.
- **Close preconditions** (#7 #5) is the projection this lets span real separate
  pipelines instead of one synthetic stream.
- **`reconcile_aggregate`** (#7 #3) — the balance rollup folds across all merged
  streams.
- **change_id / watermark sync** — the per-stream offset vector is the incremental
  merge's resume state.
