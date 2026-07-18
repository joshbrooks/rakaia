# Multi-stream merge — a spike for replaying several form pipelines as one

An evaluation prototype for [issue #7](https://github.com/joshbrooks/rakaia/issues/7)
feature #2, building on the staged-replay ([`partisipa_staged`](../partisipa_staged))
and close-precondition ([`partisipa_close`](../partisipa_close)) spikes.

> **This is a spike.** The merge orchestrator lives in the example, not rakaia
> core — it prototypes the shape before a core API lands. See
> [`docs/multi-stream-merge.md`](../../docs/multi-stream-merge.md) for the design.

## The problem it validates

Every earlier spike read **one** stream and faked the multi-form shape with a
`form_type` discriminator. Real Partisipa keeps forms in **separate typed
pipelines** — an SF, a TF, an FF stream — with their own ingestion, schemas, and
history, and stitches them together downstream in SQL views / Metabase. A
subproject view consumes all of them.

This spike replaces that SQL stitch with a **deterministic merge replay**: read N
streams, consume their events in one total order, and feed the result to the same
staged replay a single stream would use. The merge itself is a trivial k-way
merge; the real work is the **order key** — the merged order must be a pure
function of the streams, or projections aren't reproducible.

## Run

```sh
just partisipa-merge-demo
```

Or directly:

```sh
cd examples/partisipa_merge
uv run python manage.py migrate
uv run python manage.py demo_merge
```

Expected output:

```
Seeded 11 events into 1 combined stream and across 3 form pipelines (forms/progress, forms/meetings, forms/finance).
[1] PARITY — 3 merged streams vs 1 combined stream:
    Fatuberliu   READY      —
    Maubara      NOT-READY  incomplete_projects, insufficient_meetings, negative_operational_balance
    → merged projection == single-stream baseline, and the merged order reconstructs the canonical sequence ✓

[2] DETERMINISM — stream argument order must not matter:
    paths given forwards vs reversed -> identical sequence.
    → merge is a pure function of the streams ✓

[3] TIE-BREAK — two events share a timestamp:
    f-mb-1 (forms/finance) and m-mb-1 (forms/meetings) both at 12:30 -> merged positions 7, 8.
    → equal timestamps break by (stream_path, offset), stably ✓

[4] SELF-HEAL — fixes arrive on separate pipelines:
    Maubara      READY      —
    → Maubara now READY; merged still matches the combined stream ✓
```

Each check is asserted hard — a regression raises `CommandError` and exits non-zero.

## The order key is the whole game

`merge_streams` sorts by `(event["ts"], stream_path, offset)`:

- **`ts`** — the envelope timestamp from the [pghistory](../partisipa_history) spike
  is the declared order key (this is where #12's envelope pays off — it's the
  merge key's home).
- **`stream_path`, `offset`** — a deterministic tiebreak, so two events sharing a
  timestamp across streams always resolve the same way, **independent of the order
  the streams are passed in**. The seed includes exactly such a tie at `12:30`.

## The checks have teeth

Each assertion was verified to fail on an injected regression:

- **removing the sort** (concatenate by stream) fails `[1] PARITY` — the merged
  order no longer matches the canonical sequence;
- **sorting by `ts` alone** (dropping the `(stream_path, offset)` tiebreak) fails
  `[2] DETERMINISM` — passing the paths forwards vs reversed produces different
  sequences.

## How it composes

Merge and staged replay are **orthogonal**, and both are needed:

| Concern | Handled by |
|---|---|
| Cross-**stream** sequencing (which event is "first") | the merge + its order key |
| Cross-**form** dependencies (a link needs a fact from another form) | staged replay + `refs` |

`staged_replay_events` takes an already-ordered event list, so the single-stream
baseline and the three-stream merge run the **identical** replay — the only
variable the parity check measures is where the events came from.

## Files

* **`seed.py`** — events with a `ts` order key, authored in canonical order, split across three pipelines, with a deliberate cross-stream tie.
* **`merge_replay.py`** — `merge_streams` (the k-way merge + order key) and `staged_replay_events` (the source-agnostic staged orchestrator).
* **`handlers.py`** — stage 0 facts, stage 1 balance aggregate, stage 2 readiness rollup.
* **`models.py`** — the five projections incl. the derived `Readiness` view.
* **`management/commands/demo_merge.py`** — builds both stream layouts and runs all four asserted checks.

## Caveats

- The merge is a full sort for clarity; the streaming form is a k-way heap over the
  stream heads (same order, O(events·log k)).
- The order key is the envelope `ts`. A non-monotonic key (a backdated correction)
  is still ordered deterministically, but "position in merged order" then differs
  from "arrival order" — fine for a rebuild-from-scratch projection, but a
  per-event **audit log** keyed by merged position would renumber, so such a log
  should key by `(stream_path, offset)` instead. See the design doc.
- A merged replay's resume point is a **vector of per-stream offsets**, not one
  number — the tie-in to the change_id / watermark sync work.
