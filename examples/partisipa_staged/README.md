# Staged replay — a spike for late-arriving cross-form links

An evaluation prototype for [issue #7](https://github.com/joshbrooks/rakaia/issues/7)
feature #1, and the follow-up to the [`formkit_submissions`](../formkit_submissions)
adoption spike.

> **This is a spike.** The `staged_replay` orchestrator here lives in the example,
> not in rakaia core — it prototypes the proposed API so we can prove the shape
> before building it properly. See [`docs/staged-replay.md`](../../docs/staged-replay.md)
> for the design.

## The problem it validates

In Partisipa, an `SF_1_2` social form must link to the `ida.Project` for its
`(suku, output)` — but the `TF_6_1_1` that *defines* that project usually arrives
**after** the social/finance forms. A single pass in stream order links the
`SF_1_2` to nothing, which is why Partisipa carries a reactive re-link signal
(`SeparatedSubmissionProject`) and one-shot backfill tasks
(`_task_sf12_backfill_project_ids`).

This spike shows a **two-stage replay** resolving those links with no backfill,
even when the dependent form precedes its reference entity, and self-healing when
the reference arrives late.

## Run

```sh
just partisipa-demo
```

Or directly:

```sh
cd examples/partisipa_staged
uv run python manage.py migrate
uv run python manage.py demo_staged
```

Expected output:

```
Seeded 4 submissions (every SF_1_2 precedes its TF_6_1_1 in the stream).
[1] NAIVE (signals today): 0 linked, 2 UNLINKED — SF forms processed before their TF link to nothing.
    sf-fatuberliu-water      NPO  — UNLINKED —
    sf-maubara-road          NPO  — UNLINKED —

[2] STAGED: 2 linked, 0 unlinked — stage 0 builds every Project before stage 1 links, so arrival order no longer matters.
    sf-fatuberliu-water      NM   Spring intake WS-014
    sf-maubara-road          NM   Culverts RD-227

[3] SELF-HEAL: appended sf-liquica-tank with no project yet -> link_reason=NPO (unlinked).
    late TF arrived -> re-replay links sf-liquica-tank to 'Storage tank WS-031' (link_reason=NM) — no backfill task.
    replayed again: (3, 3) -> (3, 3) rows — idempotent ✓

All staged-replay checks passed ✓
```

Each check is asserted hard — a regression raises `CommandError` and the command exits non-zero.

## How it maps to the proposal

| Proposed core API | In this spike |
|---|---|
| `@register_handler(..., stage=0)` reference handler | `handlers.project_registry` (TF_6_1_1 → `Project`) |
| `@register_handler(..., stage=1)` handler taking `(event, refs)` | `handlers.sf12_link` (SF_1_2 → `Sf12`, resolves project via `refs`) |
| `refs` read-only view of earlier stages | `staged_replay.Refs` (queries committed projections) |
| `staged_replay(store, stages)` in core | `staged_replay.staged_replay(...)` (example-local) |
| "signals today" baseline | `staged_replay.naive_replay(...)` (one pass, per-event apply) |

The contrast between `naive_replay` (check [1]) and `staged_replay` (checks [2]/[3])
*is* the argument for the feature.

## Files

* **`seed.py`** — submissions ordered so every `SF_1_2` precedes its `TF_6_1_1`.
* **`handlers.py`** — the two stage-aware handlers + their grouping.
* **`staged_replay.py`** — `naive_replay` vs `staged_replay`, and the `Refs` accessor.
* **`models.py`** — `Project` (reference, keyed by `(suku, output)`) and `Sf12`
  (dependent, with a `link_reason` mirroring Partisipa's `NM`/`NPO`).
* **`management/commands/demo_staged.py`** — seeds, runs all three asserted checks.

## Caveats

- Single stream, two form types, one dependency edge. Real Partisipa spans many
  forms across streams (see #7 feature #2, multi-stream merge) and deeper
  dependency graphs.
- `Refs` reads the *latest* projection (correct for link resolution). Time-correct
  aggregates would want an as-of snapshot — an open question for the core API.
