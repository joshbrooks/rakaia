---
icon: lucide/microscope
---

# Delta events: partial updates and array operations (spike)

- **Date:** 2026-08-20
- **Status:** research note — not a decision. Decisions go in `docs/adr/`.
- **Spike branch:** `spike/delta-events` — `src/rakaia/deltas.py`,
  `tests/test_rakaia/test_deltas.py`, `tests/test_rakaia/test_deltas_integration.py`
  (81 tests, full gate green)
- **Related:** [ADR 0001](../adr/0001-ordering-child-collections-in-projections.md)
  (row keying and order representation),
  [ADR 0004](../adr/0004-handler-types-and-fold-order.md) (the extension point;
  its rejection of a "thin event" concept), `event-envelope.md`
  (`append_if_changed`), `projections.py` (`project_latest`)

## The question

Two event shapes were proposed, both to stop re-carrying a whole form when one
part of it changed:

1. **Partial update** — an event carrying only the changed fields. Motivated by
   Partisipa's *repeaters*: repeated child collections inside a form's `fields`
   blob, where editing one cell today re-appends every row.
2. **Array operations** — `AddPart` (key + index), `RemovePart`, `MovePart`, so a
   collection's structural changes are recorded as intent rather than inferred
   from two snapshots.

This note answers three things: whether the mechanism can be built cleanly (yes),
what it costs the rest of rakaia (six named hazards, all payable), and whether the
production data supports the stated motivation (**no — and the numbers are not
close**).

## What was built

`rakaia.deltas` — five frozen operation types, a pure `apply_delta`, a JSON wire
form, and `fold_snapshot` over a message window. One type per shape of change,
following `effects.py`: a `part_id` on a `SetField` is a type error, not a runtime
check.

```python
from rakaia.deltas import AddPart, MovePart, SetField, encode_patch, fold_snapshot

store.append(path, json.dumps(encode_patch([
    SetField(("fields", "total_amount"), 6000),
    AddPart("repeaterBalance", 2, "b7f3", {"balance_cash": 400}),
])).encode(), AppendOptions(label="patch"))

state = fold_snapshot(store.read(path).messages)   # snapshots + patches -> state
```

Two design points carried the weight, and both are pinned by tests:

**Parts have identity; positions do not.** ADR 0001 requires a stable child id
and notes that when the source has none, one must be *assigned at ingestion*.
`AddPart` **is** that assignment — it carries the id the producer mints and stamps
it into the folded row under `_part`, so `reconcile_tree(id_fn=…)` works directly
on folded output. `index` on an add or a move is a command parameter (a position
at the moment of the edit), never an identity.

**A delta with no base is refused, never guessed.** `fold_snapshot` raises
`NoBaseSnapshotError` when a patch has no preceding snapshot and
`DeltaConflictError` when an op does not fit the state (clearing an absent key,
moving an unknown part). Both mean "the fold is not standing where the producer
stood", and applying anyway would produce a state nobody ever saved. This is the
price the size saving is bought with, and it is charged loudly.

The mechanism works. That was never the hard part.

## What it costs the rest of rakaia

Six places assume every event is a full snapshot. Each is a *test* in
`test_deltas_integration.py` rather than a paragraph here, so the cost is
measured, and so a change that removes a hazard fails a test instead of going
unnoticed. All six are payable; two are sharp.

| # | Hazard | Remedy | Sharpness |
|---|---|---|---|
| 1 | **Content routing misses a patch.** `match_field` tests a glob against `str(event[match_field])`. A snapshot carries `form_type`; a patch does not, so the handler silently does not fire — an unmatched content-routed event is *normal*, so nothing raises | carry the routing fields on the patch alongside `ops` | **sharp** — silent |
| 2 | **Upcasters do not see inside a path.** A rename upcaster moves a *key*. In a patch the old name is a segment of a path string, so the upcaster passes it through and the fold resurrects the pre-rename key | a second, path-aware upcaster body per rename — which nothing makes you write | **sharp** — long fuse; only bites when a schema version lands, by which time the un-upcast patches are durable |
| 3 | `project_latest` folds a patch as a whole state, so the row becomes the diff. No exception on the way | fold before projecting | contained |
| 4 | **History rows stop being self-contained.** "What did this look like at version N" needs a re-fold from the last snapshot row. `peak_snapshot` (most fields wins) can never select a patch — accidentally right, for the wrong reason | re-fold, or keep a snapshot column | contained |
| 5 | **Random access is gone.** A tail read must reach the last snapshot or be handed a base. The choice between *periodic snapshots* and *an explicit base from the projection row* **is** the adoption decision | either, exercised in the spike | design work |
| 6 | **A delta is not an Effect.** Every Effect converges on re-application; adding a part twice does not. Idempotency lives at the fold, not the op | rebuild by re-folding then upserting, never by re-applying a delta to a row | inherent |

Hazards 1 and 2 are the ones to weigh. Both are *silent* under the current
registries — the machinery has no way to notice that a delta was routed nowhere or
upcast incompletely — and both would need registry-side support to be safe, which
is new surface in the place ADR 0004 just finished narrowing.

## Whether the data supports the motivation

Measured against the live Partisipa dataset (`partisipa-agent-bigbang`):
16,588 submissions with a `fields` blob, 70,910 history events over 16,791
subjects, 54,119 consecutive save pairs.

### The payloads are small

| | p50 | p90 | p99 | max |
|---|---|---|---|---|
| submission `fields` bytes | **366** | 1,096 | 1,867 | 9,356 |
| repeater rows per submission | 2 | 4 | — | 32 |

8,463 of 16,588 submissions have at least one repeater row; a repeater is a median
41% of the payload it sits in. The largest form blob in production is 9 KB.

### Two thirds of the log is already avoidable without any new event type

| | pairs | share |
|---|---|---|
| consecutive save pairs | 54,119 | |
| **`fields` byte-identical** (the row changed — status, timestamps — the payload did not) | **35,981** | **66%** |
| payload actually changed | 18,138 | 34% |

That 66% confirms ADR 0004's 62% figure on a larger sample. It is removed by
`append_if_changed`, which rakaia already ships, which the glossary already names,
and which the consumer has never imported. **Two thirds of the volume needs no new
event type at all.**

### On the third that did change, deltas save 8 MB

| | value |
|---|---|
| full-snapshot bytes over all changed pairs | 11.6 MB |
| delta-patch bytes for the same changes | 3.6 MB |
| saving | **69% — 8 MB** |
| patch/snapshot ratio | p50 **0.17**, p90 0.92 |
| pairs where the patch is **no smaller** | 1,691 (**9%**) |

For scale: `formkit_ninja_submissionevent` is 62 MB and `rakaia_streamevent`
42 MB. Eight megabytes across the entire production history is not a storage
problem, and 9% of the time the delta is the *larger* representation.

### The reorder case, which the array ops exist for, does not occur

7,161 repeater-array changes, classified by comparing the before/after arrays:

| shape | n | share | is index-keying already correct? |
|---|---|---|---|
| `content_only` — same length and order, cells edited | 5,158 | 72% | yes |
| `append_only` — rows added at the end | 1,106 | 15% | yes |
| `truncate_only` — rows dropped from the end | 693 | 10% | yes |
| `insert_shift` — a row inserted mid-list, survivors' order kept | 121 | 2% | no |
| `other` | 75 | 1% | no |
| **`pure_reorder` — a permutation, nothing else** | **8** | **0.1%** | no |

Rows a positional diff marks changed: 8,466. Rows an identity-aware diff needs:
8,281. **Over-reporting factor 1.02×.**

ADR 0001's caveat — that `[A,B,C] -> [C,A,B]` is indistinguishable from "every
slot's content changed" — is real, and it fires **eight times in the entire
production history**. 97% of repeater changes are exactly the fixed-order /
append-only shape the ADR says index-keyed `reconcile_children` already handles
correctly.

### The prerequisite nobody has

The array events can only carry intent the **producer actually has**. Under
whole-form saves the producer has to *infer* "row 3 moved to position 1" by
diffing two snapshots — and a diff over rows with no stable id cannot distinguish
a move from two edits any better than the projection can. So the fidelity
argument is conditional on the frontend becoming command-sourced (emitting "row
moved" as the user drags it), which is ADR 0001's own framing: *"if the client is
command-sourced … Not available under full-snapshot submissions."*

That is a producer-side change in a different repository, and it is the real
blocker. rakaia can accept a `MovePart` today; nothing upstream is in a position
to emit one.

## Recommendation

**Do not adopt either shape yet.** Stated as triggers rather than judgement, so
the answer is checkable when it changes:

1. **Land `append_if_changed` at the consumer first.** It removes 66% of the log
   with no new event type, no fold, and none of the six hazards. Doing anything
   else before this is optimising the 34% while ignoring the 66%.
2. **Keep the spike on its branch, unmerged.** It is 81 green tests and a
   working design; its value right now is as the answer to "what would this
   cost", not as shipped surface. `rakaia.deltas` is deliberately absent from
   `__all__` — the public API promises nothing here.

### What would reopen it

- **A command-sourced producer.** A frontend that emits row-level intent, not a
  whole-form save. Until one exists the array ops have no source of truth to
  carry, and the reorder count stays at 8.
- **Payloads an order of magnitude larger.** A form family whose blob runs to
  hundreds of kilobytes, or a repeater in the hundreds of rows — 9 KB and 32 rows
  are the current maxima and neither is close.
- **A latency problem, not a storage one.** The measured case is bytes-at-rest,
  where 8 MB is nothing. If a write path becomes slow *because* of payload size
  (mobile upload over a thin link, say), that is a different measurement and this
  note does not answer it.
- **Registry support for hazards 1 and 2.** If content routing and upcasting grow
  a way to fail loudly on a payload shape they cannot handle, the sharp half of
  the cost goes away and the balance changes.

The honest summary: the mechanism is sound and cheap to build, the stated reason
(efficiency) is not supported by the data by roughly an order of magnitude, and
the better reason (recovering reorder intent) is blocked on a producer that does
not exist.
