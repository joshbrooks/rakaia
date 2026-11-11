---
type: Concept
title: Effects & executors
description: Pure data descriptions of side-effects, applied (or dry-run recorded) by an executor.
tags: [concept, effects, executors]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
---

# Definition

Handlers return `Effect`s — pure data, not I/O — and a separate `Executor`
applies them. This makes replay idempotent (re-applying an upsert converges) and
handlers trivially testable. A `CollectingExecutor` records effects instead of
applying them, after running the checks a real write runs (collision, unresolved
`Ref`): the basis for dry-run and migration verification. Symbolic `Ref`s
let one effect bind to a sibling effect's generated primary key within a batch,
and `check_disjoint_defaults` enforces the invariant that lets several owners
compose one row safely.

# Public API

Imported from `rakaia`:

* Four database effects, one per operation, sharing the `RowEffect` base
  (`model_label`, `lookup` — both required): `Upsert(…, defaults, produces)`,
  `Update(…, defaults)`, `Delete(…, spare)`, `Retire(…, patch, spare,
  transition)`. `Effect` is the union of the four; `AnyEffect` also admits
  `ExternalEffect`.
* `Exclude(lookup)` / `SpareKeys(keys)` — the two shapes a `Delete`'s single
  `spare` field can take (a `Retire` takes only `SpareKeys`).
* `Transition(kind, key_fields)` — a `Retire`'s opt-in per-flip notification
  request; rejects empty `key_fields`.
* `ExternalEffect(kind, payload)` — **not** an `Effect`: no executor applies one.
  `replay()` returns them in `ReplayResult.external` for the caller to route.
* `Ref(produces, field="pk")` and `RefResolver` — resolve batch-local FK refs;
  `UnresolvedRefError`, `DuplicateProducesError` on misuse.
* `check_disjoint_defaults` — raise `EffectCollisionError` if two write effects
  write the same column of the same row (the multi-owner guard).
* `Executor` protocol; `CollectingExecutor` (dry-run); `RecordingExecutor`
  (wraps any executor, applies through it and keeps what it handed on — what a
  rebuild gate diffs); `InMemoryProjections` (dict-backed executor and reader; raises
  `MultipleRowsMatched` where Django raises `MultipleObjectsReturned`).
* `SqliteExecutor` and `SqliteProjectionReader` — replay into a SQLite file with only the
  standard library (no Django). A model label is a table created on first use, holding an
  integer `pk` and the row as JSON, so values must be JSON-serialisable. A batch runs in one
  savepoint and lands whole or not at all. Held to the same executor and reader contracts
  as `InMemoryProjections`; `skip_unchanged` is not implemented.
* Writing your own executor: `apply_effects(effects, *, upsert, update_run, delete,
  retire)` runs a batch in the shared order (writes, deletes, retires), with `Ref`s
  resolved and defaults checked, and returns an `ApplyReport`; you supply the four writes.
  `upsert` returns an `UpsertOutcome` (`created`, `written` or `skipped`) and a
  `RowAccessor` for the row. `OnDriftPolicy` (`"warn"` or `"raise"`) is the type of
  `replay(on_drift=…)`. `rakaia.testing.executor_contract.ExecutorContract` holds an
  executor to the same checks as the built-in ones.
* Django: `DjangoExecutor` (applies to the ORM, resolves `Ref`s);
  `diff_effects_against_rows` (verify replay reproduces existing rows).

# Demonstrated by

* [sqlite_replay](../examples/sqlite-replay.md) — replay into a SQLite file, no Django.
* [multi_owner](../examples/multi-owner.md) — `Ref`/`RefResolver`, `check_disjoint_defaults`, routing `ExternalEffect`s.
* [orders](../examples/orders.md) — `Update`, `ExternalEffect`, `CollectingExecutor` dry-run.
* [projection_cookbook](../examples/projection-cookbook.md) — `diff_effects_against_rows` verification.
* [partisipa_close](../examples/partisipa-close.md) — `Retire` stamped with the triggering
  event's timestamp; replay under a clock shifted six years writes the same values, and a
  deliberately `now()`-stamped variant is shown writing different ones.

# Known gaps

* `DjangoExecutor(skip_unchanged=True)` is not exercised by an example.
* `apply_effects`, `plan_appends` and the other names for writing your own store or
  executor are exercised by the built-in stores and a tested docs page, not by an example.

# Deeper reference

* Human docs: `docs/dry-run-and-executors.md`.
* Source: `src/rakaia/effects.py`, `src/rakaia/executors.py`, `src/django_rakaia/effect_executor.py`.
