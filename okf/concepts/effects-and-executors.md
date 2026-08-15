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
applying them: the basis for dry-run and migration verification. Symbolic `Ref`s
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
* `Executor` protocol; `CollectingExecutor` (dry-run).
* Django: `DjangoExecutor` (applies to the ORM, resolves `Ref`s);
  `diff_effects_against_rows` (verify replay reproduces existing rows).

# Demonstrated by

* [multi_owner](../examples/multi-owner.md) — `Ref`/`RefResolver`, `check_disjoint_defaults`, routing `ExternalEffect`s.
* [orders](../examples/orders.md) — `Update`, `ExternalEffect`, `CollectingExecutor` dry-run.
* [projection_cookbook](../examples/projection-cookbook.md) — `diff_effects_against_rows` verification.

# Known gaps

* `DjangoExecutor(skip_unchanged=True)` is not exercised by an example.

# Deeper reference

* Human docs: `docs/dry-run-and-executors.md`.
* Source: `src/rakaia/effects.py`, `src/rakaia/executors.py`, `src/django_rakaia/effect_executor.py`.
