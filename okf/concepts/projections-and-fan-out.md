---
type: Concept
title: Projections & fan-out
description: Helpers that turn a record into idempotent, orphan-free child/aggregate rows.
tags: [concept, projections]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
---

# Definition

The common projection shape is "one source record fans out into N rows" (a
repeater, line items, answers). Replaying that with plain `update_or_create`
leaks orphans when a later version has fewer children. These helpers emit
idempotent upserts plus a single reconcile pass scoped to spare exactly the rows
still present — so a rebuild converges with no stale rows and no double counting.

# Public API

Imported from `rakaia`:

* `reconcile_children(model_label, parent_lookup, child_key, items, defaults_fn)`
  — positional (index-keyed) fan-out.
* `reconcile_by_key(model_label, scope, key_fields, items, key_fn, defaults_fn, *,
  retire_filter=…, retire="delete"|patch, transition_kind=…)` — composite
  natural key, with soft-delete (`retire=` patch) and per-flip notifications.
* `reconcile_tree(model_label, scope_lookup, node_key, nodes, id_fn, defaults_fn)`
  — unbounded nested tree, orphan-safe at any depth.
* `reconcile_aggregate(model_label, scope_lookup, group_key, groups, *, owns=…,
  retire_filter=…, allow_full_clear=…)` — one recomputed aggregate row per group;
  `owns=` composes a **shared** row across several reducers (per-group `update`,
  vanished group null-cleared on this owner's columns only).
* `project_latest(messages, model_label, *, subject_of, defaults_of, …)` — each
  subject's latest snapshot folded into one row (with tombstone deletes).

# Demonstrated by

* [multi_owner](../examples/multi-owner.md) — `reconcile_aggregate(owns=)`, `reconcile_by_key(retire=)`.
* [formkit_submissions](../examples/formkit-submissions.md) — `reconcile_children`.
* [formkit_submissions (stream)](../examples/formkit-submission-stream.md) — `project_latest`.
* [partisipa_repeaters](../examples/partisipa-repeaters.md) — whole-subtree reconcile (hand-rolled tree case).
* [partisipa_close](../examples/partisipa-close.md) — `reconcile_children` in a reducer.

# Known gaps

* `reconcile_tree` (the shipped primitive) is not used directly — the repeaters
  spike hand-rolls the equivalent with `delete` + `exclude`.

# Deeper reference

* Human docs: `docs/projections-and-fan-out.md`, `docs/tree-reconcile.md`, `docs/alerts-projection.md`.
* Source: `src/rakaia/projections.py`.
