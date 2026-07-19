# ADR 0001 — Order child collections by stable id + fractional index

- **Status:** Accepted
- **Date:** 2026-07-19
- **Deciders:** rakaia maintainers
- **Related:** [`projections-and-fan-out.md`](../projections-and-fan-out.md),
  [`tree-reconcile.md`](../tree-reconcile.md), reconcile helpers (PR #17),
  spikes #13 / #14 / #16, issue #7

## Context

rakaia projects an event stream into read models. A recurring shape is a parent
record fanning out into a **reorderable child collection** — FormKit repeaters,
an order's line items, a form's answers — frequently nested to **unbounded**
depth via a self-FK (`SeparatedSubmission.repeater_parent`). Replaying such a
projection must be **idempotent** and **orphan-free**, which the `reconcile_*`
helpers achieve with one shape: *upsert every current row, then a single
set-based reconcile `delete` of everything not in the kept set.*

The first helper, `reconcile_children`, keyed each row by its **positional
index** `(parent, idx)`. Two problems surfaced:

1. **Index-as-identity.** Reordering or inserting a child renumbers every
   subsequent `idx`, so each row's key changes: replay rewrites O(N) rows, and —
   worse — an external foreign key to a child row now points at a *logically
   different* item. Position conflated "which child" with "what position."
2. **Order representation matters independently.** Even once identity is stable,
   how you store *order* (dense index vs fractional index vs linked list) has very
   different reorder cost, read cost, robustness, and reconcile-composition
   properties.

Consumers in the target deployment (Partisipa → SQL views / Metabase) read
ordered, paginated data with `ORDER BY` / `LIMIT`, and the reconcile pattern is
fundamentally a **set** operation.

## Decision

1. **Key every child/tree row by a stable id** (a business key or an id assigned
   at ingestion), **never by position.** Position is an ordinary field.
2. **Represent order as a fractional index** field (sparse keys, lexorank-style),
   sorted with `ORDER BY`.
3. Use **`reconcile_tree`** (id-keyed, reconcile scoped to the whole subtree) for
   nested and/or reorderable collections. Keep **`reconcile_children`**
   (index-keyed) only for **fixed-order / append-only** collections where
   position *is* a stable identity.
4. **Reject** positional-index-as-identity and **reject** linked-list ordering
   (`next`/`prev`) for relational projections.

## Alternatives considered

| Model | Reorder writes | Ordered SQL read | Corruption blast radius | Composes with reconcile-delete |
|---|---|---|---|---|
| **Positional index as identity** | O(N) + **FK/identity corruption** | `ORDER BY` | local | ✓ (but identity broken) |
| **Stable id + dense index field** | O(N) index updates | `ORDER BY` | local | ✓ |
| **Stable id + fractional index (chosen)** | **O(1)** | `ORDER BY` | local | ✓ |
| **Linked list (`next`)** | O(1) (~3 pointers) | ✗ pointer-chase / recursive CTE | **global — one dangling `next` severs the tail** | ✗ deletes need pointer surgery |

- **Positional index as identity** — rejected: the renumber-on-reorder corrupts
  external references and forces O(N) rewrites.
- **Dense index as a field** — acceptable but O(N) writes per reorder; fine when
  reorders are rare or the projection is always re-derived from a full snapshot.
- **Linked list** — rejected for relational projections: it cannot be ordered by
  a query (you traverse the chain via a recursive CTE, which SQL rollups can't
  consume), a single dangling pointer loses the entire tail (a cycle hangs the
  reader), deleting an orphan requires rewriting its predecessor's pointer (so it
  does **not** compose with the set-based reconcile `delete`), and concurrent
  "insert after X" races lose updates. Its one advantage — O(1) reorder — is
  matched by a fractional index **without** any of those costs. Linked lists
  remain appropriate for in-memory structures, rich-text/CRDT piece tables, and
  append-only logs — not relational projections.

## Consequences

**Positive**

- O(1) reorder writes (fractional), and identity/FKs stay stable across reorders.
- Native, set-based ordered reads and pagination (`ORDER BY` / `LIMIT`).
- Composes with the `reconcile_*` "upsert all + one reconcile delete" pattern.
- Deterministic snapshot replay (order is a pure function of the snapshot).
- Graceful degradation: a missed/duplicated event yields at worst a misplaced or
  duplicate position that self-heals on recompute — the collection stays fully
  readable.

**Negative / caveats**

- Fractional keys eventually exhaust precision (many inserts between the same two
  neighbors) and need a **rebalance** — a local or full renumber. Amortized O(1),
  rare O(N); use lexorank-style rebalancing.
- Requires the **source to carry a stable child id**. If it does not, assign one
  at ingestion — otherwise a bare positional snapshot `[A,B,C] → [C,A,B]` is
  indistinguishable from "every slot's content changed," and no projection can
  recover "C moved."
- Position-as-a-field means a reorder still writes the moved row(s). If reorder
  churn must not disturb **order-insensitive** consumers (aggregates, counts,
  validation, search), separate order out — see the note below.

**Neutral — separating the order concern (orthogonal, not mandated here)**

Order-insensitivity is the lever: most projections don't care about child order.
When they don't, order can be split from content along two independent axes:

- **Output feed** — materialize the position projection as its own read-model with
  its own sync cursor (the change_id/watermark pattern). Order-insensitive
  consumers ignore it; order-aware consumers tail it. Needs no source change.
- **Input stream** — *if* the client is command-sourced (distinct `EditChild` vs
  `MoveChild` events), `MoveChild` events form a position stream that composes with
  staged replay (content = stage-0 reference, position = stage-1 dependent) and
  multi-stream merge for order-aware consumers. Not available under full-snapshot
  submissions.

These remain per-projection choices; this ADR fixes only the **row keying and
order representation**.
