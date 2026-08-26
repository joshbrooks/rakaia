# ADR 0005 — Stream positions stay a counted offset; ULIDs are declined for now

- **Status:** Proposed
- **Date:** 2026-08-22
- **Deciders:** rakaia maintainers
- **Related:** [ADR 0002](./0002-framework-vs-protocol-server-boundary.md) (framework
  vs protocol-server boundary); `docs/protocol.md` § *Strictly Increasing*;
  `src/rakaia/offsets.py`, `src/django_rakaia/offsets.py`,
  `src/django_rakaia/models.py`, `src/rakaia/types.py`.
  [ADR 0006](./0006-changing-backends-is-a-copy.md) (changing a backend is a copy —
  amends the format table below).
  Issues: [#138](https://github.com/joshbrooks/rakaia/issues/138) (this decision),
  #206 / #182 / #178 (one home for the five position rules — landed),
  #34 (global monotonicity — closed, and the reason the watermark exists),
  #137 (the same question answered for `Stream-Seq` — closed),
  #62 (append-path query count; its first item has landed).

## Context

Every position rakaia hands out is a zero-padded decimal number. Two formats, both
declared in one place since #206 (`src/rakaia/offsets.py`):

| Format | Shape | Store |
|---|---|---|
| `COMPOUND` | `{read_seq:016d}_{byte_offset:016d}` | in-memory `StreamStore` |
| `PLAIN` | `{entry_id:020d}` | `DjangoStreamStore` **and** `JsonlStreamStore` |

> **Amended 2026-08-26 (#233).** The amendment is the second store on the `PLAIN`
> row — not a new row, and that is the whole point. When this was written there were
> two stores and two formats, one each, and the table read as a one-to-one mapping
> between them. It is still two formats; it is no longer two stores.
> `JsonlStreamStore` (#229) reuses `PLAIN` — deliberately, since it counts entries
> exactly as the durable store does, which is what lets a copy between the two
> preserve offsets exactly.
>
> The consequence a one-store-per-format table could not state: **two backends now
> issue the same format, so `offsets.after` can no longer tell a foreign cursor from
> a local one.** Every cross-store cursor used to be refused by accident of shape;
> that now
> holds for two of the three pairs. A `DjangoStreamStore` cursor is accepted by
> `JsonlStreamStore` and vice versa — silently, and wrongly, when it sits at or below
> the other store's head. [#232](https://github.com/joshbrooks/rakaia/issues/232)
> proposes recording the issuing store beside the position;
> [ADR 0006](./0006-changing-backends-is-a-copy.md) documents the rule that keeps a
> deployment out of that state in the meantime.
>
> Nothing in the decision below changes. The widths, the lock and the ULID answer are
> unaffected — this is the table catching up with a store that arrived after it.

Both widths were chosen by hand — sixteen because the in-memory store's byte
offset cannot exceed process memory, twenty because it covers a `BigAutoField`.
The durable format is minted by `Stream.get_next_offset_block`
(`src/django_rakaia/models.py:147-213`), which takes a row lock on
`StreamOffsetWatermark` and returns a contiguous integer block.

`docs/protocol.md` § *Strictly Increasing* requires offsets to be
lexicographically greater than all previously assigned, forbids raw UTC
timestamps, and names ULIDs as an acceptable alternative: 26 Crockford-base32
characters, a 48-bit millisecond timestamp big-endian first, so byte order equals
time order. Adopting them would remove the hand-chosen width, remove the lock,
and make positions unique without anyone counting.

The question is whether to take that trade. It is not urgent — the current scheme
is correct and both ceilings are unreachable — but it gets more expensive after
adoption grows, so it is worth settling rather than leaving to drift.

### What has already changed since the question was raised

Two of the three benefits originally claimed for ULIDs have shrunk, and this is
the main reason the answer below differs from the one the issue expected.

**The hand-picked width already has one home.** #206 put both formats' five rules
— first, width, next, validity, comparison — in `OffsetFormat`. The comment
warning that a change had to update three places in lockstep is gone; widening a
field is now a one-line edit.

**The redundant per-append aggregate is already gone.** The issue cites #62's
first item — a `SELECT` plus a `Max(offset)` scan on every append — as something
ULIDs would remove. Allocation is now a single locked `get_or_create`, and the
aggregate is gated on `watermark.high == 0`, which is reached once per stream path
ever (`models.py:181-214`). Pinned by
`test_a_steady_state_allocation_does_not_aggregate_over_entries` and
`test_a_steady_state_allocation_costs_two_queries`
(`tests/test_django_rakaia/test_offset_allocation.py:47`, `:90`). **The remaining
cost is two queries and one row lock per append, not a growing table scan.**

What is left of the original case: the width ceiling (now cheap to move), the
`StreamOffsetWatermark` table itself, and the lock.

### What it would cost, measured

The trade named in the issue is real: **a lock for a clock.** `select_for_update`
buys offsets that encode *definite append order*. Within one millisecond a ULID's
order falls to its random component, so two processes appending in the same
millisecond get unique offsets whose relative order need not match the order the
data landed. Monotonic-ULID variants fix that inside one process only. An NTP step
backwards would violate the protocol's own MUST.

Three further costs are concrete, and two of them were not in the issue:

1. **Our own server would reject a ULID.** `VALID_OFFSET_PATTERN`
   (`src/rakaia/types.py:66`) is `^(-1|now|\d+(_\d+)?)$` — digits only — and
   `read_decision.py:183` guards every client-supplied offset with it. It is also
   the one position rule #206 did not move into `rakaia.offsets`, so it is the
   first thing any ULID work has to relocate and widen.
2. **Block allocation is a genuine density dependence.** The bulk-append path
   reserves one contiguous integer block and assigns `start + i`
   (`src/django_rakaia/django_store.py:975-978`). Nothing depends on offsets being
   *gapless* — reads are a `>` filter or a scan — but this depends on getting N
   consecutive positions from one lock. Minting N ULIDs is fine and cheaper, but
   `get_next_offset_block(count)` and its contract disappear with it.
3. **One sentinel has no ULID equivalent.** `channels_views.py:44` uses
   `parse_offset(last_event_id) if last_event_id else 0` as "before everything";
   under ULIDs that becomes `"0" * 26`.

Blast radius, counted on this tree rather than estimated: **46 padded-string
literals across six test files**, and **17 test functions in
`test_offset_allocation.py`** asserting integer block-allocation semantics that
ULIDs would delete outright rather than rewrite. Against that,
`tests/store_contract.py:171-196` asserts only that offsets strictly increase and
resume correctly, and `test_offset_format.py`'s
`TestAThirdPartyStoreIsStillOrdered` already exercises non-digit offsets — those
are the safety net that a switch preserved behaviour.

Only two columns would change: `StreamEntry.offset` (`BigIntegerField`, carrying a
`unique_together` and two indexes) and `StreamOffsetWatermark`.
`ConsumerCursor.offset` is **already** `CharField(max_length=64)`, so a 26-character
position fits it today.

### The dry run already done

#137 asked this exact question about `Stream-Seq` and answered it the other way:
`migrations/0009_alter_stream_last_seq.py` widened an integer column to a
`CharField` precisely so a conforming ULID could be stored, and
`test_append_decision.py::test_seq_is_compared_lexicographically` pins that nothing
may reject one. `rakaia.offsets.after` likewise already falls back to byte-order
comparison for a format it does not recognise, and its docstring names ULIDs as a
legitimate third-party choice.

So rakaia is already ULID-*tolerant* where it consumes positions. The open question
is only whether it should become ULID-*minting* where it issues them, and those are
different commitments: the first costs nothing and is done, the second trades away
a guarantee.

## Decision

**1. `StreamEntry.offset` stays a counted, zero-padded integer, allocated under a
row lock.** The lock is now two queries and it buys definite append order — a
property a clock cannot return once given up, and one that no measurement here
shows us paying too much for.

**2. Consuming a foreign ULID position stays supported and untested-by-exception.**
`offsets.after` already orders an unrecognised token byte-wise, per the protocol.
Nothing in this decision narrows the `CursorStore` seam.

**3. `VALID_OFFSET_PATTERN` is wrong where it is, independent of this decision.**
It is a position rule living outside `rakaia.offsets`, and it is digits-only, so it
would reject a conforming third-party offset a client legitimately holds. Moving
and widening it is worth doing on its own merits and does not commit us to minting
ULIDs. Tracked separately rather than folded in here.

## Alternatives considered

**Adopt ULIDs now.** Removes the width ceiling, the watermark table and the lock.
Declined because the strongest two of the three original arguments have already
been answered by #206 and #62 without giving up append order, and because the
remaining work is larger than the issue suggested — a column migration under a
unique constraint and two indexes, the server's own validity guard, and the
block-allocation contract.

**Adopt ULIDs only in the in-memory store.** Rejected as strictly worse: it is the
store used by tests, demos and the conformance runs, so it would trade the one
place ordering is trivially correct for a divergence between the twins that
`store_contract.py` exists to prevent.

**Widen the padding now, pre-emptively.** Unnecessary. Since #206 the width is one
edit, and both ceilings — 10^16 bytes in a process's memory, 10^20 durable entries
— are unreachable.

## Consequences

### Positive

- Append order stays a property of the database, not of clock agreement between
  writers.
- No migration of a uniquely-indexed column, and no rewrite of the 17
  block-allocation tests.
- `ConsumerCursor.offset` being `CharField(64)` already means a future reversal
  does not have to touch the cursor table.

### Negative, and named rather than waved away

- `StreamOffsetWatermark` keeps growing one row per stream path, with no reaper.
  That is #62's second item and this decision does not address it.
- Every append keeps paying two queries and a row lock.
- The two widths remain a choice rather than a consequence. Cheap to change, still
  a number someone picked.

### Neutral

- Nothing about rakaia's *consumption* of positions changes. A third-party store
  handing us ULIDs works today and keeps working.

## What would reopen this

Any one of these, and the measurement to take when it happens:

- **Append throughput where the row lock is the bottleneck.** Measure lock wait on
  `StreamOffsetWatermark` under concurrent appends to one path; if it dominates,
  the trade inverts.
- **A second writer process that cannot share the lock** — a different database, or
  a partitioned deployment. Definite append order is then already unavailable, so
  the thing this decision protects no longer exists to protect.
- **A durable backend with nowhere to keep a watermark.**
- **The watermark table becoming an operational problem** on a deployment with a
  very large number of short-lived stream paths.

Re-take the numbers rather than quoting the ones above; they are dated
observations, and the queries that produced them are named inline so re-taking is
a paste rather than a reconstruction.
