# ADR 0006 — Changing a stream's backend is a copy, never a setting change

- **Status:** Proposed
- **Date:** 2026-08-26
- **Deciders:** rakaia maintainers
- **Related:** [ADR 0005](./0005-stream-positions-stay-a-counted-offset.md) (positions
  stay a counted offset — amended by this decision);
  [ADR 0002](./0002-framework-vs-protocol-server-boundary.md) (the store seam this
  relies on); `docs/store-streams-in-files.md`; `src/rakaia/migrate.py`,
  `src/rakaia/offsets.py`.
  Issues: [#233](https://github.com/joshbrooks/rakaia/issues/233) (this decision),
  #229 (the third backend that raised it), #232 (a cursor should name its store —
  the gap this decision documents; narrowed by the stream stamp, see the amendment),
  #350 (the stamp),
  #34 (deletion retires offsets permanently — why a round trip cannot restore a log).

## Context

Rakaia now has three stores behind one seam: the in-memory `StreamStore`, the
database-backed `DjangoStreamStore`, and the file-backed `JsonlStreamStore`. Which
one a Django deployment uses is a single setting, `RAKAIA_STORE`.

That setting looks like it names *where the log is*. It does not. It names which
store the process constructs. Changing it and restarting does not move a single
event: the application comes up against a different, empty log while every consumer
still holds a saved position, and every one of those positions is still syntactically
valid.

What happens next depends on a detail nobody would think to check — whether the two
stores issue the same offset *format*:

| Store pair | A bare position (no stream stamp) | A consumer that has run before |
|---|---|---|
| in-memory ↔ either other | `ForeignOffset`. Loud, immediate, correct. | The same. |
| `DjangoStreamStore` ↔ `JsonlStreamStore` | **Accepted.** Both issue `PLAIN`. | `replaced`: stops, applies nothing. |
| one store class, two locations | **Accepted.** Not a pair of backends at all. | `replaced`, as above. |

*The right-hand column is an amendment (see the end of this record): when this
was written there was only the left.*

All are symmetric — a format is refused in whichever direction it is carried,
and accepted in whichever direction too.

**The third row is not a variation of the second, and it is the more reachable
one.** Two `DjangoStreamStore`s on different database aliases are two independent
logs numbering their events from the same start. Nothing about them differs in format, because nothing
about them differs at all except where they keep their data — so there is not even
a format to compare, and a cursor crosses between them as freely as within one.
Measured on this tree, three events on one alias and ten on another: a position
saved against the first resumes against the second as `advanced`, delivering seven
events and skipping three permanently.

Worth knowing while reading the tests: the second row is *modelled* by two
`JsonlStreamStore`s at different roots, which is literally this third row — the
file says so, on the grounds that `rakaia.offsets` sees a format rather than a
class, and for the offset module that is exactly right. What demonstrates the
second row for real is the pin that `DjangoStreamStore` issues the same format
this one does (`tests/test_django_rakaia/test_offset_format_pin.py`). So one
setup has been standing in for both rows, and only the alias case below separates
them.

It is the more reachable row because this codebase uses that seam on purpose.
`DjangoStreamStore(using=…)` exists so a from-scratch rebuild can replay into a
disposable database; `DjangoExecutor` and `DjangoProjectionReader` take the same
argument, and `rebuild_and_verify` composes all three. Nothing in that path
resumes a consumer, so nothing is wrong today — but "the same store class" reads
as "the same store", and it is not.

The `DjangoStreamStore` ↔ `JsonlStreamStore` entry is new. Until `JsonlStreamStore` existed, every pair of stores
disagreed about format, so every cross-store cursor was refused by accident of
shape. That protection is gone for one pair of three, and it was never a designed
guarantee — `rakaia.offsets` refuses a cursor whose format it can see belongs to
another store, and two stores sharing a format are indistinguishable to it.

Measured on this tree, the accepted case splits in two:

- **Cursor ahead of the new store's head** — reported as `rewound`, and the consumer
  re-reads from the start. Over-cautious, but loud and safe.
- **Cursor at or below the new store's head** — accepted silently. The consumer
  resumes at that number in a log it has never seen, and the events before it are
  skipped permanently. No error, no flag.

The second is the failure this decision exists to rule out. It is not reachable by
normal operation; it needs someone to change a backend, which is exactly the sort of
once-per-deployment action that gets done from memory at an awkward hour.

`rakaia.migrate.migrate_stream` already does the move properly and reports what
survived. What was missing is the *rule*, written where someone looks before doing
it rather than after.

## Decision

**1. Moving a stream between backends is a copy.** `migrate_stream` (or
`migrate_all`) is the only supported way. Changing `RAKAIA_STORE` and restarting is
not a migration and is not supported.

**2. Consumer positions survive only when the copy says so.** `Migration.cursors_valid`
is the answer, established by comparing the offsets the copy produced against the
offsets it read — not by reasoning about the two backends beforehand. When it is
`False`, consumers must be reset before they are started again, and reset
*knowingly*.

**3. A log cannot be round-tripped back into the store it was deleted from.**
Deleting a stream retires its offsets permanently (#34), so a recreated stream
resumes numbering *above* the mark it retired. "Export, delete, re-import" — the
obvious way to try to rebuild a log in place — therefore cannot restore the original
numbering. `migrate_stream` reports this as `offsets_preserved: False`, and it is
named here because the technique looks safe and is not.

**4. Two backends sharing an offset format is a known gap, recorded rather than
fixed here.** `DjangoStreamStore` and `JsonlStreamStore` both issue `PLAIN`, so
`offsets.after` cannot tell one's cursor from the other's. #232 proposes recording
the issuing store alongside the position. This decision documents the rule that
keeps deployments out of that state; it does not close the hole underneath it.

## Alternatives considered

**Make `RAKAIA_STORE` refuse to change under a populated log.** Tempting, and
rejected: the store is constructed from a setting at process start, and nothing at
that moment knows what the *previous* backend was. Detecting it would mean the new
store carrying a record of a store it has never seen, which is a worse version of
#232 with none of its benefit.

**Give `JsonlStreamStore` its own offset format.** This looks like it would restore
the accidental protection, and it is worth writing down why it would not — the
obvious version of it does nothing at all.

`OffsetFormat.owns` matches on **field count, not width** (`src/rakaia/offsets.py:89`,
and the property's docstring says so deliberately; pinned by
`test_offset_format.py::test_a_width_pads_output_without_filtering_input`). Widths
are a padding and sort rule for offsets a store *issues*, not an input filter,
because clients legitimately send unpadded positions — the dashboard's `?after=42` is
one. So a "JSONL format" of eighteen digits instead of twenty is still one numeric
field, still matched by `PLAIN`, and changes nothing: measured on this tree, minting
18-digit offsets left `format_of` still reporting `durable` and every cross-store
cursor still accepted.

Separating them would mean a second numeric field, or a non-numeric shape — which
either invents a component the file store does not have, or gives up the unpadded
client offsets that work today.

Rejected on its own merits as well: two entry-counting stores sharing a format is
*why* a copy between them can preserve offsets exactly, which is the common and
supported path. Making a rare mistake loud by making the common case impossible is
the wrong trade. #232 gets both, because it records the issuing store rather than
trying to infer it from the token's shape.

**Document it only in `docs/store-streams-in-files.md`.** That page says it, and
`migrate.py`'s own docstring says it. Both are read by someone who has already
decided to use the file store. The tempting alternative — "just point it at the new
one" — is what the rule exists to rule out, and it looks like it works right up until
consumers resume, so it belongs where decisions are recorded.

## Consequences

### Positive

- The unsupported path is named, so "just change the setting" is a documented
  mistake rather than an undocumented one.
- The one case that fails *silently* is written down next to the two that fail
  loudly, which is the distinction an operator cannot derive from behaviour.

### Negative, and named rather than waved away

- The rule is documentation, not a guard. Nothing enforces it, and #232 is still
  open. A deployment that ignores this ADR gets exactly the failure described above.
- `migrate_stream` copies through the public store API, so what that API cannot
  carry does not cross: producer fencing state has no public setter, and a sliding
  TTL window restarts. Both are listed in `Migration.notes` rather than dropped
  silently, but they are still not carried.

### Neutral

- Nothing about the single-backend case changes. A deployment that never switches
  backends is unaffected by all of this.

## Amendment — the stream stamp narrows the gap (#350, #232)

Written after the fact, so the record above stays as it was decided.

Since 0.10 each stream carries a stamp, set when it is created, and a consumer
saves it beside its position. `consume` stops with `replaced` when the stamp it
meets is not the one it saved, applying nothing. The stamp belongs to the log
instance, not to the store class or the offset format, which is exactly the
identity #232's discussion could not find: it tells two aliases, and two roots,
apart. So the silent row of the table is now silent only for a position that
carries no stamp — a bare `poll(cursor)`, a cursor a caller committed by hand, or
one saved before stamps existed.

That is why #232's `backend` column was not built. It would have guarded the same
window with a label that could not tell two logs of one class apart, which is the
case the stamp does.

**What this decision still stands for.** Decision 1 is unchanged: the move is a
copy. Decision 2 gains a clause — a copy that kept every offset and the head also
keeps the source's stamp (`adopt_generation`, called by `migrate_stream`), because
it is the same log. Without that, `cursors_valid: True` was a promise a consumer's
next run broke with `replaced`; the migration tests missed it because they resumed
with a bare `poll`. A copy that renumbered keeps a stamp of its own, so consumers
stop, which is right for them.

**What remains open.** A position with no stamp adopts the stream's on its first
run, so it cannot tell which log it was saved against. That window closes as
consumers run, and is pinned in `test_cross_backend_cursors.py`.

## What would reopen this

- **Unstamped positions matter again.** The stream stamp (see the amendment) makes
  the silent row loud for any consumer that has run. If bare `poll` positions or
  hand-committed cursors turn out to be common, something has to stamp them on the
  way in, and this decision is again the only thing between an operator and skipped
  events. Amend, do not delete — the copy is still the supported move.
- **A fourth store.** Whether it shares `PLAIN` decides which row of the table it
  joins, and that is a decision to take deliberately rather than by picking a
  convenient format.
- **A supported dual-run or rollback story.** Both backends live at once, heads
  diverging, is the case most likely to put a consumer's cursor *below* the other
  store's head. Nothing here makes that safe and this ADR does not attempt it.
