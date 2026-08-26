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
  the gap this decision documents rather than closes),
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

| Store pair | What a resumed consumer gets |
|---|---|
| in-memory ↔ either other | `ForeignOffset`. Loud, immediate, correct. |
| `DjangoStreamStore` ↔ `JsonlStreamStore` | **Accepted.** Both issue `PLAIN`. |

Both are symmetric — a format is refused in whichever direction it is carried,
and accepted in whichever direction too.

The second entry is new. Until `JsonlStreamStore` existed, every pair of stores
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

## What would reopen this

- **#232 lands.** A cursor that names its issuing store makes the silent row of the
  table above loud, at which point this decision is a convenience rather than the
  only thing standing between an operator and skipped events. Amend, do not delete —
  the copy is still the supported move.
- **A fourth store.** Whether it shares `PLAIN` decides which row of the table it
  joins, and that is a decision to take deliberately rather than by picking a
  convenient format.
- **A supported dual-run or rollback story.** Both backends live at once, heads
  diverging, is the case most likely to put a consumer's cursor *below* the other
  store's head. Nothing here makes that safe and this ADR does not attempt it.
