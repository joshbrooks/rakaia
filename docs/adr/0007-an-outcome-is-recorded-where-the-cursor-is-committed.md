# ADR 0007 — An outcome is recorded where the cursor is committed, not where the effect is applied

- **Status:** Proposed
- **Date:** 2026-09-05
- **Deciders:** rakaia maintainers
- **Related:** [ADR 0002](./0002-framework-vs-protocol-server-boundary.md) (the
  framework/protocol tiers this splits across);
  [ADR 0006](./0006-changing-backends-is-a-copy.md) (three stores behind one seam —
  why an outcome record cannot be Django-shaped by default);
  [ADR 0005](./0005-stream-positions-stay-a-counted-offset.md) (an offset is an
  opaque token, which is why an outcome's is a string);
  `src/rakaia/subscription.py`, `src/rakaia/protocols.py`,
  `src/django_rakaia/effect_executor.py`, `src/rakaia/replay.py`.
  Issues: #232 (a cursor should name its store — an outcome inherits this gap
  unchanged), #34 (deletion retires offsets permanently).

## Context

A consumer applies an event and the apply fails. Rakaia has nowhere to say so.

There is no dead-letter queue, no park, no attempt count, no per-event error record
anywhere in the tree. `DriftLedger` is the closest thing, and it is per *rule*,
deduplicated, in memory, and handed back on the result object rather than stored.
`ApplyReport` carries `created`/`written`/`skipped` counts per batch, and only its
`retire_flips` are read (`replay.py:521`, `:553`) — the counts go nowhere. What
survives a restart is the cursor, and a cursor says only how far a consumer got — not
whether it got there cleanly.

That gap has a specific consequence, and it is not "we lack observability". It is
that **absence of a record is read as success**. A consumer whose cursor is past
offset 40 asserts nothing about offsets 1–39. If one of them was skipped by a bulk
write path, or applied by a handler that raised and was swallowed upstream, nothing
in rakaia distinguishes that from forty clean applies. The first consumer to hit this
in production reported a form as imported when its row had been refused.

Three things about this codebase decide the shape of the answer, and two of them rule
out the obvious implementation.

**The executor cannot record it.** `DjangoExecutor.apply` wraps its batch in
`transaction.atomic` (`src/django_rakaia/effect_executor.py:204`). An outcome row
written inside that block rolls back with the batch whose failure it exists to
record. Worse, it could not name the event anyway: a stage-0 pass buffers many events
into one `apply()` (`_StageBuffer`, `src/rakaia/replay.py:434`), and `RowEffect`
carries `model_label` and `lookup` and nothing else (`src/rakaia/effects.py:176-189`).
By the time effects reach an executor, which event produced which effect is not
recoverable.

**There is no loop to record it in.** `poll → apply → commit` is written by hand by
each consumer. `django_rakaia.subscription` is 46 lines of free functions and holds no
`try`. In `replay.py` the only `try` on the dispatch path is a bare `finally`
(`:275-326`) whose comment explains it is there to flush buffered effects, not to
catch anything; the four `except` clauses elsewhere in the file cover signature
introspection, a non-hashable dedup key, merge-order comparability and JSON decoding,
and each either returns a default or re-raises. None of them catches a handler
exception, and neither does anything else — it propagates out of `replay()` uncaught.
So there is no existing site
where an outcome *would* be written; this decision introduces the site as well as the
record.

**A Django-only table would be the second mistake of a kind already regretted.**
Migration 0008 says it plainly of the removed demo model: *"Because it was in
`0001_initial`, **every** consumer got the table whether or not they ever used it."*
Meanwhile `docs/framework-vs-protocol-server.md` lists cursors **twice** — *"Subscriber
cursors … Python stdlib only"* (`:33`) and *"Durable subscriber cursors — persisted
watermarks … Django (ORM)"* (`:39`). That split is the template. `poll` is stdlib
because the decision is store-agnostic; only the place to keep the answer is Django.
An outcome keyed by `(consumer, stream_path, offset)` is store-agnostic in exactly the
same way, and there are now three stores.

## Decision

**1. An outcome is a core concept with pluggable storage.** `Outcome`,
`OutcomeStatus`, `Stage` and an `OutcomeStore` protocol live in `rakaia`, dependency-free,
with `OutcomeStore` in `src/rakaia/protocols.py` beside `CursorStore`. Django supplies a
durable place to keep them and nothing else, mirroring `load_cursor`/`commit_cursor`.
A shared contract suite at `tests/outcome_store_contract.py` is subclassed once per
backend, so "does this work on JSONL?" is a red test rather than a question.

**2. The record is written where the cursor is committed.** Rakaia gains the consume
loop it has never had, next to `poll`. It applies, records any outcome in its own
transaction — outside the executor's — and then commits the cursor. Nothing is
recorded from inside an `Effect`, an executor, or a handler.

**3. Only exceptions are recorded; the cursor is the success record.** Everything
below the cursor succeeded unless an outcome says otherwise, and anything the cursor
never reached is visibly unprocessed. The alternative — a row per applied event — is a
durable write on the hot path for the case that is fine, and the append cost tests exist
to catch exactly that.

There is deliberately **no gap query**, and an earlier draft of this decision was wrong to
promise one. It proposed reporting "offsets below the cursor with no outcome recorded",
which under this very decision is *every event that worked*: success writes nothing, so
absence of a record is not a gap, it is the normal case. The two halves contradicted each
other and the contradiction was only visible once a contract test had to state an expected
value.

What the model actually gives, and it is enough:

- **Lag** is the head against the cursor. No outcomes required.
- **Below the cursor**, an outcome means it failed, was refused or was skipped; no outcome
  means it succeeded. By construction, with no third state.
- An event below the cursor that neither succeeded nor recorded is a **defect in the loop**,
  not a data condition. A test pins the loop; no runtime query can distinguish it, because
  the design deliberately left no trace of success to look for.

The class this was reaching for — a bulk write bypassing the pipeline, leaving derived rows
missing and no record saying so — is answered structurally instead. A write that does not go
through the loop does not advance the cursor, so the events are still pending and are still
delivered. The bypass stops being a thing to detect.

**4. Where the event is decides how it is recovered, and replay is only one of the
answers.** `stage` says whether the event reached the log; `status` says what happened to
it. Together they name the recovery, and a re-drive that assumed every recorded outcome
was replayable would take the wrong action on most of them.

| `stage` | `status` | Where the fact is | Recovery |
|---|---|---|---|
| `project` | `failed` | Safe in the log, unapplied | Replay it |
| `append` | `failed` | **Gone.** The append was attempted and lost | Re-produce from whatever the consumer derived it from |
| `append` | `refused` | Safe upstream, deliberately not logged | Fix the data upstream and let it be produced again |
| either | `skipped` | Wherever it was | None wanted — not applying it *was* the decision |

The third row is the one worth spelling out, because it looks like the second and is not.
A consumer that gates its events refuses some of them *on purpose* — the log is the
system of record, so it must not carry a row the consumer declined to accept — and the
fact is not lost at all, it is sitting in whatever the consumer produced the event from.
Nothing is missing; something was rejected. Treating that as a lost append would send a
re-drive hunting for a fact that is exactly where it should be, and treating it as an
unapplied projection would send a replay looking for an event that was never written.

**5. The failure policy is a parameter with per-mode defaults, never inferred.**
`on_error="skip"` when consuming continuously, `on_error="halt"` when rebuilding. The
same shape as `on_drift` (`src/rakaia/drift.py:41`), and for the same reason: one code
path serving two operations with opposite invariants is how a guard ends up correct in
one mode and silently wrong in the other.

**6. An outcome carries reason codes and bounded parameters, never a message.**
`reasons` is a tuple drawn from a closed vocabulary; `params` is a flat string map. An
interpolated message is where field values leak, and this library is used on submissions
carrying personal and financial data. It also makes outcomes countable — "how many failed
for this reason" is a query rather than a grep.

Plural, not singular, because one event can fail several rules at once and the consumer
that motivated this already collects them as a tuple before flattening them with a
`", ".join(...)` for display. Storing the join would re-lose exactly the structure this
decision exists to keep.

**6a. An outcome is immutable; nothing is resolved on it.** An earlier draft gave the
record a `resolved_at`. That is wrong wherever the reason codes come from a consumer's own
observations, because those observations already carry a resolution lifecycle — the
motivating consumer's flags have `resolved_at`, `resolved_by`, `assigned_to` and an
auto-resolve sweep. A second place to mark the same fact fixed is a second answer that
will eventually disagree with the first. So outcomes are append-only and attempt-numbered,
and "is this still failing?" is the latest outcome for the key, not a column. `unresolved`
is therefore a derived query, not stored state.

**7. Sequencing is recorded, not enforced.** An outcome carries a `sequence_key`.
Rakaia does not yet refuse an event whose sequence has an unresolved failure; the field
exists so that adding it later does not require re-deriving groupings a consumer has
already decided. Named here because recording a key nothing reads is otherwise the sort
of thing a later reader deletes as dead weight.

## Observations, refusals, and the record that one happened

Consumers already separate two of these three carefully, and the motivating one says so in
its own vocabulary file: *"a gate is a **refusal**; a flag is an *observation* — do not use
the words interchangeably."* An outcome is the third, and it is neither.

| | What it is | Who owns it | Lifecycle |
|---|---|---|---|
| **Observation** | A rule noticed something about the data | The consumer's rules layer | Raised, assigned, resolved |
| **Refusal** | The pipeline declined to accept a row | The consumer's policy | Decided per save |
| **Outcome** | The record that a refusal (or a failure) happened | This decision | **Immutable** |

One refusal produces several observations and exactly one outcome. The outcome's `reasons`
are a *snapshot of the observations that were open at the moment it was refused* — in the
motivating consumer, literally read back from its unresolved flags — which is why the
outcome does not carry a resolution of its own (Decision 6a): resolving the observation is
what fixes the underlying problem, and the next attempt records the next outcome.

Two consequences worth stating before someone infers the opposite:

- **rakaia does not define the vocabulary.** `reasons` is opaque to it. Which codes exist,
  what they mean, and when they are resolved are the consumer's, exactly as
  `ConsumerCursor` holds an offset whose meaning is the store's.
- **An outcome is not a verification result.** The parity commands that also get called
  gates — the ones that refuse to pass unless a replay matches live rows — are a different
  thing again, and what this decision exposes — lag, and which events failed — is
  deliberately a *report* rather than an input to one. A consumer's own gate command warns about this directly, of its coverage counter:
  it "belongs in a coverage verdict and would be wrong as a write blocker", and "a future
  apply must derive its own predicate from the report rather than reuse this verdict."
  Outcomes make such a residual explainable — *this row is missing and here is the record
  saying why* — and must not become the thing that decides whether it passes.

## A worked example, and the thing it exposes

The consumer that motivated this decision gates its events per row. Traced through its
live path, one refused repeater on a progress form does this:

1. The document is split. Every row exists as a derived row, including the bad one.
2. Every row is checked — the gate deliberately does not short-circuit, so the whole
   failure set is reported in one pass rather than one row per attempt.
3. Policy for this form is *refuse the row, keep its siblings*. The refused row's event
   is dropped from the batch: **neither appended nor projected**, on the stated
   principle that the log must not carry a row the consumer declined to accept.
4. An outcome is recorded against the refused row; the clean rows record success.
5. The user sees a partial failure naming the offending row.

Under this ADR that is `stage="append"`, `status="refused"`, `sequence_key` = the row —
the third table row above, and the design handles it correctly. The siblings are
genuinely independent, so parking the row and nothing else is the right answer.

**Then a stage-2 reducer runs, and the containment stops.** It recomputes a field on a
*different* form by reading the latest progress row per project **out of the projection**.
The refused row was never written, so the reducer cannot see it, takes the previous
period's figure instead, and *writes* the resulting value. Not stale by omission —
actively written from an input with a hole in it, with nothing linking that write back to
the refusal.

That is not a defect this decision introduces; the consumer measured it when it chose the
policy, and chose it because refusing the row moves fewer of those derived values than
refusing the whole document would. What matters here is narrower and is a limit of *this*
design: **a sequence key protects ordering within a sequence, and a reducer reads across
sequences.** Nothing else catches it either: the refused event has no offset, so it is
absent from every view keyed on one.

So the record says "this row was refused" and stays silent on "that derived value is now
computed from an incomplete population". Recorded here as a known limit rather than
closed, because closing it is a decision about reducers, not about outcomes — see *What
would reopen this*.

## Alternatives considered

**Thread event identity through `Effect` and record in the executor.** The version that
first looks right, and it fails on its own terms: the outcome is written inside
`transaction.atomic`, so a failing batch discards the record of its own failure. Making
the record survive means a second connection or an autocommit escape hatch inside a
class whose whole contract is that a batch is atomic. Rejected before the ergonomics of
adding a field to `RowEffect` even matter.

**Put the model in `django_rakaia` and be done.** Fastest, and consistent with
`ConsumerCursor` as it stands. Rejected on the 0008 precedent: it lands a table on every
consumer, on a branch whose recent history is almost entirely about removing Django
coupling, and it would need undoing to reach the shape this ADR describes. `envelope.py`
(`:21-23`) states the actual test — things live in `django_rakaia` when they are
*intrinsically* Django-shaped, "the core package stays dependency-free" — and an outcome
is not.

**Record every applied event, not only the exceptions.** It answers the completeness
question directly, with no reliance on the cursor meaning what it says. Rejected on cost
and on precedent: it is
one durable write per event on the success path, in a codebase that has a test file
asserting the *set of SQL statements* an append issues
(`tests/test_django_rakaia/test_append_query_cost.py`) because #202 found three
duplicates. The consumer that motivated this work already writes a row per row per save
and has neither a retention policy nor an index supporting the query its two hot paths
run; that is the outcome to avoid reproducing, not to generalise.

**Ship re-drive in the same change.** The obvious pairing — a dead letter is for
re-driving — and deliberately deferred. Re-drive writes to the projection, so it needs
the rebuild and parity gates extended to cover it, and the consumer that asked for this
had precisely that feature cut in review after four attempts to guard it, each failing
differently, because one path served both a scratch rebuild and a live database. The
record is useful alone: it makes the manual repair path targeted instead of a sweep.

**Reuse `DriftLedger`.** It already accumulates problems and reports them on the result
object. Rejected because it is deliberately per-registration and memoised — it reports a
rule once, not an event ever — and making it per-event would remove the deduplication
that is the point of it.

## Consequences

### Positive

- "Did we lose anything, and which ones?" becomes a query, and it survives a restart.
- The consume loop lands as a first-class thing. Rakaia has documented at-least-once
  semantics and, until now, no code expressing them; every consumer wrote the loop.
- `stage` makes the difference between a lost fact and an unapplied one legible at the
  point someone is deciding what to do about it.

### Negative, and named rather than waved away

- **Nothing enforces that the loop is used.** A consumer keeping its hand-written
  `poll`/`commit` records nothing, and its stream looks identical to a clean one. This
  decision adds a supported path; it does not close the unsupported one.
- **Everything rests on the cursor meaning what it says.** A consumer that commits before
  applying — which `poll`'s docstring warns against and nothing prevents — silently
  converts "unapplied" into "succeeded", because success is the absence of a record. The
  alternative design, a row per applied event, does not have this weakness, and giving it
  up is the price of not writing on the hot path.
- **An outcome inherits #232 exactly.** `DjangoStreamStore` and `JsonlStreamStore` both
  issue `PLAIN`, so an outcome's `(stream_path, offset)` cannot say which store's offset
  it is any more than a cursor's can. Two backends at the same path produce outcomes that
  collide silently.
- **Sequencing is a field and a promise.** Decision 7 records a key nothing acts on. If
  the enforcement never lands, this is a column carrying a consumer's private grouping
  for no benefit rakaia delivers.
- **A sequence key does not reach a reducer.** The worked example above is the case: a
  stage-2 construct reads committed projections across sequences, so an event parked in
  one sequence silently changes a value derived in another, and no outcome says so.
  Nothing in this decision detects it, and nothing keyed on an offset can — an event that
  was never appended has no offset to be found at. This is the largest thing the design leaves
  open, and it is a property of reducers reading projections, not of the record.
- **The admin registration breaks a local pattern.** `ConsumerCursor`,
  `StreamOffsetWatermark` and `StreamProducer` have no admin; operational bookkeeping is
  not browsed. A dead-letter table is argued as the exception because looking at it *is*
  the feature, but it is a deviation, not a precedent being followed.

### Neutral

- A consumer that never fails never writes a row, so the table is empty and the cost is
  a migration.
- Nothing about existing replay behaviour changes. `replay()` still raises out; the loop
  is a new caller, not a change to what it calls.

## What would reopen this

- **#232 lands.** An outcome should name its issuing store at the same moment a cursor
  does, and by the same mechanism. Amend rather than duplicate.
- **Re-drive.** The deferred half. It needs a decision of its own about what a
  re-drive may write and which gates must pass first, and Decision 4's `stage` split is
  the part of this ADR it will lean on.
- **Sequencing enforcement.** Refusing an event whose sequence has an unresolved failure
  changes live behaviour and needs its own argument. If it is declined for good, Decision
  7's field should go with it.
- **An answer to the reducer gap.** Three shapes, none obviously right, and the choice
  between them is a decision about reducers rather than about outcomes:
  *(a)* extend parking to reducers, so one refuses to run over a population with an
  unresolved outcome — correct, changes live behaviour, needs the gates;
  *(b)* record a derived outcome against the affected row when a reducer's input has a
  hole — purely additive, but a second kind of outcome with different provenance;
  *(c)* leave the behaviour alone and make the affected population queryable.
  Whichever lands, `stage`/`status` from Decision 4 is what it will key off.
- **A fourth store.** The contract suite is where that is absorbed; if a store cannot
  implement `OutcomeStore` cheaply, the protocol is wrong rather than the store.
