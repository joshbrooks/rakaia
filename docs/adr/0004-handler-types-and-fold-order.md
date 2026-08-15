# ADR 0004 — The content-routed staged handler is the extension point; fold order is a stage

- **Status:** Accepted
- **Date:** 2026-08-15
- **Deciders:** rakaia maintainers
- **Related:** [ADR 0002](./0002-framework-vs-protocol-server-boundary.md) (framework
  vs protocol-server boundary), [ADR 0003](./0003-handler-hermeticity.md) (handler
  hermeticity), [`docs/research/handler-types.md`](../research/handler-types.md)
  (the survey this decision rests on), [`glossary.md`](../glossary.md),
  [`projection-cookbook.md`](../projection-cookbook.md);
  `src/rakaia/registry.py`, `src/rakaia/replay.py`, `src/rakaia/effects.py`.
  Upstream consumer decisions this responds to: partisipa-import ADR-0005
  (an edge annotates, never materialises), ADR-0008 (no rakaia edge primitive
  yet), ADR-0016 (corrections are events), ADR-0018 (one fact, one derivation),
  ADR-0021 (a producer must not consult the projection).

## Context

rakaia names three framework-tier extension points:

| Concept | Shape | Selection |
|---|---|---|
| **handler** | `event -> Effect`, or `event, reader -> Effect` at stage > 0 | seq-bracketed version series; glob on the stream path **or** on `event[match_field]` |
| **reducer** | `reader -> Effect`, or `reader, touched -> Effect` | one current definition per name, last-write-wins, not seq-versioned |
| **upcaster** | `event -> event` | chained by `from_version` |

The first production consumer has since built five shapes on top of these that
rakaia does not name: **edge/relation binders** (bind a link between two existing
subjects, `LINK`/`UNLINK`, last-write-wins by append order), **thin attribute
folds** (a status stream carrying identity plus one field, folded over the content
stream), **correction folds** (a later stream whose events override an earlier
stream's projection), **derivation folds** (a value recomputed from another
aggregate, where a user-authored terminal value outranks a derived one regardless
of arrival order), and **producers** (the write side — deciding what to emit).

The question this ADR settles: is any of those a distinct handler *type* that
deserves first-class support, or are they compositions of what exists? ADR 0002
committed rakaia to bringing its framework-tier seams up to the protocol tier's
standard, so if a fourth registration shape is coming, this is the moment to know.

**The survey's finding is that four of the five are the same registration.** Not
"similar to" — the same three lines, differing only in the value of `event_match`:

```python
reg.register(name=…, event_match=…, fn=…,
             effective_from=0, match_field="relation", stage=1)
```

Everything that distinguishes an edge binder from a correction fold from a
derivation fold lives inside the function body: which projection it reads, which
column it writes, what rule decides. That is application logic, which is what a
handler body is for. **The variety is domain variety, not mechanism variety.**

Two further facts shaped the decision:

- **Correction precedence is not a temporal fact.** It is tempting to express
  "a correction beats the base value" as `merge_replay(order_key=ENVELOPE_TS)`.
  That is wrong: a later re-import carries a later `event_ts`, so the re-import
  would win and the correction be lost — the exact failure the consumer's ADR-0016
  exists to prevent, and the alternative it explicitly rejected. Correction
  precedence is *typed authority*: this class of event outranks that class,
  forever, independent of when either happened.
- **Prior art puts the line in the same place.** Axon supports ordering handlers
  *within* one event processor (`@Order`) and states it is "not possible to order
  event handlers belonging to different Event Processors", warning that ordering
  at all introduces coupling. The consumer's four sequential `replay()` calls are
  four processors with no ordering contract — the arrangement Axon says has none
  to offer.

## Decision

**1. The content-routed staged handler is the framework-tier extension point.**
No new registration kind is added for edges, corrections, derivations or thin
attribute folds. A consumer models all four as `register_handler(...,
match_field=…, stage=…)` with the domain rule in the function body.

**2. Fold order between stream families is a `stage`, and is part of a
projection's definition — not an implementation detail of a rebuild command.**
rakaia already expresses this two ways, and the choice between them is not
stylistic:

- **`stage=` within one replay (or one `merge_replay`)** — for precedence that is
  **typed**: correction-beats-base, derived-beats-nothing. Declared at
  registration, visible to `stages()`, and checkable.
- **`merge_replay(order_key=…)`** — for precedence that is **temporal**: streams
  that genuinely interleave and where "later wins" is the rule.

Correction precedence is the first kind and **must not** be modelled as the
second. This goes in `glossary.md` as a named term.

**3. No cross-replay fold-ordering API.** Running a list of `(stream, registry)`
pairs in order is a `for` loop; wrapping it buys a name and no checking. The
version with real value — one composed registry where family precedence *is* the
stage — needs no new API, only `merge_replay` and a documentation page.

**4. No framework-tier producer abstraction.** The write side stays outside the
library. The rule that motivates one (a producer must not consult the projection
to decide whether to emit) is recorded as a **documented contract** beside
ADR 0003, stated plainly as *not mechanically enforceable* — unlike its read-side
twin, which got `deny_database_access` only because "no statement may reach this
alias" is decidable by a query wrapper. Whether a read shaped a payload or decided
an emission is not.

Note the name is already spent: `rakaia.producer` is **protocol-tier fencing**
(epoch/seq validation) and says nothing about what to emit. Any future
framework-tier write-side concept must settle that collision first.

**5. The already-shipped write-side helpers are the whole of the write-side
answer for now.** `django_rakaia.envelope.append_event` / `fold_events` (#94)
cover the envelope-and-scratch-fold ritual the consumer asked to hand over.
Nothing further is added on that axis.

## Alternatives considered

| Candidate | Why it was proposed | Verdict |
|---|---|---|
| **Edge / graph primitive** (`register_edge`, edge event type) | The consumer built a `LINK`/`UNLINK` lifecycle with a node-existence guard | **Rejected.** One adopter; the load-bearing part is one registration plus a ~40-line body. The only generalisable piece — "annotate, never materialise" — is already `Effect(op="update")`, which the consumer is not using |
| **Correction-fold primitive** (`register_correction`, `overrides=`) | Corrections need to outrank the base value | **Rejected.** A correction fold *is* a stage-1 handler; what is special is precedence, and precedence is already spelled `stage=`. A kwarg would be a second, weaker spelling of an existing concept |
| **Discriminated-authority mechanism** (declarative non-last-write-wins) | A user-authored terminal status must beat a derived one | **Rejected.** The consumer's implementation is a four-line `if` in a handler body. A library concept here would be a configuration language for `if`, encoding domain knowledge (which values are user-authored terminals) in a general library |
| **"Thin event" / attribute-fold concept** | 62% of the consumer's re-saves re-carried a full payload with no content change | **Rejected.** Nothing is thin about the *fold* — it is a normal handler over a normal event. Thinness is a property of what the producer chose to emit; a read-side concept would name the wrong end of the pipe |
| **`Producer` class / emit-decision framework** | rakaia has a read-side hermeticity mechanism and no write-side twin | **Rejected.** See Decision 4 — plus the motivating rule currently has one spike-branch implementation, zero conforming producers on the branch reviewed, and one live producer that violates it |
| **Cross-replay ordering API** (`replay_families([...])`) | Fold order is presently an unwritten convention | **Rejected.** It is a `for` loop. The composed-registry route gives the same guarantee with real checking |
| **Registry-side "one fact, one derivation" conflict check** | The consumer's most expensive recorded defect: one column written by a stage-1 fold and a stage-2 reducer under *different* rules, each green in a different gate | **Deferred, not rejected** — see below |

The last one deserves its reason stated, because the failure it would have caught
was real and expensive. A handler's target column is only knowable from the
`Effect`s it returns — i.e. at replay time, not registration time — so the check
would be a runtime warning fired on a pattern rakaia explicitly supports
(multi-owner projection rows; `op="update"` exists to serve them). It is recorded
as an open question rather than a decision.

## Consequences

**Positive**

- The extension surface stays at three concepts. A consumer learning rakaia learns
  handler, reducer, upcaster — not seven near-synonyms distinguished by which
  domain rule lives in the body.
- Four future proposals now have a written answer with reasons, which is what
  stops an architecture review re-suggesting them. This is the same job
  partisipa-import ADR-0008 does downstream, and the same job the absence of such
  a record failed to do for the `Translatable` demo app.
- `stage=` gains a second, documented meaning it already had implicitly:
  cross-family precedence, not just intra-replay pass ordering.

**Negative / caveats**

- **The composed-registry route is untested by anyone.** `merge_replay` has *zero*
  call sites in the consumer; the recommendation that families-as-stages works at
  their scale is sound on paper and unproven in practice. It has one documented
  cost: merged `seq` is a position in the *merged* order, so a handler with a
  closed `effective_to` sized to one stream raises `HandlerGapError`. The
  consumer's folds all use `effective_to=None` and would survive — but a migration
  must state that constraint, not discover it.
- Declining the edge primitive means a second adopter will re-derive the pattern.
  That is the accepted cost of not designing against a single call site; the
  mitigation is a **recipe** in `projection-cookbook.md`, not an abstraction.
- The producer rule is documented and unenforced. A consumer can violate it and
  find out only when someone rebuilds — which is precisely how the consumer found
  it.

**Neutral**

- No behaviour change ships with this ADR. The follow-on work is documentation:
  a `fold order` glossary entry, the producer contract beside ADR 0003, and an
  edge recipe in the cookbook.

## What would reopen this

Stated so the triggers are checkable rather than a matter of judgement:

- **A second adopter needing edges.** This is partisipa-import ADR-0008's own
  extraction trigger and it has not fired. Two real call sites make the shape
  designable; one still makes it guesswork.
- **The composed-registry experiment failing.** If rebuilding a real multi-family
  projection as one merged, staged replay fails on `HandlerGapError`, memory, or
  the `seq`-semantics change, then the sequential-replay convention *is* the
  design and Decision 2 should document that instead.
- **A second consumer hitting the ADR-0018 defect** (two live derivations of one
  fact, disagreeing at the edges). That is a composition failure, not a
  handler-type failure, and would argue for declarable column ownership — new
  registration *metadata*, still not a new handler type.
- **Real evidence for the producer rule.** When the consumer's spike branch lands
  and its one violating producer is either reconciled or explicitly excepted,
  there will be more than one data point. Revisit Decision 4 then, not before.
  A recorded decision is not evidence of a working pattern.
