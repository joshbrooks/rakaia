# One pipeline for live processing and for a rebuild: what the literature says

**Status:** research notes, 2026-11-12. Not a decision. No ADR is implied by this
file, and nothing here changes source. Like the other notes in `docs/research/`
it is deliberately not in the `zensical.toml` nav.

**Why now.** The first production consumer, partisipa-import, has proposed
(its ADR-0056, *A save and a rebuild run one list of steps*) that saving a form
and rebuilding every table from the log run one ordered list of steps, where a
step that runs on only one of the two must say why. Two open issues here ask
rakaia for the library half of the same idea: #374 (a consumer hands rakaia the
cross-stream rebuild order) and #375 (a handler declares the tables it reads and
writes). This note collects what is published on the question so those three
pieces of work rest on the same reading.

**Cross-repo state, measured 2026-11-12.** ADR-0056 and its code are an open,
unmerged pull request, catalpainternational/partisipa-import #3443, from branch
`feat/pipeline-steps` into `develop` (`gh pr view`). It was read before it was
pushed, from that branch's worktree; the history has been rebuilt since, so
treat every statement about it below as a description of a proposal and re-read
the pull request before relying on one. #374 and #375 were open
(`gh issue list`).

---

## The six practices

Each is stated plainly, then sourced. Quotes are verbatim from pages fetched on
the date above.

### 1. Run the same code live and when reprocessing

Two implementations of one transformation drift apart, and the cost of keeping
them aligned is the argument against having two.

- Kreps, *Questioning the Lambda Architecture* (2014): "You implement your
  transformation logic twice, once in the batch system and once in the stream
  processing system", and "Keeping code written in two different systems
  perfectly in sync was really, really hard." His alternative reprocesses with
  "just an improved version of the same code", reading the retained log from the
  start into a new table.
- Fowler, *Event Sourcing* (2005): "The general principle here is that the
  domain logic should never care about the context of the running of the
  events."
- Marten: "Both inline and asynchronous projections can be rebuilt with the
  async daemon" — the rebuild is the same projection run from position zero.
- Dudycz, *Projections and read models* (2023): a projection is predictable
  because "the same logic will generate the same result for the same events",
  and "we should not have multiple projections updating the same data."

### 2. What does not run on a replay is declared on the handler, and the reason is an external effect

Every framework surveyed lets a handler opt out of replay, and every one of
them motivates it with something that leaves the system: an email, a message, a
payment.

- Axon: `@DisallowReplay` "serves to define whether a processor should not
  invoke the class or method when a replay is in transit"; the documented sample
  is a handler that "performs a 'side effect', like sending an e-mail or a sms."
- Marten: `RaiseSideEffects()` "is only called during continuous asynchronous
  projection execution" and "will not be called during projection rebuilds."
- Fowler: "you'll need to wrap any external systems with a Gateway", and "it's
  usually sufficient for the gateways to be able to be disabled during the
  replay processing." Note the mechanism differs: a runtime flag the gateway
  checks, with nothing declared per handler.

The line these sources draw is **state versus external effect**, not one kind of
handler versus another. A step that writes rows the system later reads is state,
and a rebuild that leaves it out does not reproduce those rows. None of the
sources requires the opt-out to carry a reason.

### 3. Order comes from declared inputs and outputs, not from a hand-written sequence

- dbt: `ref()` "creates dependencies between the referenced node and the current
  model", and dbt uses "these references between models to automatically build
  the dependency graph." Its own caveat is the useful part: a reference inside a
  branch not taken at parse time is missed, and has to be declared by hand.
- Dagster: an asset declares "a set of upstream asset keys, which refer to
  assets that the contents of the asset definition are derived from."

Nothing in the event-sourcing sources addresses order *between* projections
during a rebuild. Marten rebuilds them in parallel; Axon orders only within one
processor. The declared-dependency idea comes from data-pipeline tools, and
applying it to a rebuild is a transfer, not an established practice.

### 4. Prefer a structure that cannot hold the bad state to a check that detects it

- King, *Parse, don't validate* (2019): "Use a data structure that makes illegal
  states unrepresentable"; "validation-based approaches make it extremely
  difficult or impossible to determine if everything was actually validated up
  front." She adds that in a dynamic language keeping the two in sync is "a
  little tricky without extensive test coverage", and that these are "ideals to
  strive for, not strict requirements to meet."
- Minsky, *Effective ML Revisited* (2011): the heading "Make illegal states
  unrepresentable", shown by replacing a record of optional fields with a
  variant.
- Thoughtworks, *Fitness function-driven development*: where structure cannot
  carry the rule, a test that measures "how close an architecture is to
  achieving an architectural aim" does, as an automated gatekeeper.

### 5. A rebuild is checked by comparing what it produced with what is live

- Dudycz: rebuild by truncating and reapplying, or blue-green into a new table
  and switching readers over.
- Kreps: write the reprocessed output beside the old and keep the old to revert.

A comparison covers only the tables it compares. Anything a rebuild produces, or
declines to produce, outside that set has no automated check.

### 6. A decision record states one decision, its alternatives, its costs and how it will be confirmed

- Nygard (2011): "One ADR describes one significant decision"; "All consequences
  should be listed here, not just the 'positive' ones"; a reversed decision is
  kept and "mark[ed] as superseded."
- MADR 4.0.0: *Considered Options* is not optional; *Confirmation* is "how the
  implementation of/compliance with the ADR can/will be confirmed."
- Fowler, *Architecture Decision Record* (2026): list "all the serious
  alternatives that were considered, together with their pros and cons"; "Once
  an ADR is accepted, it should never be reopened or changed - instead it should
  be superseded"; put "the most important material at the start."

## What this means for rakaia

- **Practice 1 is what `replay` already is** for a single stream: the handlers
  registered for live folding are the ones a rebuild runs. The gap #374
  describes is above that: the order *across* streams lives in consumer code,
  twice.
- **Practice 2 has no rakaia equivalent today.** A handler cannot say it does
  not run on a rebuild, and ADR 0003 makes the question mostly moot for
  handlers, since a hermetic handler has no external effect to suppress. The
  consumer's one-route steps are not handlers; they are work done around the
  replay. If #374 gives rakaia a plan object, a step in it needs somewhere to
  carry "not on a rebuild, because", and the consumer's trial suggests three
  kinds of reason worth telling apart: not needed on this route, done by other
  code on this route, and deliberately not reproduced. Only the third is the
  case the literature covers; the second is practice 1 being broken on purpose
  and is the one worth being able to list.
- **Practice 3 is #375.** Declared reads and writes are what would let an order
  be checked, or derived, where today it is argued by reading the handlers.
  dbt's caveat applies directly: a handler that picks its table at runtime is
  the untaken branch, and the fingerprint walker already reports those seven.
- **Practice 4 argues for a plan that is constructed, not inspected**: a
  registration that refuses an incomplete step, with tests for what Python's
  types cannot hold.
- **Practice 5 is `rebuild_and_verify`**, and its limit is the same: it verifies
  the tables it is told about (see also #369).

## Sources

- Jay Kreps, *Questioning the Lambda Architecture*, O'Reilly Radar, 2014-07-02.
  <https://www.oreilly.com/radar/questioning-the-lambda-architecture/>
- Martin Fowler, *Event Sourcing*, 2005.
  <https://martinfowler.com/eaaDev/EventSourcing.html>
- Martin Fowler, *Architecture Decision Record*, 2026-03-24.
  <https://martinfowler.com/bliki/ArchitectureDecisionRecord.html>
- Axon Framework 4.11 reference, *Streaming event processor — Replaying events*.
  <https://docs.axoniq.io/axon-framework-reference/4.11/events/event-processors/streaming/>
  The page does not say whether a handler is replayed by default.
- Marten, *Side effects* and *Rebuilding projections*.
  <https://martendb.io/events/projections/side-effects.html>,
  <https://martendb.io/events/projections/rebuilding.html>
- Oskar Dudycz, *Guide to projections and read models in event-driven
  architecture*, 2023-01-20.
  <https://event-driven.io/en/projections_and_read_models_in_event_driven_architecture/>
- dbt, *About ref function*.
  <https://docs.getdbt.com/reference/dbt-jinja-functions/ref>
- Dagster, *Defining assets*.
  <https://dagster.io/docs/concepts/assets/software-defined-assets>
- Alexis King, *Parse, don't validate*, 2019-11-05.
  <https://lexi-lambda.github.io/blog/2019/11/05/parse-don-t-validate/>
- Yaron Minsky, *Effective ML Revisited*, 2011.
  <https://blog.janestreet.com/effective-ml-revisited/>
- Thoughtworks, *Fitness function-driven development*.
  <https://www.thoughtworks.com/insights/articles/fitness-function-driven-development>
- Michael Nygard, *Documenting Architecture Decisions*, 2011-11-15.
  <https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions>
- MADR 4.0.0. <https://adr.github.io/madr/>

**Not used.** *Build Systems à la Carte* (Mokhov, Mitchell, Peyton Jones) would
be the reference for static versus dynamic dependencies under practice 3; the
PDF did not load as text, so nothing here rests on it. Event naming, schema and
versioning (CloudEvents, Young's *Versioning in an Event Sourced System*, the
Decider pattern) were read and set aside: ADR-0056 does not decide them.
