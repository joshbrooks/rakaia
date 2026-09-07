---
type: Concept
title: Consuming a stream & failure records
description: One loop that reads, applies, records what it could not apply, and commits the reading position.
tags: [concept, consuming, outcomes, cursors]
status: stable
generated: { by: claude-code/opus-5, at: 2026-09-08T00:00:00Z }
---

# Definition

A durable log already carries positions, so "give me what changed since I last
looked" is *remember the last offset, read after it*. `poll` is that read.
`consume` is the loop around it — poll, apply, record anything that could not be
applied, commit the position, in that order — and `Consumer` holds the five
things the loop needs so a consumer is built once rather than assembled at every
call.

The record is the point. A position says how far a consumer got, never whether it
got there cleanly, so without one an event that was skipped, refused or lost is
indistinguishable from one applied without incident: **absence of a record reads
as success**. An `Outcome` closes that gap. Only failures are recorded — the
position is the success record — so a clean run writes nothing at all.

Two orderings are load-bearing and neither is enforceable from inside the loop.
The record is written *outside* whatever transaction the apply used, because a
record written inside rolls back with the batch whose failure it exists to
record. And the position is committed *last, per message*, which is what makes
`on_error="halt"` mean anything: the watermark stays below the event that failed,
so the event is still pending. Redelivery is expected either way, so **an apply
must be idempotent**.

# Public API

Imported from `rakaia`:

* `poll(store, path, cursor) -> Poll` — the incremental read. `PollStatus` is
  `fresh` / `advanced` / `caught_up` / `rewound` / `absent`; `rewound` means the
  log shrank beneath the cursor and derived state must be reset first.
* `consume(...) -> Consumed` — the loop. `on_error` is `OnErrorPolicy`
  (`"skip"` for a live consumer, `"halt"` for a rebuild) and has **no default**,
  deliberately: the two modes have opposite invariants.
* `Consumer(store, path, name, cursors, outcomes)` with `run(apply, on_error=…)`
  — the loop as one object. `outcomes` is required, so the shape that records
  nothing cannot be built.
* `ConsumerCursorStore` protocol and `InMemoryConsumerCursorStore` — where the
  reading position is kept between runs. Distinct from `CursorStore`, which is
  the *event* store a subscriber polls.
* `Outcome` — one event's failure to apply, as an immutable value: `stage`
  (`append` / `project`) says how far the event got, `status` (`failed` /
  `refused` / `skipped`) says what happened, and together they name the recovery.
  `reasons` are codes, never an interpolated message; `params` is a flat string
  map.
* `OutcomeStore` protocol with three implementations: `InMemoryOutcomeStore`,
  `JsonlOutcomeStore`, and Django's `DjangoOutcomeStore`. `encode_outcome` /
  `decode_outcome` is the one translation all three share.
* `RakaiaError` with `code`, `REASON_CODES` (the closed, published set of ten),
  `UNHANDLED` and `EXCEPTION_TYPE_KEY` — the reason codes rakaia records for its
  own failures. A consumer's own exception is recorded as `unhandled` with its
  type in `params`, never its message.

Imported from `django_rakaia`:

* `django_consumer(store, path, name, using=…, subject_of=…, sequence_of=…)` → a
  `DjangoConsumer` with the durable cursor and outcome stores wired in.
  `subject_of` names what a record the loop writes is about — without it the loop
  can only name the event's position, which reads as a different kind of thing
  beside a record the consumer named itself. Its `run` raises
  `CallerTransactionOpen` when the caller already has a transaction open on the
  alias the records are written to — the one hazard the dependency-free core
  cannot see.
* `DjangoConsumerCursorStore`, `DjangoOutcomeStore`, `load_cursor`,
  `commit_cursor`.
* `manage.py prune_outcomes --older-than-days N` — retention. No default age:
  the right period differs per installation and a wrong guess deletes evidence.
* A read-only admin for the records, the one screen this library's operational
  bookkeeping gets.

# Demonstrated by

* [partisipa_intake](../examples/partisipa-intake.md) — the whole loop end to
  end: an event refused before the log, one that failed to apply, one skipped on
  purpose, `halt` versus `skip`, and everything read back after a restart.
* [protocol_streams](../examples/protocol-streams.md) — `poll` on its own, with
  no Django.

# Not yet covered by an example

* `JsonlOutcomeStore` and `InMemoryOutcomeStore` as an example's own choice — the
  intake example keeps both the position and the records in the database, because
  what it is demonstrating is what survives a restart.
* Reading a code off a record: catching `RakaiaError`, or branching on the code a
  record carries, which is what an operator's own tooling would do with them.

# Decisions

* [ADR 0007](../../docs/adr/0007-an-outcome-is-recorded-where-the-cursor-is-committed.md)
  — why the record is written where the position is committed, and why there is
  no query for "what is missing".
* [ADR 0002](../../docs/adr/0002-framework-vs-protocol-server-boundary.md) — how
  far this library goes into handling an event, and what stays out: retrying,
  re-driving a recorded failure, and holding events back behind a failed one.
