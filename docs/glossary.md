---
icon: lucide/book-a
---

# Glossary

Rakaia borrows a handful of event-sourcing terms. Here's each one in plain
language, grouped so it reads as a mental model rather than an alphabet soup.
New to the whole idea? The [guided tour](whats-new.md) shows these working with a
demo for each.

The one-sentence version: **your database tables are rebuilt from an append-only
log of events by small, pure functions — so you can replay history and get
answers that were correct at the time, not just answers your current code would
give.**

## The log

### Stream

A named, append-only, ordered sequence of events — e.g. `orders` or
`room:5:messages`. It's the source of truth. Nothing is ever edited or deleted in
place; you only ever append.

_Avoid_: topic, feed, queue.

→ *[protocol](protocol.md), [django integration](django-integration.md)*

### Event

One immutable record appended to a stream: a JSON payload describing something
that *happened* ("order placed", "activity updated"). Events are facts — they're
never changed after the fact.

_Avoid_: message (that is a [StreamMessage](#streammessage)), row.

→ *[versioned handlers](versioned-handlers.md)*

### StreamMessage

The wire form of one log entry: the payload (the [event](#event)) plus its
[envelope](#envelope) and transport timestamp, as a store returns it from `read`
and hands it to a [consumer](#consumer). "Message" on its own usually means this;
the event is only the payload inside it.

_Avoid_: record, packet.

→ *[protocol](protocol.md), [event envelope](event-envelope.md)*

### Offset / seq

An event's position in its stream, assigned monotonically (0, 1, 2, …). The same
number does two jobs: as **seq** it selects which handler *version* applies to an
event; as **offset** it is the position a reader resumes after. An offset alone is
not a [cursor](#cursor) — a cursor also carries the stream's
[stamp](#stream-stamp-generation).

_Avoid_: position, index.

→ *[versioned handlers](versioned-handlers.md), [protocol](protocol.md)*

### Stream stamp (generation)

A value that changes when a stream is deleted (or expires) and is created again,
which the rising offsets do not show. A [consumer](#consumer) saves it beside its
offset, so it can tell "the same log, further on" from "a different log at the
same position". The prose calls it a stamp; the code calls it `generation`
(`get_generation`, `adopt_generation`).

_Avoid_: version (that is the handler series), id.

→ *[subscriber cursors](subscriber-cursors.md), [store streams in files](store-streams-in-files.md)*

### Durable store

Where the log physically lives. The default in-memory store is fast but
process-local — the log is lost on restart. The durable `DjangoStreamStore`
persists events in your database, so you can emit an event from a web request and
replay that stream later, in a different process. → *[adopting the durable store](django-integration.md#adopting-the-durable-store)*

### Envelope

The audit metadata a stream message carries *alongside* its payload: a **label**
(the change type — create/update/delete) and an open **metadata** dict (actor,
url, causation, …). The payload says *what the thing became*; the envelope says
*who/when/how*. The transport ignores it; the event-sourcing layer reads it. → *[event envelope](event-envelope.md)*

### Provenance

The "who and from where" of an append — typically the acting user and request
URL. `provenance()` sets it *ambiently* for a block of code, so every append
inside (e.g. a whole web request) is attributed without threading the actor
through every call. → *[event envelope](event-envelope.md#provenance-attaching-the-actor-ambiently)*

### No-op suppression

Recording an event only when something actually changed. `append_if_changed`
compares a new payload to the subject's *current* snapshot and appends only on a
difference — the write-side analogue of pghistory's "record on change" and of the
`skip_unchanged` executor.

→ *[event envelope](event-envelope.md#append_if_changed-suppress-no-op-appends)*

### Producer fencing

An appender that must not double-write on a retry names itself (`producer_id`)
and sends an epoch and a sequence with each append. A superseded writer, a
duplicate and a gap are then refused with `ProducerStaleEpoch`,
`ProducerDuplicate` and `ProducerSequenceGap` instead of landing twice or out of
order.

_Avoid_: dedupe, idempotency key (that names the event, not the writer).

→ *[producer fencing](producer-fencing.md), [protocol](protocol.md)*

## Deriving state

### Projection

A database table *derived* from the log rather than written to directly. You
don't `UPDATE` it by hand — you replay events and let handlers rebuild it.
Because it's derived, you can drop it and regenerate it at will.

_Avoid_: view, cache, denormalised copy.

→ *[versioned handlers](versioned-handlers.md), [projections & fan-out](projections-and-fan-out.md)*

### Subject

The thing a row is about — an order, a form submission, a room. A latest-state
projection keeps one row per subject; a history read-model keeps one row per
`(subject, version)`.

_Avoid_: entity, object, aggregate.

→ *[projections & fan-out](projections-and-fan-out.md)*

### Latest-state vs history read-model

Two projections you can derive from one log. A **latest-state** projection
*folds* the stream — one row per subject, overwritten by each event
(`project_latest`). A **history read-model** *multiplies* it — one immutable row
per event, keyed by `(subject, version)` (`materialize_history`) — the queryable
audit trail that replaces `pgh_event`. → *[history read-model](history-read-model.md)*

### Peak snapshot

The most complete snapshot in a subject's history — the one with the most fields.
Recovering it restores a subject that a legacy blank/truncating save corrupted
(newest wins on a tie). A legacy-recovery move over the audit rows, not an
everyday one. → *[history read-model](history-read-model.md#recovering-the-peak-snapshot)*

### Handler

A **pure** function that turns one event into one or more [Effects](#effect). It
performs no database writes itself and has no side effects — which is exactly
what makes replay safe, repeatable, and testable.

_Avoid_: listener, callback, processor, reducer (a reducer is a stage aggregate,
not a per-event handler).

→ *[versioned handlers](versioned-handlers.md)*

### Effect

A *description* of a database write, returned by a handler. Nothing actually
happens until an [executor](#executor) applies it. There are exactly four, one
per operation — [Upsert](#upsert), [Update](#update), [Delete](#delete) and
[Retire](#retire) — and `Effect` is the name for "any of those four". Each
carries only the fields its own operation uses, so a combination that makes no
sense (a `produces` on a delete, a `patch` on an upsert) cannot be written down
at all. A write that isn't to the database is an
[external effect](#external-effect), which is deliberately *not* an Effect.
→ *[versioned handlers](versioned-handlers.md), [dry-run & executors](dry-run-and-executors.md)*

### Upsert

Create the row matching `lookup`, or update it in place — the replay-safe write,
and by far the most common effect. Carries `defaults` (the field values) and,
optionally, `produces`: a batch-local name for the single row it materialises, so
a sibling effect can point a foreign key at it with `Ref(...)` before the
database has assigned a primary key. Only an upsert has `produces` — an
[Update](#update) may match many rows and the other two mint none.
→ *[versioned handlers](versioned-handlers.md)*

### Update

Update-if-exists: the rows matching `lookup` are updated in place and **never**
inserted, so it is a clean no-op when nothing matches. This is what a *secondary*
owner of a shared projection row emits — it writes its own columns without ever
claiming responsibility for the row's existence. → *[versioned handlers](versioned-handlers.md)*

### Delete

Hard-delete the rows matching `lookup`, minus anything named by its `spare`. The
two ways to spare rows are alternatives, so they are two shapes of one field
rather than two fields: [Exclude](#exclude-and-sparekeys) for a flat lookup and
[SpareKeys](#exclude-and-sparekeys) for composite natural keys.
→ *[projections & fan-out](projections-and-fan-out.md)*

### Retire

Soft-delete: the rows matching `lookup` (minus its `spare`) are UPDATEd with a
`patch` instead of being deleted — stamping e.g. `resolved_at` rather than
destroying an authored row. The patch value must be the *triggering event's*
timestamp, never `now()`, or replay stops being deterministic. May carry a
[Transition](#transition) to ask for notifications.
→ *[alerts projection](alerts-projection.md)*

### Exclude and SpareKeys

The two ways to spare rows from a [Delete](#delete) or [Retire](#retire).
`Exclude({"idx__in": [0, 1]})` is a single flat lookup — the positional
`reconcile_children` case. `SpareKeys([{"alert_type": "ff4", "field_key": "a"}])`
names composite natural keys — the `reconcile_by_key` case. They are alternatives
(applying both at once was never intended), which is why a delete carries one
`spare` field with two shapes instead of two fields and a rule forbidding the
combination. → *[projections & fan-out](projections-and-fan-out.md)*

### Transition

A [Retire](#retire)'s opt-in request for notifications: `kind` names the
[external effect](#external-effect) to emit, and `key_fields` names the columns
that identify each affected row. One notification is emitted per row the retire
*actually* flipped — a real machine resolution — not per row it scanned. The two
values are a pair (the executor's deterministic identity query needs both), so
they live in one object rather than as two fields that could be half-set.
→ *[alerts projection](alerts-projection.md)*

### Executor

The component that *applies* effects. `DjangoExecutor` writes them to the
database; `CollectingExecutor` merely records them (a [dry run](dry-run-and-executors.md)).
Same effects, different destination. → *[dry-run & executors](dry-run-and-executors.md)*

### Replay

Reading a stream from the start (or over a range) and running every event through
its handlers to (re)build a projection.

_Avoid_: rebuild (that is the response to a rewound or replaced log, below),
reprocess.

→ *[versioned handlers](versioned-handlers.md)*

### Rebuild

Replaying a projection from the start because the log underneath it was rewound
or replaced — the action `Consumed.needs_rebuild` asks for, and the one
`rebuild_and_verify` runs guarded by a diff. [Replay](#replay) is the act;
rebuild is why you do it.

_Avoid_: reindex (that is a search-index job).

→ *[subscriber cursors](subscriber-cursors.md), [django integration](django-integration.md)*

### Idempotent

Safe to run more than once with the same result. Replay is idempotent: running it
twice produces identical rows, because handlers emit an [Upsert](#upsert)
instead of blindly inserting.

→ *[versioned handlers](versioned-handlers.md)*

## Consuming the log

### Cursor

Where a [consumer](#consumer) has got to: the last offset it applied *and* the
[stream's stamp](#stream-stamp-generation). The offset alone says how far it has
read; the stamp says whether the log underneath is still the one it read.

_Avoid_: watermark (that is a database counter), position.

→ *[subscriber cursors](subscriber-cursors.md)*

### Consumer

A loop that reads one stream forward from its [cursor](#cursor) and applies each
event somewhere outside the [projections](#projection) — another system, an
index, a file. Unlike a [handler](#handler) it has side effects, so it must be
idempotent and record, per event, what it did.

_Avoid_: subscriber, listener, worker; and handler, which is the pure case.

→ *[subscriber cursors](subscriber-cursors.md), [django integration](django-integration.md)*

### Outcome

What a [consumer](#consumer) recorded for an event it could not simply apply: a
status (`failed`, `refused`, `skipped`), a reason code from the closed published
set, the attempt count and the error, kept in an outcome store. It is the
per-event record behind `Consumed.transient`.

_Avoid_: error (an outcome is the record, not the exception), log line.

→ *[subscriber cursors](subscriber-cursors.md#the-reason-codes-rakaia-records-for-itself), [ADR 0007](adr/0007-an-outcome-is-recorded-where-the-cursor-is-committed.md)*

## Evolving safely

### Versioned handler

A handler registered for a specific **range of sequence numbers**, so old events
run through the logic that was correct *when they happened*. Fixing a rule going
forward never rewrites the past. → *[versioned handlers](versioned-handlers.md)*

### Handler dependencies

A stage-0 handler is called `fn(event)` and a stage > 0 handler `fn(event, reader)`,
so anything else it needs is bound with `functools.partial` — not a closure, whose
drift hash covers only the wrapper. The usual case is a probe bound to the alias a
rebuild replays into.

→ *[versioned handlers](versioned-handlers.md#handler-dependencies), [ADR 0003](adr/0003-handler-hermeticity.md)*

### Registration site

The module that ran a registration decorator, recorded on every registration as
`registered_in`. `rehydrate()` re-imports these and lets the decorators run
again, so a registration comes back only if the module holding its *decorator
call* is imported. That is not the module holding the function: bind a
dependency with `functools.partial` and the wiring lives in your app's
`handlers.py` while the function lives in a shared module. The `dotted_path` on
the same record says where the *logic* is, and is what drift detection reports.

### Upcaster

A small pure function that upgrades an *old* event to the current shape (e.g.
renames `qty` → `quantity`) before any handler sees it. It lets handlers assume a
single schema even as producers change theirs. → *[versioned handlers](versioned-handlers.md)*

### Drift

When a "frozen" historical handler's source is edited *after* old events already
ran through it — so a replay would no longer reproduce the original result.
Rakaia detects this and can fail loudly (`--strict-drift`). → *[versioned handlers](versioned-handlers.md#drift-detection)*

## Collections — one event, many rows

### Fan-out

When a single event projects into *many* rows: a form's repeater answers, an
order's line items, a submission's activities. → *[projections & fan-out](projections-and-fan-out.md)*

### Reconcile

Materialising a collection so it exactly matches the event — upsert the current
children **and** delete any that are no longer present, so a shrinking collection
never leaves orphaned rows behind. Comes in several shapes: `reconcile_children`
(a flat list keyed by index), `reconcile_tree` (unbounded nesting),
`reconcile_aggregate` (grouped rollups), and `reconcile_by_key` (a composite
natural key, with `retire=` for soft-delete instead of hard delete). → *[projections & fan-out](projections-and-fan-out.md), [tree-reconcile](tree-reconcile.md), [alerts projection](alerts-projection.md)*

## Advanced replay

### Stage / staged replay

Handlers can declare a `stage=`. Replay runs stage 0 across the whole stream,
then stage 1, and so on — so a later stage can rely on projections earlier stages
already built. This is how one form resolves a reference another form produced,
even out of order. → *[staged replay](staged-replay.md)*

### Projection reader

A read-only view (`get` / `filter` / `query`) of the projections built by earlier
stages, handed to any stage > 0 handler so it can look up references without
writing. → *[staged replay](staged-replay.md)*

### Reducer

A per-stage **aggregate** step. Unlike a handler (called once per event), a
reducer runs *once* per its stage — after that stage's handlers commit — reading
the accumulated projections via the reader and returning idempotent Effects
(typically via `reconcile_aggregate`). Because it **recomputes** the rollup from
current rows every replay, re-running never double-counts. → *[staged replay](staged-replay.md#per-stage-aggregates-reducers)*

### event_match / match_field

How a handler declares *which* events it wants. `event_match` matches the stream
path (globs like `room:*:messages` allowed); `match_field` routes by a field
*inside* the event (e.g. `form_type`), so several form types can share one stream.
→ *[versioned handlers](versioned-handlers.md)*

### External effect

An `ExternalEffect`: something that isn't a database write — sending an email,
calling an API. Deliberately **not** an [Effect](#effect): it names no row, and
no [executor](#executor) ever receives one. Replay collects them and hands them
back in `ReplayResult.external`, leaving the caller to decide whether to deliver
them — so re-deriving state never re-sends last year's receipts, and an executor
never needs a branch for effects it must not apply.
→ *[versioned handlers](versioned-handlers.md), [orders example](../examples/orders/)*
