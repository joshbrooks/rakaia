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
place; you only ever append. → *[protocol](protocol.md), [django integration](django-integration.md)*

### Event

One immutable record appended to a stream: a JSON payload describing something
that *happened* ("order placed", "activity updated"). Events are facts — they're
never changed after the fact. → *[versioned handlers](versioned-handlers.md)*

### Offset / seq

An event's position in its stream, assigned monotonically (0, 1, 2, …). The same
number does two jobs: as **seq** it selects which handler *version* applies to an
event; as **offset** it's the cursor you resume reads from. → *[versioned handlers](versioned-handlers.md), [protocol](protocol.md)*

### Durable store

Where the log physically lives. The default in-memory store is fast but
process-local — the log is lost on restart. The durable `DjangoStreamStore`
persists events in your database, so you can emit an event from a web request and
replay that stream later, in a different process. → *[adopting the durable store](django-integration.md#adopting-the-durable-store)*

## Deriving state

### Projection

A database table *derived* from the log rather than written to directly. You
don't `UPDATE` it by hand — you replay events and let handlers rebuild it.
Because it's derived, you can drop it and regenerate it at will. → *[versioned handlers](versioned-handlers.md), [projections & fan-out](projections-and-fan-out.md)*

### Handler

A **pure** function that turns one event into one or more [Effects](#effect). It
performs no database writes itself and has no side effects — which is exactly
what makes replay safe, repeatable, and testable. → *[versioned handlers](versioned-handlers.md)*

### Effect

A *description* of a write — "update-or-create this row", "delete these rows",
"send this email" — returned by a handler. Nothing actually happens until an
[executor](#executor) applies it. → *[versioned handlers](versioned-handlers.md), [dry-run & executors](dry-run-and-executors.md)*

### Executor

The component that *applies* effects. `DjangoExecutor` writes them to the
database; `CollectingExecutor` merely records them (a [dry run](dry-run-and-executors.md)).
Same effects, different destination. → *[dry-run & executors](dry-run-and-executors.md)*

### Replay

Reading a stream from the start (or over a range) and running every event through
its handlers to (re)build a projection. → *[versioned handlers](versioned-handlers.md)*

### Idempotent

Safe to run more than once with the same result. Replay is idempotent: running it
twice produces identical rows, because handlers upsert with `update_or_create`
instead of blindly inserting. → *[versioned handlers](versioned-handlers.md)*

## Evolving safely

### Versioned handler

A handler registered for a specific **range of sequence numbers**, so old events
run through the logic that was correct *when they happened*. Fixing a rule going
forward never rewrites the past. → *[versioned handlers](versioned-handlers.md)*

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
never leaves orphaned rows behind. Comes in three shapes: `reconcile_children`
(a flat list), `reconcile_tree` (unbounded nesting), and `reconcile_aggregate`
(grouped rollups). → *[projections & fan-out](projections-and-fan-out.md), [tree-reconcile](tree-reconcile.md)*

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

### event_match / match_field

How a handler declares *which* events it wants. `event_match` matches the stream
path (globs like `room:*:messages` allowed); `match_field` routes by a field
*inside* the event (e.g. `form_type`), so several form types can share one stream.
→ *[versioned handlers](versioned-handlers.md)*

### External effect

An effect that isn't a database write — sending an email, calling an API.
Replay **skips** external effects by default, so re-deriving state never re-sends
last year's receipts. → *[versioned handlers](versioned-handlers.md), [orders example](../examples/orders/)*
