# ADR 0008 — Tags and a correlation id belong to the envelope, and are stored as columns

- **Status:** Accepted (implemented on branch `event-tags`, for 0.8.0)
- **Date:** 2026-10-02
- **Deciders:** rakaia maintainers, with partisipa-import as the first consumer
- **Related:** [ADR 0005](./0005-stream-positions-stay-a-counted-offset.md) (an event
  is still found by its offset; this adds no new identifier);
  [ADR 0006](./0006-changing-backends-is-a-copy.md) (three stores behind one seam —
  why the fields have to exist on all of them);
  `src/rakaia/types.py`, `src/rakaia/context.py`, `src/django_rakaia/models.py`,
  `src/django_rakaia/django_store.py`.

## Context

A consumer records incidents, such as a lost form or a recovery from a backup, as
events in the ordinary log, and then has to find them again: every event about one
incident, or every event of one kind across all streams. The envelope had a label,
an open metadata dict, and a logical time. A consumer could put tags and an incident
number into `metadata`, but then finding them means a JSON query, which SQLite and
Postgres spell differently, and which neither can index without a migration the
consumer would have to own.

The consumer also needs the id of the event row an append wrote, so its own table
can point at it. The store knew the id and did not return it, so the consumer had to
query for the newest entry in the stream, which is wrong as soon as two writers race.

## Decision

1. **Two envelope fields, on every store.** `AppendOptions.tags` (a set of short
   strings) and `AppendOptions.correlation_id` (one string, or none) are read back as
   `StreamMessage.tags` and `StreamMessage.correlation_id` from the in-memory, JSONL
   and Django stores alike, and carried by `migrate_stream`. Code written against the
   in-memory store therefore sees what the durable one stores. Tags come back sorted,
   with duplicates removed, so all three stores return the same tuple.
2. **The correlation id falls back to the ambient `provenance(correlation=...)`.** An
   incident handler sets it once for every append in its block, the way it already
   sets the actor. An explicit value wins.
3. **On Django they are a table and a column, not metadata keys.** Tags are rows in
   `StreamEventTag`, unique per event, and the correlation id is an indexed column on
   `StreamEvent`, so both are index lookups on either database.
   `StreamEvent.objects.tagged(...)` and `.correlated(...)` are the lookups.
4. **A bad value is refused before anything is written.** Building `AppendOptions`
   checks both fields, and every store checks them again before it writes anything,
   because a caller can set a field after building the options. An ambient correlation is checked when its `provenance`
   block opens, so a model save inside it never runs, rather than saving its row
   and then failing to write the event. Both limits match the column widths: 100
   and 128 characters.
5. **The append says which event it wrote.** `AppendResult.event_id` is the
   `StreamEvent` primary key from `DjangoStreamStore`, and `None` from the stores
   that have no event table.

"Correlation" already means something else in `rakaia.effects`: `Upsert(produces=...)`
correlates effects inside one replay batch, and is never stored. The two are
unrelated, and the docstrings of `Upsert.produces` and `StreamMessage.correlation_id`
say so.

## Consequences

- One migration, additive and not backfilled: existing events have no tags and no
  correlation id.
- Reading a page of messages from the Django store costs one more query, to fetch the
  tags for the whole page, rather than one per event.
- Deleting events costs one more statement per chunk, for their tag rows. A consumer
  table that protects an event (`on_delete=PROTECT`) still stops the delete with
  `ProtectedError`, and the delete rolls back.
- The JSONL store writes the two keys only when they are set, so files written by 0.7
  read back unchanged, and an untagged append writes exactly what 0.7 wrote.
- The tables stay Tier 2, like the other models. The fields on `AppendOptions`,
  `AppendResult` and `StreamMessage` are Tier 1.

## Not decided here

- Tags are not ambient. Nothing has needed a block of appends to share tags, and a
  set that merges across nested blocks is a rule worth waiting for a use to shape.
- `StreamEvent` is still not meant to be subclassed by consumers.
