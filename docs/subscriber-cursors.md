# Subscriber cursors

A durable stream already assigns every event a **monotonic offset**, so "give me
everything that changed since I last looked" needs nothing more than *remember
the last offset, read after it*. `rakaia.subscription` packages that into a small
per-consumer cursor with **rewind detection** — the streams-native replacement
for a hand-rolled `last_change_id` sync endpoint (a per-consumer watermark plus a
"the log was rebuilt, resync" signal).

This is the read side of the log. Where [versioned handlers](versioned-handlers.md)
and [projections](projections-and-fan-out.md) rebuild *server* state from a
stream, a subscriber cursor lets an *external* consumer — a reporting job, a
search indexer, a browser syncing into IndexedDB — pull the delta incrementally
and resume exactly where it left off.

## The core: `poll`

`poll(store, path, cursor)` is store-agnostic and pure with respect to the store:
it reads `path` forward from `cursor` and reports what happened. It works over
any store that can `read` and expose its head offset — the in-memory
`StreamStore` and the Django `DjangoStreamStore` both qualify.

```python
from rakaia import poll

result = poll(store, "submissions", cursor=None)  # first poll: everything
for msg in result.messages:
    apply(msg)
cursor = result.cursor  # persist this watermark

# later — only the delta since `cursor`
result = poll(store, "submissions", cursor)
```

`result.status` is one of:

| status | meaning |
|---|---|
| `fresh` | first poll (no prior cursor) — every message returned |
| `advanced` | new messages since the cursor — the delta returned |
| `caught_up` | cursor is at the head — nothing new (`result.caught_up`) |
| `rewound` | cursor sorts **past** the head — the log was rebuilt beneath it. `poll` returns the re-read (`result.rewound`); `consume` applies none of it, so rebuild the derived state first |
| `replaced` | the stream was deleted and recreated, or expired and recreated, since the cursor was saved (needs `poll(..., generation=)`); nothing returned |
| `absent` | the stream does not exist |

## At-least-once: poll, apply, *then* commit

`poll` never advances anything itself — it returns the *new* watermark in
`result.cursor`, and you persist it only **after** the messages are applied. A
crash between applying and committing re-delivers the batch rather than skipping
it, so downstream apply logic should be idempotent (which, for a projection, it
already is).

## The loop: `consume`

Writing that order by hand is where it goes wrong, so there is one written
version of it. `consume` polls, applies each message, records any outcome, and
commits the cursor — per message, in that order:

```python
from rakaia.subscription import consume

result = consume(
    store,
    "submissions",
    apply,  # called once per message
    consumer="reporting",
    on_error="skip",  # "halt" when rebuilding — no default
    cursor=load_cursor("reporting", "submissions"),
    commit=lambda offset: commit_cursor("reporting", "submissions", offset),
    outcomes=outcome_store,
)
```

Three things about it are deliberate, and
[ADR 0007](adr/0007-an-outcome-is-recorded-where-the-cursor-is-committed.md) has
the reasoning:

- **The outcome is written outside the apply's transaction.** A Django executor
  wraps its batch in `transaction.atomic`; a record written inside rolls back
  with the batch whose failure it exists to record. So do not call `consume`
  from inside a transaction of your own either — `django_consumer` refuses to
  start inside one rather than leaving that as advice. The usual way to meet
  that refusal without having written a transaction yourself is
  `ATOMIC_REQUESTS = True`, which wraps every view in one: run the consumer
  outside the request, or on a database alias that setting does not cover.
- **The cursor is committed last, per message.** That is what makes `halt` mean
  anything: the watermark stops *below* the event that failed, so the event is
  still pending and is delivered again.
- **`on_error` has no default.** `"skip"` for a live consumer — one poisoned
  event must not stall a stream. `"halt"` for a rebuild — a rebuild that skips
  an event has produced a projection that is not derived from every event, and
  reported success anyway.

An `apply` may also *return* outcomes instead of raising, for a fact it decided
rather than an exception it hit. Returning one is not a failure: the message
still applied and the cursor still advances.

## Holding it as one thing: `Consumer`

The call above names `"reporting"` and `"submissions"` three times each, and
`outcomes=` can be left off without anything complaining. `Consumer` holds the
wiring instead: the name and the path are said once, and the outcome store is a
required argument, so a consumer that records nothing cannot be built.

```python
from django_rakaia import django_consumer

consumer = django_consumer(store, "submissions", "reporting")
result = consumer.run(apply, on_error="skip")
```

`django_consumer` also takes `subject_of` and `sequence_of` — how to name what a
record is about, and what it is ordered within — which is worth passing whenever
your consumer records outcomes of its own: without them the records the loop
writes name the event's position while yours name a row, and both land in one
column on one screen.

`django_consumer` fills in the durable cursor and outcome stores, and refuses to
run inside a transaction you opened — the third bullet above, enforced rather
than described. Outside Django, build a `rakaia.Consumer` directly and pass your
own `ConsumerCursorStore` and `OutcomeStore`; `InMemoryConsumerCursorStore` and
`InMemoryOutcomeStore` are the reference pair.

## The reason codes rakaia records for itself

A record's `reasons` are codes rather than a sentence, so "how many failed for
this reason" is something you count rather than grep. The codes on an outcome
*your* consumer records are yours — rakaia never looks at them. These ten are the
ones rakaia writes when the failure is its own, and they are a closed, published
set: adding one is a public API change, and renaming one breaks whatever an
operator has been counting.

| code | what failed |
| --- | --- |
| `handler_gap` | no handler version covers the event's position |
| `upcaster_chain` | the chain of upcasters could not carry the event to the version asked for — a missing link, two that both match, or an event already above it |
| `effect_collision` | two effects in one batch write the same field of the same row |
| `unresolved_ref` | an effect pointed at a row no earlier effect in its batch produced, or at a field that row does not have |
| `duplicate_produces` | two effects in one batch claimed the same name |
| `handler_drift` | a handler's source changed since it was registered |
| `missing_reader` | a staged replay was run without the reader its later stages need |
| `undecodable_event` | the event's payload is not readable as JSON |
| `merge_key` | a merged stream's events lack, or disagree about, the ordering key |
| `multiple_rows_matched` | an upsert's lookup matched more than one row, on the in-memory executor |
| `unhandled` | anything else — your `apply` raised, and the exception's type is in `params` |

The last one is deliberately the only place a Python type name appears, and it
appears as a parameter rather than as the code, so a rename inside rakaia can
never rewrite what an operator has been reading.

## Rewind detection

If the stored cursor sorts *after* the current head, the log shrank beneath it,
so the consumer resets and re-reads from the start. Offsets are compared
**numerically** (parsing the `_`-joined integer parts) as a defensive fallback.
Both stores emit zero-padded, lexicographically-sortable offsets per the
protocol ([#34](https://github.com/joshbrooks/rakaia/issues/34)), so plain string
order already matches; the numeric parse just tolerates any stray non-padded
offset.

Store offsets are **globally monotonic** ([#34](https://github.com/joshbrooks/rakaia/issues/34)):
a stream recreated at a path issues offsets strictly greater than any it issued
before (the in-memory store bumps the offset's `read_seq` generation; the
EventStoreDB `$all` / Kinesis model). So a normal delete+recreate can never
collide with a stale cursor — the recreated content sorts *past* it and is
delivered as an ordinary `advanced`. Offsets alone therefore cannot say "this is a
different stream", so each stream also carries a stamp (`get_generation`) that
changes when it is recreated, and the consumer saves it beside its cursor; on a
mismatch `consume` stops with status `replaced`, applying nothing and leaving the
cursor where it was ([#350](https://github.com/joshbrooks/rakaia/issues/350)). A
`rewound` pass stops the same way ([#343](https://github.com/joshbrooks/rakaia/issues/343)):
rebuild the derived state, then start the consumer again. `rewound` is therefore
a **defensive** status: it fires only for a genuinely truncated log, or a cursor
carried over from a different stream, where the head really does sort before the
cursor.

## Durable cursors (Django)

`django_rakaia.subscription` persists the watermark in a `ConsumerCursor` row
(`(consumer_id, stream_path) → offset`) so a consumer survives restarts:

```python
from django_rakaia import DjangoStreamStore
from django_rakaia import poll_consumer, commit_cursor

store = DjangoStreamStore()
result = poll_consumer(store, consumer_id="reporting", stream_path="submissions")
if result.status in ("rewound", "replaced"):
    # The stream is not the one this consumer read. Committing here would adopt
    # the new stream over the old cursor and hide that; rebuild instead.
    raise RuntimeError(f"submissions was {result.status}: rebuild 'reporting'")
for msg in result.messages:
    apply(msg)
commit_cursor(
    "reporting", "submissions", result.cursor, generation=result.generation
)  # after applying
```

`poll_consumer` loads the stored cursor and calls `poll`; `commit_cursor` upserts
the new watermark. Splitting them preserves the at-least-once guarantee above.

## Two workers, one consumer

`django_consumer` runs each pass in one transaction that holds a `select_for_update`
lock on the consumer's cursor row. A second worker started under the same consumer
name waits for the first to finish and then reads the position the first saved, so
no event is applied twice. This holds with `DjangoConsumerCursorStore`; a custom
cursor store gets no lock, and calling `poll_consumer` and `commit_cursor` yourself
gets only the forward-only check below. Each `apply` runs in its own savepoint on the
cursor store's database, so one that raises undoes only its own writes there, not on
another alias.

A pass is one transaction over the whole backlog: there is no per-pass limit
([#297](https://github.com/joshbrooks/rakaia/issues/297)). A big first run or catch-up
holds the lock from start to finish, and if the worker dies or the connection is lost
before the pass commits, everything that pass did is lost and the next run does it
again (before, each event committed as it went). A lost connection raises out of `run`
rather than returning a `transient` result. The lock is real on Postgres only; on
SQLite the pass's first write takes the whole database's write lock, so every other
writer waits until the pass ends.

`commit_cursor` is forward-only as well: an offset behind the saved one raises
`CursorMovedBackwards` and writes nothing, so a slow worker that gets past the lock
cannot move the position back. To start a consumer over after a rebuild, delete its
`ConsumerCursor` row.

## Why this matters for the Partisipa migration

Partisipa's `sync_identity/watermarks.py` + `list_options(last_change_id)`
hand-roll exactly this: monotonic change-ids, per-consumer cursors, and
DB-rewind detection. Once `Submission` is a rakaia stream, that machinery *is* a
subscriber cursor — the stream's offsets are the change-ids, `ConsumerCursor` is
the per-consumer watermark, and `rewound` is the rewind signal the frontend needs
to trigger a full IndexedDB resync. This is quick-win **#4** of the
[streams-migration issue (#11)](https://github.com/joshbrooks/rakaia/issues/11):
the rakaia primitive is shipped here; wiring the client sync protocol onto it is
the Partisipa-side follow-on.
