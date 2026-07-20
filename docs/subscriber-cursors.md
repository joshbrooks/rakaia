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

result = poll(store, "submissions", cursor=None)   # first poll: everything
for msg in result.messages:
    apply(msg)
cursor = result.cursor                              # persist this watermark

# later — only the delta since `cursor`
result = poll(store, "submissions", cursor)
```

`result.status` is one of:

| status | meaning |
|---|---|
| `fresh` | first poll (no prior cursor) — every message returned |
| `advanced` | new messages since the cursor — the delta returned |
| `caught_up` | cursor is at the head — nothing new (`result.caught_up`) |
| `rewound` | cursor sorts **past** the head — the log was rebuilt beneath it; re-read from the start and reset derived state (`result.rewound`) |
| `absent` | the stream does not exist |

## At-least-once: poll, apply, *then* commit

`poll` never advances anything itself — it returns the *new* watermark in
`result.cursor`, and you persist it only **after** the messages are applied. A
crash between applying and committing re-delivers the batch rather than skipping
it, so downstream apply logic should be idempotent (which, for a projection, it
already is).

## Rewind detection

If the stored cursor sorts *after* the current head, the log shrank beneath it —
a truncation, or a delete-and-recreate — so the consumer must reset and re-read
from the start. Offsets are compared **numerically** (parsing the `_`-joined
integer parts), so detection is correct even for the Django store's
non-zero-padded offsets, where `"10"` is later than `"2"` despite sorting before
it as a string.

**One blind spot:** a stream deleted and then re-grown *past* the old cursor
before the next poll reads as a normal `advanced`. Offsets alone cannot see that;
a stream-generation token would close it, and is a deliberate future extension.

## Durable cursors (Django)

`django_rakaia.subscription` persists the watermark in a `ConsumerCursor` row
(`(consumer_id, stream_path) → offset`) so a consumer survives restarts:

```python
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.subscription import poll_consumer, commit_cursor

store = DjangoStreamStore()
result = poll_consumer(store, consumer_id="reporting", stream_path="submissions")
for msg in result.messages:
    apply(msg)
commit_cursor("reporting", "submissions", result.cursor)   # after applying
```

`poll_consumer` loads the stored cursor and calls `poll`; `commit_cursor` upserts
the new watermark. Splitting them preserves the at-least-once guarantee above.

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
