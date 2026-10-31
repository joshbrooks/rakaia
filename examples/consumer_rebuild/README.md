# Consumer rebuild — what a reader does when its log changes

A small ledger projection with a consumer reading it, and the three things that
go wrong in production and are usually only proven in the test suite:

* the stream is deleted and recreated under the reader, so the log is no longer
  the one the saved position was about;
* the database drops the connection mid-apply;
* the process dies part-way through a pass.

Every claim is asserted, and a failed claim raises `CommandError`, so the command
is the gate rather than a story about one.

## Run

```sh
just consumer-rebuild-demo
```

or directly:

```sh
cd examples/consumer_rebuild
uv run python manage.py migrate
uv run python manage.py demo_consumer_rebuild
```

## What each check shows

| Step | What happens | What the demo asserts |
|---|---|---|
| **REPLACED** | The stream is deleted and recreated, which issues a new stamp, then two events are appended. | The next run reports `replaced`, applies zero rows, records no outcome, and leaves both its cursor and its saved stamp unchanged. `Consumed.needs_rebuild` is true, so an operator knows derived data is stale rather than merely behind. |
| **REBUILD** | Derived rows are cleared and the recreated log is replayed; the consumer's position is moved to the new head. | The rebuilt rows match the recreated log, and the next event applies exactly once from the restored position. Deleting the log is not a reason to lose the projection — it is a reason to rebuild it. |
| **TRANSIENT** | An apply raises Django's `OperationalError`. | The run reports `transient` and `halted`, records **no** outcome, leaves the cursor below the event, and the next run hands back the same offset. A database blink is not a bad event; recording it would let `on_error="skip"` lose it for good. |
| **CRASH** | The process raises `SystemExit` after two events of a five-event pass. | The cursor and that pass's writes roll back together, so the next run delivers all five events again — two of them a second time — and the rows converge. The apply writes absolute values, so at-least-once delivery plus an idempotent apply is not a doubled total. |

## The shapes

`ledger/handlers.py` holds the pure `event -> Upsert` handler. The consumer's
apply calls the same function and applies it with `DjangoExecutor`, because a
rebuild and a live pass must agree on what an event means — otherwise a rebuild
certifies a projection the consumer does not produce.

`ledger/models.py` stores an **absolute** value per key, not a delta. That is the
whole reason the crash check can assert identical rows: an event delivered twice
writes the same row twice.

`RAKAIA_STORE = "durable"` puts the log in the same SQLite database as the
cursor, so the stamps and the positions in the demo are the ones an operator
would actually be looking at.

## Where the behaviour lives

* `Consumed.needs_rebuild`, `Consumed.transient` and the `replaced` status —
  `rakaia.subscription`.
* The stamp saved beside the cursor, and the per-pass transaction that rolls the
  cursor and the pass's writes back together — `django_rakaia.consumer` and
  `django_rakaia.subscription`.
* `DjangoExecutor` — `django_rakaia.effect_executor`.
