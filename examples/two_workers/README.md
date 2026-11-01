# Two workers under one consumer name — the cursor row lock, and resuming

A runnable answer to two questions that are otherwise only prose:

- What stops two **processes** running the same consumer from applying the same
  events twice?
- After a worker has caught up, does "read what changed" replay the log or read
  the delta?

It is a Django example with no server: a stream of bookings, a projection with
one row per applied event, and `django_consumer` keeping the reading position in
the database. The two workers are real OS processes (`manage.py worker_pass`),
not threads, so each has its own database connection — which is the only way the
row lock is contended at all.

## Run

```sh
just two-workers-demo       # SQLite: the lock half is skipped, with a reason
just two-workers-demo-pg    # Postgres: the lock half runs for real
```

Or directly:

```sh
cd examples/two_workers
uv run python manage.py migrate
uv run python manage.py demo_two_workers
```

Expected output on Postgres:

```
[1] TAKE TURNS — two processes, one consumer name:
    worker a: 200 applied, status fresh, cursor <monotonic offset>
    worker b: 0 applied, status caught_up, cursor <monotonic offset>
    the second worker was observed blocking while the first held the pass (pg_stat_activity: wait_event_type=Lock)
    → 200 events, 200 rows, each applied once; both workers finished and the cursor moved forward to the head ✓

[2] FORWARD ONLY — try to move the cursor back:
    ...
    → the rewind raised and wrote nothing: the watermark cannot go back over applied events ✓

[3] RESUME — append five events and poll from the cursor:
    log now holds 205 events; cursor at <monotonic offset>; poll status advanced
    read 5 events: ['bk-0200', 'bk-0201', 'bk-0202', 'bk-0203', 'bk-0204']
    → 5 events read from a 205-event log: a resume is a delta, not a replay ✓
```

The cursor is a durable, globally-monotonic offset — not the event count — so its
exact value depends on the store's history, not on this run's 200 events.

On SQLite, `[1]` prints `skipped: SQLite has no row locks` and runs one worker
instead: the lock is the only thing the backend lacks, and `[2]`/`[3]` are real
there.

## What each check proves, and how

**[1] TAKE TURNS.** The demo seeds 200 events and creates the cursor row *empty*,
then starts worker A. A's first pass is held open at a marker file, so it is
inside its transaction with the cursor row locked. Only then is worker B started.
Three independent facts are asserted:

- the database itself reports a backend waiting on a lock (`pg_stat_activity`
  with `wait_event_type = 'Lock'`) while A holds the pass — a measurement, not a
  stopwatch;
- B's poll status is `caught_up`, which is only possible if it loaded a cursor
  the first worker had already committed; without the lock it polls as `fresh`
  and applies everything;
- 200 events produced 200 rows, A applied all of them and B applied none.

The cursor row is created up front on purpose: left absent, the second worker
waits on the *insert*, which looks identical and would pass without
`select_for_update()` ever being taken.

**[2] FORWARD ONLY.** `commit_cursor` refuses an offset behind the saved one with
`CursorMovedBackwards`, and writes nothing. The row lock is not what protects
this — the forward-only commit is — so it runs on both backends.

**[3] RESUME.** Five events are appended and polled from the saved cursor: five
come back, out of a log of 205. That is the replay-cost claim stated as an event
count. Elapsed time would be the flaky version of the same assertion, and it says
less: a fast machine replays 205 events quickly too.

## Files

* **`workers/seed.py`** — the stream path, the consumer name, the backlog and the tail.
* **`workers/consume.py`** — the apply: one `AppliedEvent` row per message.
* **`workers/models.py`** — the projection; `offset` is unique, so a double apply
  raises instead of quietly double-counting.
* **`workers/management/commands/worker_pass.py`** — one worker's single pass, in
  its own process, reporting what it did as JSON.
* **`workers/management/commands/demo_two_workers.py`** — the three checks.

## Caveats

- The held pass is a demo handshake, not something a real worker does. It exists
  because the interesting moment — the second worker blocked at the lock — is
  otherwise a few milliseconds wide.
- A pass is one transaction over the whole backlog: a long first run holds the
  lock throughout. That is the trade `django_rakaia.consumer` documents, and this
  demo is where you can watch it.
- Postgres is selected by `RAKAIA_TEST_DB=postgres` plus the usual `PG*`
  variables, the same switch the test suite uses. `just two-workers-demo-pg`
  needs a server first: `just pg-up` locally, or the CI `test-postgres` job's
  service container.
