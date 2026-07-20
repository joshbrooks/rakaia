# submission_stream — the converged-design spike (RFC #22, Decision #13)

A minimal, runnable proof of the **arrow-flip**: instead of pghistory's

```
Submission  ──trigger──▶  SubmissionEvent      (event log is a derived shadow)
```

make the event log the source of truth and the row the projection:

```
SubmissionEvent  ──project──▶  Submission       (Submission is the LATEST version)
```

Both tables already exist in a real adopter (formkit-ninja has `Submission` and
pghistory's `SubmissionEvent`); this reverses which one is authoritative rather
than adding a store. Here the event log *is* rakaia's durable
`StreamEvent`/`StreamEntry` (`DjangoStreamStore`), and `Submission` /
`SubmissionHistory` are projections rebuilt from it.

## Run

```sh
cd examples/formkit_submissions
uv run --extra django python manage.py migrate
uv run --extra django python manage.py demo_submission_stream
# then, in a fresh process, prove the log is durable:
uv run --extra django python manage.py demo_submission_stream --reproject-only
```

## What each assertion proves

* **[1] APPEND→PROJECT + REPLAY.** A "save" is `record_submission`, which appends
  one `SubmissionEvent` and reprojects `Submission` (via the shipped
  `rakaia.project_latest`) in a single `transaction.atomic()`. `Submission`
  resolves to the *latest* event, and rebuilding from scratch reproduces it —
  `Submission` is a pure function of the log (trivial because every event is a
  full snapshot, Decision #5).
* **[2] HISTORY == the log.** `/history` is the ordered `SubmissionEvent` rows,
  materialised into `SubmissionHistory` by the shipped
  `django_rakaia.materialize_history`. Marker (`label_marker`), actor and **url**
  (`envelope_actor` / envelope metadata) are recovered from the envelope —
  provenance rides `append()`, not a `post_save` signal (which would miss bulk
  writes; Decision #13).
* **[3] SELF-HEALING.** A *direct* write to the `Submission` projection is
  overwritten by the next reprojection. Durable state **is** the event log, not
  the row — so a bypassing write is ephemeral, not a durable unaudited change.
  This is why Decision #10's coverage guard is belt-and-suspenders in this
  topology rather than load-bearing.
* **[4] MODE B.** A context-less write (an import, no request) still logs a
  *full* event; only `actor`/`url` are null — graceful, and exactly what
  pghistory does on non-request writes.
* **[5] TOMBSTONE.** A `delete` event (Decision #2) removes the submission's
  projection row, but the log keeps the full `create`/`verify`/`delete` trail
  (`+`/`~`/`-`) — you can still audit a deleted submission.
* **[6] DURABILITY** (`--reproject-only`). A second process rebuilds `Submission`
  from the persisted `SubmissionEvent` log — surviving rows and tombstones alike
  reconstruct, because the log is a real durable table, not in-memory.

## Tests

`uv run --extra django python manage.py test submission_stream` — seven tests
covering latest-wins, **append+project atomicity** (Decision #11 rollback),
tombstone projection, reproject/history idempotency, and mode-B/provenance. The
Postgres coverage guard (Decision #10) is out of scope on sqlite and is tested
with the guard itself.

## Scope / caveats

* Runs on **sqlite**, so the Postgres coverage guard (Decision #10 — the
  immediate trigger + appended-set) is **out of scope**; this spike deliberately
  exercises the topology where that guard is *not* load-bearing.
* The read side is thin adapters over core: `reproject_all` calls
  `rakaia.project_latest` and `materialize_history` calls
  `django_rakaia.materialize_history` — the example carries no hand-rolled fold.
  For clarity it reads the whole stream each time; `project_latest` also accepts
  an incremental tail read (`store.read(path, offset=…)`) for real use.
* `version` is the global stream position (never renumbered, Decision #7), so a
  submission's history versions are its positions in the whole log, not a
  per-submission 1..N sequence.
