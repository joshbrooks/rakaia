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
  one `SubmissionEvent` and reprojects `Submission` in a single
  `transaction.atomic()`. `Submission` resolves to the *latest* event, and
  rebuilding from scratch reproduces it — `Submission` is a pure function of the
  log (trivial because every event is a full snapshot, Decision #5).
* **[2] HISTORY == the log.** `/history` is the ordered `SubmissionEvent` rows,
  fanned into `SubmissionHistory` by the shipped `history_effects`. Marker
  (`label_marker`), actor and **url** (`envelope_actor` / envelope metadata) are
  recovered from the envelope — provenance rides `append()`, not a `post_save`
  signal (which would miss bulk writes; Decision #13).
* **[3] SELF-HEALING.** A *direct* write to the `Submission` projection is
  overwritten by the next reprojection. Durable state **is** the event log, not
  the row — so a bypassing write is ephemeral, not a durable unaudited change.
  This is why Decision #10's coverage guard is belt-and-suspenders in this
  topology rather than load-bearing.
* **[4] MODE B.** A context-less write (an import, no request) still logs a
  *full* event; only `actor`/`url` are null — graceful, and exactly what
  pghistory does on non-request writes.
* **[5] DURABILITY** (`--reproject-only`). A second process rebuilds `Submission`
  from the persisted `SubmissionEvent` log — state survives, because the log is a
  real durable table, not in-memory.

## Scope / caveats

* Runs on **sqlite**, so the Postgres coverage guard (Decision #10 — the
  immediate trigger + appended-set) is **out of scope**; this spike deliberately
  exercises the topology where that guard is *not* load-bearing.
* `reproject_all` does a full rebuild for clarity; real code would project
  incrementally via replay handlers.
* `version` is the global stream position (never renumbered, Decision #7), so a
  submission's history versions are its positions in the whole log, not a
  per-submission 1..N sequence.
