# Stream coverage — a stream that fell behind its table

A stream written from a table can stop keeping up with it without anything
failing. Rows that arrive by a path that writes no event — a bulk import, a
backfill nobody ran, an `update()` in a script — are in the table and not in the
stream, and the first sign is usually a rebuild from the stream that comes up
short. `manage.py check_stream_coverage` compares the two and fails when they
disagree. This example opens that gap on purpose, runs the check, and repairs
it.

## What it shows

One table, `Submission`, with a name and an `updated_at` that Django sets on every
save. Each save writes one event to the `submissions` stream carrying the row's
id, and the settings tell the check about it:

```python
RAKAIA_COVERAGE_CHECKS = [
    {
        "model": "survey.Submission",
        "stream_path": "submissions",
        "subject_key": "submission",  # each event carries {"submission": <row id>}
        "changed_field": "updated_at",
    },
]
```

The demo then does four things and checks after each:

| Step | What happens | What the check says |
|---|---|---|
| 1 | 20 rows saved the normal way | ok: 20 rows, all covered |
| 2 | 5 more loaded with `bulk_create`, which writes no events | 5 missing, named by id; the command fails |
| 3 | 2 of the first rows changed with `update()`, which writes no events | 2 stale, named by id; still failing |
| 4 | The rows it named are saved again the normal way | ok: 25 rows, all covered |

When the check fails it exits with a non-zero status, which is all a nightly
timer needs to raise an alert. [Check a stream still matches its
table](../../docs/check-stream-coverage.md) shows how to schedule it.

## Run

```sh
just coverage-demo
```

Or directly:

```sh
cd examples/stream_coverage
uv run python manage.py migrate
uv run python manage.py demo_coverage
```

The demo empties its own database first, so every run starts from nothing and
prints the same ids. Every step is asserted: the walkthrough ends with `All
stream coverage checks passed.` or an error, and it fails if the check ever stops
finding the gap.

## Where things are

- `coverage_project/settings.py` — the durable store and the one coverage entry.
- `survey/models.py` — the table, and the `@stream_model` producer that writes
  one event per save.
- `survey/management/commands/demo_coverage.py` — the walkthrough.
