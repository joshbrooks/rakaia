# Progress-form intake — the consuming loop, end to end

The example for [issue #254](https://github.com/joshbrooks/rakaia/issues/254):
rakaia offers one loop that reads a stream, applies each event, records anything
it could not apply, and commits the reading position — and nothing under
`examples/` used it. This does, with the durable pair underneath it, so the
reading position and the failure records both survive the process ending.

It is Django rather than a script on purpose. The half that matters here is what
is still there after a restart, and a consumer that keeps its position in a
variable can only claim it.

## The shapes

Partisipa's progress form: one document per suku per reporting month, with a
repeating row per output. `Suku` and `ReportingPeriod` are reference data that
arrives by other means — an administrative import, an operator closing a month —
and `ProgressRow` is the only derived table, written solely by the loop.

## What it shows

Four things happen to an event here, and
[ADR 0007](../../docs/adr/0007-an-outcome-is-recorded-where-the-cursor-is-committed.md)
Decision 4 names the recovery for each:

| What happened | Record | Where the fact is | Recovery |
|---|---|---|---|
| A row reports 140 % | `append` / `refused` | Upstream, in the form | Fix the form and submit again |
| An event names an unregistered village | `project` / `failed` | Safe in the log | Load the village; the event is delivered again |
| An event lands in a closed month | `project` / `skipped` | Wherever it was | None wanted — declining it *was* the decision |
| Everything else | none | Applied | — |

The refused row's siblings are appended and get real offsets: rows of one form are
independent facts, and declining one is not declining the submission.

Under `on_error="halt"` the position stops **below** the event that failed, so it
is still pending and the next run delivers it again. Under `on_error="skip"` the
position advances past it and the record is how it is found later. The demo runs
both against the same kind of failure and prints the two positions.

**One thing the final table shows that is worth reading rather than tidying
away.** Two records name a row, two name a position in the log. A record this
consumer writes itself — the refusal, and the row that landed in a closed month —
can say what the row was called. The two the loop wrote when an apply raised
cannot: `django_consumer` does not yet let a caller say how to name a message, so
the loop falls back to the one name it always has. That gap is
[#272](https://github.com/joshbrooks/rakaia/issues/272); the example shows it as
it is rather than hiding it behind a subject the loop cannot really supply.

## Run

```sh
just intake-demo
```

Or directly:

```sh
cd examples/partisipa_intake
uv run python manage.py migrate
uv run python manage.py demo_intake
```

Every check is asserted — the walkthrough ends with `All consuming-loop checks
passed ✓` or a traceback.

## Where things are

- `intake/gate.py` — the rules a row is checked against, before the log.
- `intake/ingest.py` — split a form into row events, append the acceptable ones,
  record the refusals.
- `intake/consumer.py` — what applying one event means: apply, raise, or return a
  record and write nothing.
- `intake/rows.py` — the database-backed write.
- `intake/management/commands/demo_intake.py` — the walkthrough.

The first three touch no models, and `tests/test_examples/test_partisipa_intake.py`
exercises them against the real durable cursor and outcome tables.
