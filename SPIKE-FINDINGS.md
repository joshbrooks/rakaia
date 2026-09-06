# Spike: does a third backend prove the class is closed?

Not for merge. Branch `spike/django-outcome-store`, run against `feat/event-outcomes`.

## The question

Five review rounds each found one more value that one store kept and another
refused. `encode`/`decode` was written to close that class by reading each field's
declared type rather than naming the fields. A third backend, with a constraint
neither existing one has, is the falsifiable test: do the shared suites catch it
**without anyone adding a case for it**?

## Result: yes for shape, no for size — and the gap is in the test environment

**The contract discriminated on the first run.** `test_re_recording_an_attempt_replaces_it`
failed immediately: a `unique_together` on `(consumer, stream_path, subject, attempt)`
refuses a re-recorded attempt where both other stores replace it. No Django-specific test
was written; the shared suite found it. The store was wrong, not the contract — it now
upserts, and all 17 contract cases pass.

**The predicted sixth member is real, and the suite cannot see it.** The prediction was
that a `CharField(max_length=…)` would accept a long name in memory and in files and
truncate or refuse in a database. A 300-character stream path:

| backend | result |
|---|---|
| in-memory | kept, 311 characters |
| JSONL | kept, 311 characters |
| Django on **SQLite** | kept, 311 characters — `max_length` is not enforced |
| Django on **Postgres** | `DataError: value too long for type character varying(255)` |

So the class is **not** closed for size, and the reason it looked closed is the same
structural fault one level up from the one this work fixed: **the default test database is
the permissive one.** `RAKAIA_TEST_DB` defaults to `sqlite`; CI runs a `test-postgres` job,
but a developer running the suite locally sees green.

## What that implies

1. The codec closes what it was built to close. Reading the declaration also caught two
   cases nobody reported — a value outside a `Literal`'s domain, and `bool` where `int`
   was declared.
2. A length bound is **not** expressible as a type, so the codec cannot own it. Either
   the record declares its own limits (and the codec enforces them, making every backend
   agree), or the differential suite has to run somewhere the limits are real.
3. The second is required regardless: a backend can always add a constraint the record
   does not know about. That is the argument for the cross-backend comparison existing at
   all, and for it running under `RAKAIA_TEST_DB=postgres` in the same job that already
   does.

## What is here

- `src/django_rakaia/outcomes.py`, `models_outcomes.py`, `migrations/0010_eventoutcome.py`
- `tests/test_django_rakaia/test_outcome_store_contract.py`

Not reviewed, not exported, no admin, no retention. The model's field widths copy
`ConsumerCursor` deliberately, because that is what a real one would inherit.
