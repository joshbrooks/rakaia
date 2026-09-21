# Check a stream still matches its table

A stream written from a table can fall behind it without anyone noticing. A
code path that saves rows without emitting an event, a producer that was
switched off for a deploy and never switched back on, a bulk import that went
straight to the database: in each case the table keeps growing, the stream does
not, and nothing fails until someone rebuilds from the stream and the result
comes up short. One real stream held 2,251 events where re-seeding from its rows
produced 15,474, and the only thing that found it was a full rebuild on a
restored copy.

`check_stream_coverage` finds that gap on its own. For each stream you list, it
compares the table with the events and reports three things: rows that no event
names (**missing**), rows that changed after their newest event (**stale**), and
events whose row no longer exists (**extra**). Missing or stale rows make the
command fail, so a nightly timer can alert on it. Extra events do not, because a
deleted row leaves its events behind on purpose.

## Setting it up

The check needs to know which table feeds which stream, and where in each event
the row's id lives. List that in settings:

```python
RAKAIA_COVERAGE_CHECKS = [
    {
        "model": "forms.Submission",
        "filter": {"form__slug": "tf611"},
        "stream_path": "submissions/tf611",
        "subject_key": "submission",  # each event carries {"submission": <row id>}
    },
]
```

Then run it:

```console
$ python manage.py check_stream_coverage
GAP submissions/tf611: rows=15474 covered=2251 missing=13223 stale=- extra=0 skipped=0
    missing_sample: 3, 4, 7, 8, 11, 12, 15, 16, 19, 20
CommandError: 1 of 1 streams do not cover their table: submissions/tf611
```

The sample is at most ten ids, which is enough to go and look at why those rows
never produced an event. `--json` prints the same reports as a JSON list, for a
script or a monitoring system to read.

## Catching rows that changed later

A row can have an event and still be out of date, if it was edited later and the
edit never reached the stream. The check can catch that too, if it knows when
each row last changed. Name that time with `changed_field`:

```python
{
    "model": "forms.Submission",
    "stream_path": "submissions/tf611",
    "subject_key": "submission",
    "changed_field": "updated_at",
}
```

Many tables have no such column. If the change time lives somewhere else, most
often in a history table filled by database triggers, point the entry at a
function that returns the queryset with the time already worked out, and name
that instead:

```python
# forms/coverage.py
from django.db.models import Max
from forms.models import Submission


def tf611_submissions():
    return Submission.objects.filter(form__slug="tf611").annotate(
        last_changed=Max("events__pgh_created_at")
    )
```

```python
RAKAIA_COVERAGE_CHECKS = [
    {
        "queryset": "forms.coverage.tf611_submissions",
        "stream_path": "submissions/tf611",
        "subject_key": "submission",
        "changed_field": "last_changed",
    },
]
```

## Running it every night

The command exits with a non-zero status when any stream has a gap, which is
all a scheduler needs. With systemd:

```ini
# /etc/systemd/system/stream-coverage.service
[Unit]
Description=Check each stream still covers its table

[Service]
Type=oneshot
User=app
WorkingDirectory=/srv/app
ExecStart=/srv/app/.venv/bin/python manage.py check_stream_coverage
```

```ini
# /etc/systemd/system/stream-coverage.timer
[Unit]
Description=Nightly stream coverage check

[Timer]
OnCalendar=*-*-* 03:30:00
Persistent=true

[Install]
WantedBy=timers.target
```

```console
$ sudo systemctl enable --now stream-coverage.timer
```

A failed run shows up in `systemctl --failed` and in the journal with the full
report; hang whatever alerting you already use for failed units off it. With
cron, the same thing is one line. Cron mails whatever a job prints, so send the
report itself to `/dev/null` and only a failure's error message is mailed:

```cron
30 3 * * *  cd /srv/app && .venv/bin/python manage.py check_stream_coverage >/dev/null
```

From Python, the same check is one call:

```python
from django_rakaia import stream_coverage

report = stream_coverage(
    Submission.objects.filter(form__slug="tf611"),
    "submissions/tf611",
    subject_key="submission",
)
if not report.ok:
    print(report.missing, report.missing_sample)
```

## Appendix

**Settings entry.** Each entry is a dict. It names its rows with exactly one of
`model` (an app label, `"app.Model"`, optionally with `filter`, a dict of lookups
passed to `.filter()`) or `queryset` (the dotted path of a function taking no
arguments and returning a `QuerySet`). `stream_path` and `subject_key` are
required. `row_key` (default `"pk"`) names the column the payload's key is
compared against, and `changed_field` is optional. Any other key is refused, and
every entry is validated before any is run. An empty or missing
`RAKAIA_COVERAGE_CHECKS` is an error rather than a pass, because a nightly check
with nothing to check would otherwise report success forever.

**What counts as covered.** An event is about a row when
`data[subject_key]` equals the row's `row_key` compared as text, so
`{"submission": 7}` and `{"submission": "7"}` both name pk 7. A UUID key is
compared as bare hex, so the hyphenated and plain spellings match. A JSON float
is not normalised: `7.0` does not name pk 7. Only events in the one stream named
are considered; a table fanned out into one stream per row is out of scope for
this check. Events whose `payload_encoding` is set (a `text/plain` or binary
protocol stream) are not looked inside, and are counted in `skipped`.

**What counts as stale.** A row is stale when `changed_field` is later than the
newest event about it. An event's time is its `event_ts` if set, otherwise its
`created_at` (the event row's, written in the same transaction as the entry's
`created_at` that readers fall back to). `changed_field` may be a
column or any annotation, and may hold a datetime or Unix seconds. A row with no
change time is never stale, and a missing row is counted as missing, not stale.
`stale` is `None` in the report when no `changed_field` was given, so "not
checked" never reads as zero.

**Cost.** `stream_coverage` runs three read-only queries on `queryset.db`,
whatever the size of the table: one groups the stream's events by subject and
takes each subject's newest time, one streams the rows once in key order, and one
counts skipped events. The two sides are matched in Python. Payloads are never
decoded in Python; the database extracts the key. Memory grows with the number of
distinct subjects in the stream, not with the number of rows. The first version
compared each row against the stream in SQL instead, and with no index on the
payload key that was quadratic: over ten minutes for 15,000 rows on SQLite. The
current shape takes about 0.3 seconds for the same table on SQLite and Postgres.

**Database alias.** Everything is read through the queryset's own alias, events
included, so a check pointed at a restored copy with `.using("restored")` reads
that copy's stream, not the live one. The durable store is the only store read.

**Consistency.** The three queries run in one transaction on the queryset's
database, and on Postgres that transaction is read-only and `REPEATABLE READ`, so
they all see the same snapshot: a row and its event committed while the check
runs are either both counted or both not. Called inside a transaction of your
own, the check uses your isolation level instead. SQLite reads one snapshot per
transaction already.

**Clocks.** Event times come from the application server's clock: `event_ts`
when rakaia or a producer stamps it, and `created_at`, which Django fills in when
the row is saved. A change time taken from a history table is usually the
database's clock at the start of the writing transaction. If the two clocks
drift apart, rows can read as stale when they are not, or the reverse, by up to
the drift. Keep the servers on NTP; a handful of stale rows that are all within
seconds of their event is the sign to look at.

**Numeric keys.** A `subject_key` that looks like a number, such as `"0"`, is
read as a position in a JSON list rather than an object key, so it never matches.
Name payload keys with letters.

**JSON null.** A payload whose key is JSON `null` names no row, and is ignored on
both databases rather than counted as a subject. A subject that is the *string*
`"null"` is ignored on SQLite too, where Django extracts it as the same text, but
counted on Postgres; it can only move `extra`, never `missing` or `stale`.

**Samples.** Each sample holds at most ten keys (`django_rakaia.coverage.SAMPLE_LIMIT`).
Missing and stale samples are in the queryset's `row_key` order; extra samples
are sorted as text.
