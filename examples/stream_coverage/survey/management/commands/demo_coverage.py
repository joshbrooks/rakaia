"""`manage.py demo_coverage` — a stream falls behind its table, and the check says so.

It answers: *if rows reach the table by a path that writes no event, does
anything notice before a rebuild comes up short?* Four steps, each asserted hard
(a failure raises CommandError, so the demo exits non-zero if the check ever
stops finding the gap):

  [1] COVERED  — 20 rows saved the normal way; the check passes.
  [2] MISSING  — 5 more rows loaded with ``bulk_create``, which writes no
                 events; the check fails and names them.
  [3] STALE    — 2 of the first rows changed with ``QuerySet.update``, which
                 writes no events either; the check counts them as stale.
  [4] REPAIRED — the rows the check named are saved again the normal way, and
                 the check passes.

The database is flushed first, so every run starts from nothing and prints the
same ids.
"""

from __future__ import annotations

import contextlib
import json
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Any

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Max

from django_rakaia.models import StreamEvent
from survey.models import STREAM, Submission


def heading(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def check(expect_gap: bool) -> dict[str, Any]:
    """Run ``check_stream_coverage`` as an operator would, and return its report.

    Prints the command's own output. A gap makes the command raise, which is what
    turns into a non-zero exit status under ``manage.py`` and a failed unit under
    a nightly timer; that is asserted here, in both directions.
    """
    out = StringIO()
    failure: CommandError | None = None
    try:
        call_command("check_stream_coverage", stdout=out, no_color=True)
    except CommandError as exc:
        failure = exc
    print("$ python manage.py check_stream_coverage")
    print(out.getvalue().rstrip())
    if failure is not None:
        print(f"CommandError: {failure}")
        print("(exit status 1: this is what a nightly timer alerts on)")
    else:
        print("(exit status 0)")
    if expect_gap and failure is None:
        raise CommandError("expected the check to fail on a gap, and it passed")
    if not expect_gap and failure is not None:
        raise CommandError(f"expected the check to pass, and it failed: {failure}")

    # The same report as JSON, to assert on the numbers rather than the wording.
    data = StringIO()
    with contextlib.suppress(CommandError):
        call_command("check_stream_coverage", "--json", stdout=data)
    (report,) = json.loads(data.getvalue())
    return report


def expect(report: dict[str, Any], **wanted: Any) -> None:
    for key, value in wanted.items():
        if report[key] != value:
            raise CommandError(
                f"expected {key}={value!r}, the check says {report[key]!r}"
            )


class Command(BaseCommand):
    help = "Open a gap between a table and its stream, find it, and repair it."

    def handle(self, *args: Any, **options: Any) -> None:  # noqa: ARG002
        call_command("flush", interactive=False, verbosity=0)

        heading("[1] COVERED: 20 submissions saved the normal way")
        for n in range(1, 21):
            Submission.objects.create(name=f"submission {n}")
        print(f"Each save wrote one event to {STREAM!r}.")
        report = check(expect_gap=False)
        expect(report, ok=True, rows=20, covered=20, missing=0, stale=0, extra=0)

        heading("[2] MISSING: 5 more loaded with bulk_create")
        print(
            "bulk_create goes straight to the table and never calls save(), so no\n"
            "event is written. A bulk import or a backfill that was never run leaves\n"
            "exactly this gap: the table grows, the stream does not."
        )
        loaded = Submission.objects.bulk_create(
            Submission(name=f"imported {n}") for n in range(1, 6)
        )
        loaded_ids = tuple(str(row.pk) for row in loaded)
        report = check(expect_gap=True)
        expect(
            report,
            ok=False,
            rows=25,
            covered=20,
            missing=5,
            missing_sample=list(loaded_ids),
            stale=0,
        )

        heading("[3] STALE: 2 of the first rows changed with QuerySet.update")
        print(
            "update() writes the new values in one UPDATE and calls no save() either,\n"
            "so the rows have an event, but an out-of-date one."
        )
        # Set the change time explicitly, a second after the newest event, rather
        # than trusting the clock to have moved on since step 1.
        newest = StreamEvent.objects.aggregate(t=Max("event_ts"))["t"]
        changed_at = datetime.fromtimestamp(newest, tz=timezone.utc) + timedelta(
            seconds=1
        )
        edited = list(
            Submission.objects.order_by("pk").values_list("pk", flat=True)[:2]
        )
        Submission.objects.filter(pk__in=edited).update(
            name="edited in bulk", updated_at=changed_at
        )
        report = check(expect_gap=True)
        expect(
            report,
            ok=False,
            missing=5,
            stale=2,
            stale_sample=[str(pk) for pk in edited],
        )

        heading("[4] REPAIRED: save the rows the check named, the normal way")
        # Seven rows fit in the samples (at most ten each), so here they name every
        # row to repair. On a real table, the samples are where to start looking.
        to_repair = report["missing_sample"] + report["stale_sample"]
        print(f"Saving submissions {', '.join(to_repair)} again, one event each.")
        for row in Submission.objects.filter(pk__in=to_repair):
            row.save()
        report = check(expect_gap=False)
        expect(report, ok=True, rows=25, covered=25, missing=0, stale=0, extra=0)

        print()
        print("All stream coverage checks passed.")
