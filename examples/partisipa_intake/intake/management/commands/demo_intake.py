"""`manage.py demo_intake` — the consuming loop, end to end.

It answers: *what does a consumer look like when the reading position and the
record of what could not be applied are both kept in the database, and what does
each kind of failure leave behind?*

Six checks, each asserted hard (a failure raises CommandError):

  [1] REFUSED   — a progress row the rules decline never reaches the log, its
                  siblings do, and the refusal is a record with no offset.
  [2] HALT      — an event that fails to apply leaves a record, and the reading
                  position stays *below* it, so the event is still pending.
  [3] RECOVER   — the missing reference data arrives; the next run applies the
                  event that failed, skips one for a closed period, and does not
                  re-apply anything already committed.
  [4] RESUME    — a further run has nothing to do and applies nothing.
  [5] SKIP      — the same failure under the other policy advances past it.
  [6] DURABLE   — a fresh store object reads back the position and every record.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from typing import Any

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from django_rakaia import django_consumer
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import ConsumerCursor, ConsumerOutcome
from django_rakaia.outcomes import DjangoOutcomeStore
from django_rakaia.subscription import load_cursor
from intake.consumer import make_apply
from intake.ingest import submit_form
from intake.models import ProgressRow, ReportingPeriod, Suku
from intake.rows import DatabaseRows
from intake.seed import LATE_FORMS, PERIODS, PROGRESS_FORMS, REGISTERED_SUKUS
from rakaia.outcomes import Outcome
from rakaia.subscription import Consumed
from rakaia.types import StreamMessage

STREAM = "partisipa:progress"
CONSUMER = "progress-intake"


class CountedApply:
    """The apply, with a count of how many events were handed to it.

    A projection row proves an event was applied at some point; it cannot say
    whether it was applied twice. The count can, which is what check [3] needs.
    """

    def __init__(self, inner: Callable[[StreamMessage], Iterable[Outcome] | None]):
        self._inner = inner
        self.calls = 0

    def __call__(self, message: StreamMessage) -> Iterable[Outcome] | None:
        self.calls += 1
        return self._inner(message)


class Command(BaseCommand):
    help = "Consume a stream, recording what could not be applied and where it stopped."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        # Self-contained: ensure this demo's tables exist so `manage.py
        # demo_intake` works when run directly, not only via the migrate-first
        # `just` recipe. Idempotent — a no-op once migrations are applied.
        call_command("migrate", verbosity=0, interactive=False)

        store = DjangoStreamStore()
        self._reset(store)
        self._reference_data()

        offsets = self._check_refused(store)
        self._check_halt(store, offsets)
        self._check_recover()
        self._check_resume()
        self._check_skip(store)
        self._check_durable(store)

        self.stdout.write(self.style.SUCCESS("\nAll consuming-loop checks passed ✓"))

    # -- setup --------------------------------------------------------------

    def _reset(self, store: DjangoStreamStore) -> None:
        store.delete(STREAM)
        store.create(STREAM)
        for model in (ProgressRow, Suku, ReportingPeriod):
            model.objects.all().delete()
        ConsumerCursor.objects.filter(consumer_id=CONSUMER).delete()
        ConsumerOutcome.objects.all().delete()

    def _reference_data(self) -> None:
        for name in REGISTERED_SUKUS:
            Suku.objects.create(name=name)
        for period, closed in PERIODS.items():
            ReportingPeriod.objects.create(period=period, closed=closed)

    def _consumer(self) -> Any:
        # Name a record after the row it is about, the same way this consumer
        # names the ones it writes itself. Without this the loop falls back to
        # the event's position in the log, and the final table reads as two kinds
        # of thing in one column.
        return django_consumer(
            DjangoStreamStore(),
            STREAM,
            CONSUMER,
            subject_of=lambda message: json.loads(message.data)["row_key"],
            sequence_of=lambda message: json.loads(message.data)["form_key"],
        )

    def _run(self, on_error: str) -> tuple[Consumed, int]:
        apply = CountedApply(make_apply(DatabaseRows(), consumer=CONSUMER, path=STREAM))
        result = self._consumer().run(apply, on_error=on_error)
        return result, apply.calls

    def _records(self) -> list[Outcome]:
        return DjangoOutcomeStore().latest(CONSUMER, STREAM)

    # -- [1] refused --------------------------------------------------------

    def _check_refused(self, store: DjangoStreamStore) -> list[str]:
        refused: list[Outcome] = []
        for form in PROGRESS_FORMS:
            submitted = submit_form(
                form,
                store=store,
                outcomes=DjangoOutcomeStore(),
                consumer=CONSUMER,
                path=STREAM,
            )
            refused.extend(submitted.refused)

        messages, _ = store.read(STREAM)
        rows = [json.loads(m.data)["row_key"] for m in messages]

        self.stdout.write(
            "[1] REFUSED — a row the rules decline never reaches the log:"
        )
        for row in rows:
            self.stdout.write(f"    appended  {row}")
        for record in refused:
            self.stdout.write(
                f"    refused   {record.subject}  "
                f"({', '.join(record.reasons)}, offset {record.offset})"
            )

        if len(refused) != 1 or refused[0].subject in rows:
            raise CommandError(
                f"expected one refused row absent from the log: {refused}"
            )
        if refused[0].stage != "append" or refused[0].offset is not None:
            raise CommandError(f"a refused row has no offset: {refused[0]}")
        if len(rows) != 5:
            raise CommandError(f"expected 5 appended rows, got {rows}")
        self.stdout.write(
            self.style.SUCCESS(
                "    → the fact is upstream, not lost: fix the form and submit again ✓"
            )
        )
        return [m.offset for m in messages]

    # -- [2] halt -----------------------------------------------------------

    def _check_halt(self, store: DjangoStreamStore, offsets: list[str]) -> None:
        result, calls = self._run("halt")
        committed = load_cursor(CONSUMER, STREAM)
        failed = [r for r in self._records() if r.status == "failed"]

        self.stdout.write("\n[2] HALT — an event that fails to apply stops the pass:")
        self.stdout.write(f"    applied {result.applied} of {calls} handed over")
        self.stdout.write(f"    position {committed} (the failure is at {offsets[2]})")
        for record in failed:
            self.stdout.write(
                f"    failed    offset {record.offset}  "
                f"({', '.join(record.reasons)}"
                f", {record.params.get('exception_type', '?')})"
            )

        if not result.halted or committed != offsets[1]:
            raise CommandError(
                f"halt should leave the position below the failure: {committed}"
            )
        # The code is `unhandled`: our exception is the consumer's, not one of
        # rakaia's promised set, so the type is recorded beside it rather than
        # as it. Renaming our class cannot rewrite an operator's counts.
        if failed and failed[0].params.get("exception_type") != "UnknownSuku":
            raise CommandError(
                f"the consumer's own exception type belongs in params: {failed}"
            )
        if len(failed) != 1 or failed[0].offset != offsets[2]:
            raise CommandError(
                f"expected one failure recorded at {offsets[2]}: {failed}"
            )
        if ProgressRow.objects.count() != 2:
            raise CommandError(
                f"only the events below the failure applied, got "
                f"{ProgressRow.objects.count()} rows"
            )
        # And they carry what was reported. Counting rows says the loop ran;
        # only reading one says the projection is derived from the event rather
        # than from a constant, and this is the one place the real projection
        # (rather than the test double) is exercised at all.
        projected = {(row.output, row.percent) for row in ProgressRow.objects.all()}
        if projected != {("WATER", 40), ("SANITATION", 60)}:
            raise CommandError(f"the rows do not carry what was reported: {projected}")
        # The record names an offset, so the log answers what failed.
        message = self._at(store, failed[0].offset)
        self.stdout.write(
            f"    the log at that offset: {json.loads(message.data)['row_key']}"
        )
        self.stdout.write(
            self.style.SUCCESS(
                "    → still pending, so the next run delivers it again ✓"
            )
        )

    def _at(self, store: DjangoStreamStore, offset: str | None) -> StreamMessage:
        messages, _ = store.read(STREAM)
        for message in messages:
            if message.offset == offset:
                return message
        raise CommandError(f"no message at {offset}")

    # -- [3] recover --------------------------------------------------------

    def _check_recover(self) -> None:
        Suku.objects.create(name="Maubara")
        before = ProgressRow.objects.count()
        result, calls = self._run("halt")
        skipped = [r for r in self._records() if r.status == "skipped"]

        self.stdout.write("\n[3] RECOVER — the missing village is registered, and:")
        self.stdout.write(f"    {calls} events handed over, {result.applied} applied")
        for record in skipped:
            self.stdout.write(
                f"    skipped   {record.subject}  "
                f"({', '.join(record.reasons)}, offset {record.offset})"
            )

        if calls != 3:
            raise CommandError(
                f"the two already committed should not be handed over again, got {calls}"
            )
        if len(skipped) != 1 or skipped[0].stage != "project":
            raise CommandError(f"expected one deliberate skip: {skipped}")
        # The record this consumer wrote itself names the row, the same way the
        # refusal on the other side of the log does — so one name follows a row
        # whether or not its event ever got there. Nothing else checks this, and
        # it is a choice the example is making rather than behaviour it inherits.
        if "/" not in skipped[0].subject:
            raise CommandError(
                f"the skipped record should name the row, not its position in "
                f"the log: {skipped[0].subject}"
            )
        if ProgressRow.objects.count() != before + 2:
            raise CommandError(
                f"expected two more rows, got {ProgressRow.objects.count()}"
            )
        self.stdout.write(
            self.style.SUCCESS(
                "    → the failure replayed, the closed period was declined on purpose ✓"
            )
        )

    # -- [4] resume ---------------------------------------------------------

    def _check_resume(self) -> None:
        before = ProgressRow.objects.count()
        result, calls = self._run("halt")

        self.stdout.write("\n[4] RESUME — run it again with nothing new:")
        self.stdout.write(f"    status {result.status}, {calls} events handed over")
        if calls != 0 or result.applied != 0 or result.status != "caught_up":
            raise CommandError(f"a caught-up run applies nothing, got {result}")
        if ProgressRow.objects.count() != before:
            raise CommandError("a caught-up run changed the projection")
        self.stdout.write(
            self.style.SUCCESS(
                "    → the position is where the work stopped, not where it started ✓"
            )
        )

    # -- [5] skip -----------------------------------------------------------

    def _check_skip(self, store: DjangoStreamStore) -> None:
        for form in LATE_FORMS:
            submit_form(
                form,
                store=store,
                outcomes=DjangoOutcomeStore(),
                consumer=CONSUMER,
                path=STREAM,
            )
        head = store.get_current_offset(STREAM)
        result, _ = self._run("skip")
        committed = load_cursor(CONSUMER, STREAM)

        self.stdout.write(
            "\n[5] SKIP — the same kind of failure under the other policy:"
        )
        self.stdout.write(f"    applied {result.applied}, position {committed}")
        if result.halted or committed != head:
            raise CommandError(
                f"skip should advance past the failure to {head}, got {committed}"
            )
        if len([r for r in self._records() if r.status == "failed"]) != 2:
            raise CommandError("the skipped failure should still be recorded")
        self.stdout.write(
            self.style.SUCCESS(
                "    → one poisoned event does not stop a live stream; the record is how "
                "it is found again ✓"
            )
        )

    # -- [6] durable --------------------------------------------------------

    def _check_durable(self, store: DjangoStreamStore) -> None:
        # Nothing from the runs above is reused: a new store object, a new
        # consumer, as a restarted process would build.
        fresh = self._consumer()
        records = fresh.outcomes.latest(CONSUMER, STREAM)
        committed = fresh.cursors.load(CONSUMER, STREAM)

        self.stdout.write("\n[6] DURABLE — a fresh consumer reads it all back:")
        for record in records:
            self.stdout.write(
                f"    {record.stage:<8} {record.status:<8} {record.subject:<28} "
                f"{', '.join(record.reasons)}"
            )
        self.stdout.write(f"    position {committed}")
        # Every one of them names a row, whichever side of the log it came from
        # and whoever wrote it — the two this consumer wrote itself, and the two
        # the loop wrote when an apply raised. That is the comparison someone
        # opens this list to make, so it is asserted rather than admired.
        positional = [r.subject for r in records if "/" not in r.subject]
        if positional:
            raise CommandError(
                f"these records name a position rather than a row: {positional}"
            )
        self.stdout.write(
            "    (every record names a row — the consumer's own and the loop's "
            "alike, because the consumer said how)"
        )

        if committed != store.get_current_offset(STREAM):
            raise CommandError("the position did not survive the restart")
        seen = sorted((r.stage, r.status) for r in records)
        expected = sorted(
            [
                ("append", "refused"),
                ("project", "failed"),
                ("project", "failed"),
                ("project", "skipped"),
            ]
        )
        if seen != expected:
            raise CommandError(f"expected {expected}, got {seen}")
        self.stdout.write(
            self.style.SUCCESS(
                "    → four records, three recoveries: fix upstream, replay, or nothing ✓"
            )
        )
