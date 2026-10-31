"""`manage.py demo_consumer_rebuild` — what a consumer does when its log changes.

It answers four questions the happy path never asks, each asserted hard (a
failure raises CommandError):

  [1] REPLACED  — the stream is deleted and recreated with a new stamp. The
                  next run applies nothing, keeps its cursor and its rows, and
                  says `replaced` so an operator knows to rebuild.
  [2] REBUILD   — derived rows are cleared and replayed from the recreated log,
                  the position is moved to the new head, and a later event
                  resumes from there.
  [3] TRANSIENT — an apply raises Django's `OperationalError`. The run reports
                  `transient`, records no outcome, leaves the cursor below the
                  event, and the next run retries that same event.
  [4] CRASH     — the process dies mid-pass. The cursor and that pass's writes
                  roll back together, so the next run delivers every event again
                  and the projection converges rather than doubling.

The projection stores absolute values, which is what makes [4] idempotent: an
event delivered twice writes the same row twice, not a doubled total.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from typing import Any

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.db import OperationalError

from django_rakaia import DjangoExecutor, django_consumer
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import ConsumerCursor, ConsumerOutcome
from django_rakaia.outcomes import DjangoOutcomeStore
from django_rakaia.subscription import load_cursor, load_generation
from ledger.handlers import ledger_row
from ledger.models import LedgerRow
from rakaia import replay
from rakaia.subscription import Consumed
from rakaia.types import StreamMessage

STREAM = "ledger:entries"
CONSUMER = "ledger-rebuild"

INITIAL = [
    {"key": "alpha", "value": 1},
    {"key": "beta", "value": 2},
    {"key": "gamma", "value": 3},
]
RECREATED = [
    {"key": "alpha", "value": 10},
    {"key": "delta", "value": 4},
]
AFTER_REBUILD = [{"key": "beta", "value": 20}]
TRANSIENT_EVENT = [{"key": "zeta", "value": 60}]
CRASH_EVENTS = [
    {"key": "alpha", "value": 100},
    {"key": "beta", "value": 200},
    {"key": "gamma", "value": 300},
    {"key": "delta", "value": 400},
    {"key": "epsilon", "value": 500},
]


def apply_ledger(message: StreamMessage) -> None:
    """Apply one event, idempotently: set a keyed row to an absolute value."""
    DjangoExecutor().apply([ledger_row(json.loads(message.data))])


class CountedApply:
    """The apply, with a count and the offsets handed over in this pass."""

    def __init__(
        self,
        inner: Callable[[StreamMessage], Iterable[Any] | None] = apply_ledger,
    ) -> None:
        self._inner = inner
        self.offsets: list[str] = []

    @property
    def calls(self) -> int:
        return len(self.offsets)

    def __call__(self, message: StreamMessage) -> Iterable[Any] | None:
        self.offsets.append(message.offset)
        return self._inner(message)


class CrashAfter(CountedApply):
    """Apply a few events, then die the way a killed process does.

    ``SystemExit`` is not caught by the consume loop (which records exceptions),
    and it is not caught by the pass's transaction either: both contexts roll
    back and re-raise, so this is a pass that leaves no cursor behind.
    """

    def __init__(self, limit: int) -> None:
        super().__init__()
        self.limit = limit

    def __call__(self, message: StreamMessage) -> Iterable[Any] | None:
        result = super().__call__(message)
        if self.calls > self.limit:
            raise SystemExit(f"simulated process death after {self.limit} events")
        return result


class TransientOnce(CountedApply):
    """Raise Django's ``OperationalError`` on the first event, then behave."""

    def __init__(self) -> None:
        super().__init__()
        self.raised = False

    def __call__(self, message: StreamMessage) -> Iterable[Any] | None:
        super().__call__(message)
        if not self.raised:
            self.raised = True
            raise OperationalError("simulated dropped connection")
        return apply_ledger(message)


class Command(BaseCommand):
    help = "A consumer survives its stream being replaced, and a pass being cut short."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        # Self-contained: ensure this demo's tables exist so running the command
        # directly works as well as through the migrate-first `just` recipe.
        call_command("migrate", verbosity=0, interactive=False)

        self.store = DjangoStreamStore()
        self._reset()

        self._check_replaced()
        self._check_rebuild()
        self._check_transient()
        self._check_crash()

        self.stdout.write(
            self.style.SUCCESS("\nAll consumer failure-path checks passed ✓")
        )

    # -- setup --------------------------------------------------------------

    def _reset(self) -> None:
        self.store.delete(STREAM)
        self.store.create(STREAM)
        LedgerRow.objects.all().delete()
        ConsumerCursor.objects.all().delete()
        ConsumerOutcome.objects.all().delete()

    def _append(self, events: list[dict[str, Any]]) -> None:
        self.store.append_many(
            STREAM, [(json.dumps(event).encode(), None) for event in events]
        )

    def _run(self, apply: Callable[..., Any] = apply_ledger) -> Consumed:
        return django_consumer(self.store, STREAM, CONSUMER).run(apply, on_error="halt")

    def _rows(self) -> dict[str, int]:
        return dict(LedgerRow.objects.values_list("key", "value"))

    def _expect_rows(self, wanted: dict[str, int], where: str) -> None:
        got = self._rows()
        if got != wanted:
            raise CommandError(f"{where}: expected rows {wanted}, got {got}")

    def _expect(self, condition: bool, message: str) -> None:
        if not condition:
            raise CommandError(message)

    # -- [1] replaced -------------------------------------------------------

    def _check_replaced(self) -> None:
        self._append(INITIAL)
        first = self._run()

        self.stdout.write(
            "[1] REPLACED — delete and recreate the log under the reader:"
        )
        self.stdout.write(
            f"    first run: {first.status}, applied {first.applied}, "
            f"position {first.cursor}"
        )
        self._expect(first.status == "fresh", f"a first run is fresh: {first.status}")
        self._expect(first.applied == 3, f"expected 3 applied, got {first.applied}")
        self._expect_rows({"alpha": 1, "beta": 2, "gamma": 3}, "after the first run")

        old_cursor = load_cursor(CONSUMER, STREAM)
        old_generation = load_generation(CONSUMER, STREAM)
        old_rows = self._rows()
        self._expect(
            old_cursor == self.store.get_current_offset(STREAM),
            "the first run should be caught up to the head",
        )
        self._expect(
            old_generation is not None, "the cursor should save the stream stamp"
        )

        # The operator's actual mistake: delete the log and start again.
        self.store.delete(STREAM)
        self.store.create(STREAM)
        new_generation = self.store.get_generation(STREAM)
        self._expect(
            new_generation != old_generation,
            "recreating a stream must issue a new stamp",
        )
        self._append(RECREATED)

        result = self._run()

        self.stdout.write(
            f"    recreated with a new stamp; run again: {result.status}, "
            f"applied {result.applied}"
        )
        self.stdout.write(
            f"    position still {load_cursor(CONSUMER, STREAM)} "
            f"(the new head is {self.store.get_current_offset(STREAM)})"
        )
        self._expect(
            result.status == "replaced", f"expected replaced, got {result.status}"
        )
        self._expect(result.needs_rebuild, "a replaced stream needs a rebuild")
        self._expect(
            result.applied == 0, f"a replaced stream applies nothing: {result}"
        )
        self._expect(result.outcomes == (), "a replaced stream records no outcome")
        self._expect(
            load_cursor(CONSUMER, STREAM) == old_cursor,
            "a replaced stream must leave the cursor unchanged",
        )
        self._expect(
            load_generation(CONSUMER, STREAM) == old_generation,
            "a replaced stream must not adopt the new stamp",
        )
        self._expect_rows(old_rows, "a replaced stream must leave the rows alone")
        self.stdout.write(
            self.style.SUCCESS(
                "    → nothing applied, nothing moved: the derived data is now stale "
                "and the operator is told so ✓"
            )
        )

    # -- [2] rebuild --------------------------------------------------------

    def _check_rebuild(self) -> None:
        # A rebuild is two things: derive the rows again from the log, then put
        # the consumer's position at the head so it resumes rather than replays.
        LedgerRow.objects.all().delete()
        ConsumerCursor.objects.filter(consumer_id=CONSUMER, stream_path=STREAM).delete()

        replay(store=self.store, stream_path=STREAM, executor=DjangoExecutor())
        rebuilt = self._rows()
        self.stdout.write(
            "\n[2] REBUILD — clear the rows and replay the recreated log:"
        )
        self.stdout.write(f"    rebuilt rows {rebuilt}")
        self._expect_rows({"alpha": 10, "delta": 4}, "after the rebuild")

        # A fresh consumer applies the same events again (idempotently) and lands
        # at the head. That is the position a later event resumes from.
        caught_up = self._run()
        self._expect(
            caught_up.cursor == self.store.get_current_offset(STREAM),
            "the rebuild should leave the position at the head",
        )
        self._expect_rows({"alpha": 10, "delta": 4}, "after the catch-up run")

        self._append(AFTER_REBUILD)
        result = self._run()
        self.stdout.write(
            f"    one new event after the rebuild: {result.status}, "
            f"applied {result.applied}, position {result.cursor}"
        )
        self._expect(result.status == "advanced", f"expected advanced: {result.status}")
        self._expect(
            result.applied == 1,
            f"resuming should apply only the new event, got {result.applied}",
        )
        self._expect(
            result.cursor == self.store.get_current_offset(STREAM),
            "the resumed run should reach the head",
        )
        self._expect_rows(
            {"alpha": 10, "beta": 20, "delta": 4}, "after resuming at the right offset"
        )
        self.stdout.write(
            self.style.SUCCESS(
                "    → the rebuilt projection and the position agree again, and the "
                "next event resumes rather than replaying ✓"
            )
        )

    # -- [3] transient ------------------------------------------------------

    def _check_transient(self) -> None:
        before = load_cursor(CONSUMER, STREAM)
        self._append(TRANSIENT_EVENT)
        event_offset = self.store.get_current_offset(STREAM)
        apply = TransientOnce()
        result = self._run(apply)
        records = DjangoOutcomeStore().latest(CONSUMER, STREAM)

        self.stdout.write(
            "\n[3] TRANSIENT — the apply raises Django's OperationalError:"
        )
        self.stdout.write(
            f"    status {result.status}, transient={result.transient}, "
            f"applied {result.applied}, halted={result.halted}"
        )
        self.stdout.write(
            f"    position {load_cursor(CONSUMER, STREAM)} (event at {event_offset})"
        )
        self._expect(result.transient, f"expected a transient result: {result}")
        self._expect(result.halted, "a transient error halts the pass")
        self._expect(result.applied == 0, f"nothing should apply: {result}")
        self._expect(result.outcomes == (), "a transient error records no outcome")
        self._expect(records == [], f"the outcome table should stay empty: {records}")
        self._expect(
            load_cursor(CONSUMER, STREAM) == before,
            "the cursor must stay below the event that failed",
        )
        self._expect(
            before is not None and before < event_offset,
            "the retried event must still be above the cursor",
        )
        self._expect(
            not LedgerRow.objects.filter(key="zeta").exists(),
            "the failed event must not have been applied",
        )

        retry = CountedApply()
        second = self._run(retry)
        self.stdout.write(
            f"    next run retried {retry.offsets} and applied {second.applied}"
        )
        self._expect(
            retry.offsets == [event_offset],
            f"the retry should hand over the same event, got {retry.offsets}",
        )
        self._expect(second.applied == 1, f"the retry should apply it: {second}")
        self._expect(
            LedgerRow.objects.filter(key="zeta", value=60).exists(),
            "the retried event should reach the projection",
        )
        self.stdout.write(
            self.style.SUCCESS(
                "    → a database blink is not a bad event: it is still pending, and "
                "the next run tries it again ✓"
            )
        )

    # -- [4] crash ----------------------------------------------------------

    def _check_crash(self) -> None:
        before = load_cursor(CONSUMER, STREAM)
        baseline = self._rows()
        self._append(CRASH_EVENTS)

        crasher = CrashAfter(limit=2)
        try:
            self._run(crasher)
        except SystemExit as exc:
            self.stdout.write(f"\n[4] CRASH — the process dies mid-pass: {exc}")
        else:  # pragma: no cover - the crash above is the point
            raise CommandError("the simulated crash never happened")

        self.stdout.write(
            f"    {crasher.calls} events handed over before the crash, "
            f"{crasher.calls - 1} applied"
        )
        self.stdout.write(
            f"    position still {load_cursor(CONSUMER, STREAM)} "
            f"(the pass and the cursor rolled back together)"
        )
        self._expect(
            load_cursor(CONSUMER, STREAM) == before,
            "a crash must leave the cursor where the pass started",
        )
        self._expect_rows(baseline, "after a crashed pass (its writes roll back)")

        counter = CountedApply()
        result = self._run(counter)
        expected = {
            "alpha": 100,
            "beta": 200,
            "gamma": 300,
            "delta": 400,
            "epsilon": 500,
            "zeta": 60,
        }
        self.stdout.write(
            f"    re-run from the same cursor: {counter.calls} events handed over, "
            f"applied {result.applied}"
        )
        self._expect(
            counter.calls == len(CRASH_EVENTS),
            f"every event should be delivered again, got {counter.calls}",
        )
        self._expect(result.applied == len(CRASH_EVENTS), f"all should apply: {result}")
        self._expect_rows(expected, "after the re-run")
        self._expect(
            crasher.calls + counter.calls > len(CRASH_EVENTS),
            "at least one event should have been delivered twice",
        )

        again = self._run()
        self._expect(again.applied == 0, f"a third run applies nothing: {again}")
        self._expect_rows(expected, "after a third run (idempotent)")
        self.stdout.write(
            self.style.SUCCESS(
                "    → at-least-once delivery plus an idempotent apply: the rows "
                "converge however many times the pass is cut short ✓"
            )
        )
