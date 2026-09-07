"""What the intake example claims about the library, tested away from its prose.

The example is a walkthrough: it narrates a progress form arriving, a row being
refused before it reaches the log, an event that fails to apply, one deliberately
skipped, and a second run picking up where the first stopped. What is worth
pinning is not the narration but the four facts underneath it, each of which is a
property of `django_consumer` and the outcome record rather than of the demo:

* a refused row leaves a record and no log entry, so the fact is upstream and not
  lost (ADR 0007, Decision 4, row three);
* a failed apply leaves a record and leaves the watermark **below** the event
  under ``halt``, so the event is still pending;
* a second run resumes from the committed watermark instead of re-applying what
  already landed;
* the records read back from a fresh store object, which is the half a standalone
  script cannot show.

The projection half of the example needs its own app installed, so these tests
give the apply an in-memory `ProgressRows` and let the demo command cover the
database-backed one. Everything else here — the cursor, the outcomes, the loop —
is the real thing.

``transaction=True`` on the consumer tests is not decoration: `DjangoConsumer`
refuses to run inside a transaction the caller opened, and under a plain
``django_db`` marker pytest-django has already opened one.
"""

from __future__ import annotations

import json

import pytest
from intake.consumer import UnknownSuku, make_apply
from intake.ingest import submit_form

from django_rakaia.consumer import django_consumer
from django_rakaia.outcomes import DjangoOutcomeStore
from django_rakaia.subscription import load_cursor
from rakaia.store import StreamStore
from rakaia.types import StreamMessage

STREAM = "partisipa:progress"
CONSUMER = "progress-intake"


class InMemoryProgressRows:
    """The projection half, as a dict — the counterpart of the example's tables."""

    def __init__(self, registered: set[str], closed: set[str] | None = None) -> None:
        self.registered = registered
        self.closed = closed or set()
        self.written: dict[tuple[str, str, str], int] = {}
        self.upserts = 0

    def period_is_open(self, period: str) -> bool:
        return period not in self.closed

    def upsert(self, *, suku: str, output: str, period: str, percent: int) -> None:
        self.upserts += 1
        if suku not in self.registered:
            raise UnknownSuku(suku)
        self.written[(suku, output, period)] = percent


def _row(output: str, percent: int) -> dict[str, object]:
    return {"output": output, "percent": percent}


FORM = {
    "key": "prog-fatuberliu-2026-01",
    "form_type": "PROGRESS",
    "suku": "Fatuberliu",
    "period": "2026-01",
    "rows": [
        _row("WATER", 40),
        _row("ROAD", 140),
        _row("SANITATION", 60),
    ],
}


def _seed(store: StreamStore, events: list[dict[str, object]]) -> list[StreamMessage]:
    store.create(STREAM)
    for event in events:
        store.append(STREAM, json.dumps(event).encode())
    messages, _ = store.read(STREAM)
    return messages


def _event(suku: str, output: str, period: str = "2026-01", percent: int = 40) -> dict:
    return {
        "schema_version": 1,
        "form_type": "PROGRESS",
        "form_key": f"prog-{suku.lower()}-{period}",
        "row_key": f"{suku}/{output}/{period}",
        "suku": suku,
        "output": output,
        "period": period,
        "percent": percent,
    }


@pytest.mark.django_db
class TestARefusedRowNeverReachesTheLog:
    def test_the_siblings_are_appended_and_the_refused_row_is_not(self) -> None:
        store = StreamStore()
        store.create(STREAM)

        submit_form(
            FORM,
            store=store,
            outcomes=DjangoOutcomeStore(),
            consumer=CONSUMER,
            path=STREAM,
        )

        messages, _ = store.read(STREAM)
        outputs = [json.loads(m.data)["output"] for m in messages]
        assert outputs == ["WATER", "SANITATION"]

    def test_the_refusal_is_a_record_with_no_offset(self) -> None:
        store = StreamStore()
        store.create(STREAM)
        outcomes = DjangoOutcomeStore()

        submit_form(
            FORM, store=store, outcomes=outcomes, consumer=CONSUMER, path=STREAM
        )

        records = outcomes.latest(CONSUMER, STREAM)
        assert len(records) == 1
        record = records[0]
        assert (record.stage, record.status, record.offset) == (
            "append",
            "refused",
            None,
        )
        assert record.subject == "Fatuberliu/ROAD/2026-01"
        assert "percent_out_of_range" in record.reasons
        # The whole row is checked, not the first breach — ADR 0007's worked
        # example turns on the gate not short-circuiting.
        assert record.sequence_key == FORM["key"]


@pytest.mark.django_db(transaction=True)
class TestAFailedApply:
    def _stream(self) -> tuple[StreamStore, list[StreamMessage]]:
        store = StreamStore()
        messages = _seed(
            store,
            [
                _event("Fatuberliu", "WATER"),
                _event("Fatuberliu", "SANITATION", percent=60),
                _event("Maubara", "ROAD", percent=30),
                _event("Fatuberliu", "WATER", period="2026-02", percent=55),
            ],
        )
        return store, messages

    def test_halt_leaves_the_position_below_the_event_that_failed(self) -> None:
        store, messages = self._stream()
        rows = InMemoryProgressRows(registered={"Fatuberliu"})
        consumer = django_consumer(store, STREAM, CONSUMER)

        result = consumer.run(
            make_apply(rows, consumer=CONSUMER, path=STREAM), on_error="halt"
        )

        assert result.halted
        assert result.applied == 2
        assert result.cursor == messages[1].offset
        assert load_cursor(CONSUMER, STREAM) == messages[1].offset
        # The event after the failure is still pending, not skipped.
        assert len(rows.written) == 2

    def test_the_record_names_the_offset_the_replay_would_start_from(self) -> None:
        store, messages = self._stream()
        rows = InMemoryProgressRows(registered={"Fatuberliu"})
        consumer = django_consumer(store, STREAM, CONSUMER)

        consumer.run(make_apply(rows, consumer=CONSUMER, path=STREAM), on_error="halt")

        records = DjangoOutcomeStore().latest(CONSUMER, STREAM)
        assert len(records) == 1
        assert (records[0].stage, records[0].status) == ("project", "failed")
        assert records[0].offset == messages[2].offset
        assert records[0].reasons == ("UnknownSuku",)

    def test_skip_advances_past_it_where_halt_stops(self) -> None:
        store, messages = self._stream()
        rows = InMemoryProgressRows(registered={"Fatuberliu"})
        consumer = django_consumer(store, STREAM, CONSUMER)

        result = consumer.run(
            make_apply(rows, consumer=CONSUMER, path=STREAM), on_error="skip"
        )

        assert not result.halted
        assert result.cursor == messages[-1].offset
        assert load_cursor(CONSUMER, STREAM) == messages[-1].offset
        assert len(rows.written) == 3

    def test_a_second_run_resumes_rather_than_re_applying(self) -> None:
        store, _ = self._stream()
        rows = InMemoryProgressRows(registered={"Fatuberliu"})
        consumer = django_consumer(store, STREAM, CONSUMER)
        apply = make_apply(rows, consumer=CONSUMER, path=STREAM)

        consumer.run(apply, on_error="halt")
        rows.registered.add("Maubara")
        second = consumer.run(apply, on_error="halt")

        assert second.applied == 2
        # Five attempts for four events: the one that failed is attempted twice,
        # once before the suku was registered and once after. Four rows written,
        # so the retry landed on the same row rather than adding one.
        assert rows.upserts == 5
        assert len(rows.written) == 4

    def test_the_projected_row_carries_the_value_the_event_reported(self) -> None:
        """The one thing an example of a projection has to get right.

        Every other test here counts rows or reads records. None of them looked
        at what was written, so the write could have been a constant and the
        suite would not have noticed — which is the failure an example is least
        allowed to have, since someone will copy it.
        """
        store, _ = self._stream()
        rows = InMemoryProgressRows(registered={"Fatuberliu"})
        consumer = django_consumer(store, STREAM, CONSUMER)

        consumer.run(make_apply(rows, consumer=CONSUMER, path=STREAM), on_error="skip")

        assert rows.written[("Fatuberliu", "WATER", "2026-01")] == 40
        assert rows.written[("Fatuberliu", "SANITATION", "2026-01")] == 60

    def test_a_fresh_store_object_reads_the_record_back(self) -> None:
        store, _ = self._stream()
        rows = InMemoryProgressRows(registered={"Fatuberliu"})
        django_consumer(store, STREAM, CONSUMER).run(
            make_apply(rows, consumer=CONSUMER, path=STREAM), on_error="halt"
        )

        # Nothing of the first run's is reused: a new store object, as a restarted
        # process would build.
        records = DjangoOutcomeStore().latest(CONSUMER, STREAM)
        assert [(r.stage, r.status) for r in records] == [("project", "failed")]
        assert load_cursor(CONSUMER, STREAM) is not None


@pytest.mark.django_db(transaction=True)
class TestADeliberateSkip:
    def test_a_closed_period_is_recorded_and_the_position_advances(self) -> None:
        store = StreamStore()
        messages = _seed(
            store,
            [
                _event("Fatuberliu", "WATER"),
                _event("Fatuberliu", "ROAD", period="2025-12", percent=80),
            ],
        )
        rows = InMemoryProgressRows(registered={"Fatuberliu"}, closed={"2025-12"})
        consumer = django_consumer(store, STREAM, CONSUMER)

        result = consumer.run(
            make_apply(rows, consumer=CONSUMER, path=STREAM), on_error="halt"
        )

        # Returning an outcome is not failing: the message applied and the cursor
        # moved on.
        assert not result.halted
        assert result.applied == 2
        assert result.cursor == messages[-1].offset
        assert len(rows.written) == 1

        records = DjangoOutcomeStore().latest(CONSUMER, STREAM)
        assert [(r.stage, r.status) for r in records] == [("project", "skipped")]
        assert records[0].reasons == ("period_closed",)
