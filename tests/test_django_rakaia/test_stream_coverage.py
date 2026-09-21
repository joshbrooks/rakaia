"""Tests for the stream-coverage check (#285).

The check answers one question about a table and the stream written from it:
does every row have at least one event about it, and has any row changed since
its newest event? The failure it exists for was found only by a full rebuild on a
restored copy: one stream held 2,251 events where re-seeding from the rows
produced 15,474.
"""

from __future__ import annotations

import io
import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connections
from django.db.models import DateTimeField, Max, Value
from django.test.utils import CaptureQueriesContext

from django_rakaia import StreamCoverage, stream_coverage
from django_rakaia.coverage import SAMPLE_LIMIT
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.envelope import append_event
from django_rakaia.management.commands import check_stream_coverage
from django_rakaia.models import Stream, StreamEntry, StreamEvent
from tests.test_django_rakaia.models import (
    CoverageRow,
    CoverageRowChange,
    FinanceLine,
    SubmissionProjection,
)

PATH = "submissions/tf611"
T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def _about(pk, *, path: str = PATH, using: str = "default", event_ts=None) -> None:
    """Append one event naming row ``pk`` under ``submission``, as a producer does."""
    append_event(
        DjangoStreamStore(using=using),
        path,
        {"submission": pk},
        label="submitted",
        event_ts=event_ts,
    )


def _raw_event(
    data,
    *,
    path: str = PATH,
    using: str = "default",
    payload_encoding=None,
    event_ts=None,
    created_at=None,
) -> None:
    """Write one event row directly, for shapes the store's API does not produce."""
    stream, _ = Stream.objects.using(using).get_or_create(stream_id=path)
    event = StreamEvent.objects.using(using).create(
        data=data,
        event_type="raw",
        payload_encoding=payload_encoding,
        event_ts=event_ts,
    )
    if created_at is not None:
        StreamEvent.objects.using(using).filter(pk=event.pk).update(
            created_at=created_at
        )
    top = (
        StreamEntry.objects.using(using)
        .filter(stream=stream)
        .aggregate(m=Max("offset"))
    )
    StreamEntry.objects.using(using).create(
        stream=stream, event=event, offset=(top["m"] or 0) + 1
    )


def _rows(n: int, *, using: str = "default", **fields) -> list[CoverageRow]:
    return [CoverageRow.objects.using(using).create(**fields) for _ in range(n)]


def annotated_rows():
    """A queryset factory for a settings entry: change time from a history table."""
    return CoverageRow.objects.annotate(last_changed=Max("changes__changed_at"))


# ---------------------------------------------------------------------------
# missing
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestMissing:
    def test_rows_without_an_event_are_missing_and_sampled(self):
        rows = _rows(5)
        for row in rows[:3]:
            _about(row.pk)

        report = stream_coverage(
            CoverageRow.objects.all(), PATH, subject_key="submission"
        )

        assert isinstance(report, StreamCoverage)
        assert report.rows == 5
        assert report.covered == 3
        assert report.missing == 2
        assert report.missing_sample == (str(rows[3].pk), str(rows[4].pk))
        assert report.extra == 0
        assert report.stale is None

    def test_the_sample_is_bounded(self):
        _rows(SAMPLE_LIMIT + 5)

        report = stream_coverage(
            CoverageRow.objects.all(), PATH, subject_key="submission"
        )

        assert report.missing == SAMPLE_LIMIT + 5
        assert len(report.missing_sample) == SAMPLE_LIMIT

    def test_keys_compare_as_text_whatever_the_payload_type(self):
        # An integer pk against a JSON number and against a JSON string: both
        # name the row, and neither may be reported missing.
        as_number, as_string = _rows(2)
        _about(as_number.pk)
        _about(str(as_string.pk))

        report = stream_coverage(
            CoverageRow.objects.all(), PATH, subject_key="submission"
        )

        assert report.missing == 0
        assert report.covered == 2

    def test_row_key_may_name_another_column(self):
        FinanceLine.objects.create(submission_id="alpha", suku="s")
        FinanceLine.objects.create(submission_id="beta", suku="s")
        append_event(DjangoStreamStore(), PATH, {"sub": "alpha"}, label="x")

        report = stream_coverage(
            FinanceLine.objects.all(), PATH, subject_key="sub", row_key="submission_id"
        )

        assert report.missing == 1
        assert report.missing_sample == ("beta",)

    def test_a_uuid_key_matches_its_payload_form_on_every_backend(self):
        # SQLite stores a UUID column as bare hex; the payload carries the
        # hyphenated form `DjangoJSONEncoder` writes.
        covered_ref, missing_ref = uuid.uuid4(), uuid.uuid4()
        SubmissionProjection.objects.create(submission_id=covered_ref)
        SubmissionProjection.objects.create(submission_id=missing_ref)
        append_event(DjangoStreamStore(), PATH, {"submission": covered_ref}, label="x")

        report = stream_coverage(
            SubmissionProjection.objects.all(),
            PATH,
            subject_key="submission",
            row_key="submission_id",
        )

        assert report.missing == 1
        assert report.missing_sample == (str(missing_ref),)
        assert report.extra == 0

    def test_events_on_another_stream_do_not_cover(self):
        (row,) = _rows(1)
        _about(row.pk, path="submissions/other")

        report = stream_coverage(
            CoverageRow.objects.all(), PATH, subject_key="submission"
        )

        assert report.missing == 1

    def test_the_queryset_filter_is_respected(self):
        _rows(2, form="a")
        _rows(3, form="b")

        report = stream_coverage(
            CoverageRow.objects.filter(form="b"), PATH, subject_key="submission"
        )

        assert report.rows == 3
        assert report.missing == 3


# ---------------------------------------------------------------------------
# stale
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestStale:
    def test_a_row_changed_after_its_newest_event_is_stale(self):
        changed_late, changed_early, never_changed = _rows(3)
        for row in (changed_late, changed_early, never_changed):
            _about(row.pk, event_ts=T0.timestamp())
        CoverageRow.objects.filter(pk=changed_late.pk).update(
            updated_at=T0 + timedelta(hours=1)
        )
        CoverageRow.objects.filter(pk=changed_early.pk).update(
            updated_at=T0 - timedelta(hours=1)
        )

        report = stream_coverage(
            CoverageRow.objects.all(),
            PATH,
            subject_key="submission",
            changed_field="updated_at",
        )

        assert report.stale == 1
        assert report.stale_sample == (str(changed_late.pk),)
        assert report.missing == 0

    def test_the_newest_event_is_the_one_compared(self):
        (row,) = _rows(1, updated_at=T0 + timedelta(hours=1))
        _about(row.pk, event_ts=T0.timestamp())
        _about(row.pk, event_ts=(T0 + timedelta(hours=2)).timestamp())

        report = stream_coverage(
            CoverageRow.objects.all(),
            PATH,
            subject_key="submission",
            changed_field="updated_at",
        )

        assert report.stale == 0

    def test_an_event_without_event_ts_falls_back_to_created_at(self):
        late, early = _rows(2)
        _raw_event({"submission": late.pk}, created_at=T0)
        _raw_event({"submission": early.pk}, created_at=T0)
        CoverageRow.objects.filter(pk=late.pk).update(
            updated_at=T0 + timedelta(hours=1)
        )
        CoverageRow.objects.filter(pk=early.pk).update(
            updated_at=T0 - timedelta(hours=1)
        )

        report = stream_coverage(
            CoverageRow.objects.all(),
            PATH,
            subject_key="submission",
            changed_field="updated_at",
        )

        assert report.stale_sample == (str(late.pk),)

    def test_event_ts_wins_over_created_at_in_both_directions(self):
        # `ahead` has a logical time after its append time, `backfilled` one
        # before it. Both rows changed at T0; only the backfilled one has no
        # event at or after that, once `event_ts` is read in place of the
        # append time.
        ahead, backfilled = _rows(2, updated_at=T0)
        _raw_event(
            {"submission": ahead.pk},
            created_at=T0 - timedelta(hours=1),
            event_ts=(T0 + timedelta(hours=1)).timestamp(),
        )
        _raw_event(
            {"submission": backfilled.pk},
            created_at=T0 + timedelta(hours=1),
            event_ts=(T0 - timedelta(hours=1)).timestamp(),
        )

        report = stream_coverage(
            CoverageRow.objects.all(),
            PATH,
            subject_key="submission",
            changed_field="updated_at",
        )

        assert report.stale_sample == (str(backfilled.pk),)

    @pytest.mark.parametrize("newer", ["hex", "hyphenated"])
    def test_a_subject_spelt_two_ways_is_one_subject(self, newer):
        # A UUID written hyphenated by one producer and as bare hex by another
        # names the same row; its newest event is the newest across both. Both
        # orders, because the database returns the two spellings' groups in a
        # fixed order and only one of them would catch the later group winning.
        ref = uuid.uuid4()
        SubmissionProjection.objects.create(submission_id=ref)
        new_ts, old_ts = T0.timestamp(), (T0 - timedelta(hours=2)).timestamp()
        _raw_event(
            {"submission": ref.hex}, event_ts=new_ts if newer == "hex" else old_ts
        )
        _raw_event(
            {"submission": str(ref)},
            event_ts=new_ts if newer == "hyphenated" else old_ts,
        )
        changed = SubmissionProjection.objects.annotate(
            changed=Value(T0 - timedelta(hours=1), output_field=DateTimeField())
        )

        report = stream_coverage(
            changed,
            PATH,
            subject_key="submission",
            row_key="submission_id",
            changed_field="changed",
        )

        assert report.missing == 0
        assert report.stale == 0
        assert report.extra == 0

    def test_a_missing_row_is_not_also_stale(self):
        _rows(1, updated_at=T0)

        report = stream_coverage(
            CoverageRow.objects.all(),
            PATH,
            subject_key="submission",
            changed_field="updated_at",
        )

        assert report.missing == 1
        assert report.stale == 0

    def test_the_change_time_may_be_an_annotation_over_a_related_table(self):
        late, early, untouched = _rows(3)
        for row in (late, early, untouched):
            _about(row.pk, event_ts=T0.timestamp())
        CoverageRowChange.objects.create(row=late, changed_at=T0 - timedelta(hours=3))
        CoverageRowChange.objects.create(row=late, changed_at=T0 + timedelta(hours=1))
        CoverageRowChange.objects.create(row=early, changed_at=T0 - timedelta(hours=1))

        report = stream_coverage(
            annotated_rows(),
            PATH,
            subject_key="submission",
            changed_field="last_changed",
        )

        assert report.rows == 3
        assert report.missing == 0
        assert report.stale == 1
        assert report.stale_sample == (str(late.pk),)


# ---------------------------------------------------------------------------
# extra, skipped
# ---------------------------------------------------------------------------


@pytest.mark.django_db
class TestExtraAndSkipped:
    def test_events_for_a_deleted_row_are_extra(self):
        (row,) = _rows(1)
        gone = CoverageRow.objects.create()
        _about(row.pk)
        _about(gone.pk)
        _about(gone.pk)
        gone_pk = gone.pk
        gone.delete()

        report = stream_coverage(
            CoverageRow.objects.all(), PATH, subject_key="submission"
        )

        assert report.missing == 0
        assert report.extra == 1
        assert report.extra_sample == (str(gone_pk),)
        assert report.ok

    def test_a_non_json_event_is_skipped_and_counted(self):
        # The encoding decides, not the shape: a `utf-8` event is not JSON even
        # if its stored value looks like an object naming the row.
        (row,) = _rows(1)
        _raw_event({"submission": row.pk}, payload_encoding="utf-8")
        _raw_event("plain text", payload_encoding="utf-8")

        report = stream_coverage(
            CoverageRow.objects.all(), PATH, subject_key="submission"
        )

        assert report.skipped == 2
        assert report.missing == 1

    @pytest.mark.parametrize("key", ["a__b", "contains", "exact", "in", "sub-id"])
    def test_an_unusual_subject_key_is_taken_literally(self, key):
        # The null exclusion and the extraction must read the same key; a
        # Django lookup string would treat `a__b` as a path and `contains` as a
        # lookup.
        (row,) = _rows(1)
        _raw_event({key: row.pk})
        _raw_event({key: None})

        report = stream_coverage(CoverageRow.objects.all(), PATH, subject_key=key)

        assert (report.missing, report.extra) == (0, 0)

    def test_a_json_null_subject_names_no_row(self):
        # SQLite renders a JSON null as the text "null" and Postgres as NULL;
        # neither is a subject, so both report no extra.
        _raw_event({"submission": None})

        report = stream_coverage(
            CoverageRow.objects.all(), PATH, subject_key="submission"
        )

        assert report.extra == 0
        assert report.extra_sample == ()


# ---------------------------------------------------------------------------
# the queryset's alias
# ---------------------------------------------------------------------------


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
def test_reads_rows_and_events_through_the_querysets_alias():
    rows = _rows(3, using="overlay")
    _about(rows[0].pk, using="overlay")
    _about(rows[1].pk, using="overlay")
    # `default` holds events that would cover every row, and one extra. Reading
    # events from the wrong alias turns the overlay's gap into a clean report.
    for row in rows:
        _about(row.pk, using="default")
    _about(10_000, using="default")

    with CaptureQueriesContext(connections["default"]) as on_default:
        report = stream_coverage(
            CoverageRow.objects.using("overlay"),
            PATH,
            subject_key="submission",
            changed_field="updated_at",
        )

    assert report.rows == 3
    assert report.missing == 1
    assert report.missing_sample == (str(rows[2].pk),)
    assert report.extra == 0
    assert len(on_default.captured_queries) == 0


# ---------------------------------------------------------------------------
# cost
# ---------------------------------------------------------------------------


@pytest.mark.django_db
def test_query_count_does_not_grow_with_the_table():
    def queries_for(n: int) -> int:
        CoverageRow.objects.all().delete()
        rows = _rows(n, updated_at=T0 + timedelta(hours=1))
        for row in rows[: n // 2]:
            _about(row.pk, event_ts=T0.timestamp())
        _about(10_000 + n)
        with CaptureQueriesContext(connections["default"]) as ctx:
            report = stream_coverage(
                CoverageRow.objects.all(),
                PATH,
                subject_key="submission",
                changed_field="updated_at",
            )
        assert report.missing > 0
        assert report.stale
        assert report.extra
        # The snapshot transaction adds a savepoint pair here, because the test
        # already runs inside a transaction; count the reads, not those.
        return sum(
            not q["sql"].lstrip().upper().startswith(("SAVEPOINT", "RELEASE"))
            for q in ctx.captured_queries
        )

    small = queries_for(3)
    large = queries_for(40)

    assert small == large
    assert small == 3


# ---------------------------------------------------------------------------
# the command
# ---------------------------------------------------------------------------


def _entry(**overrides):
    entry = {
        "model": "test_django_rakaia.CoverageRow",
        "stream_path": PATH,
        "subject_key": "submission",
    }
    entry.update(overrides)
    return entry


def _run(*args):
    out = io.StringIO()
    call_command("check_stream_coverage", *args, stdout=out)
    return out.getvalue()


@pytest.mark.django_db
class TestCommand:
    def test_a_covered_stream_passes(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = [_entry()]
        (row,) = _rows(1)
        _about(row.pk)

        output = _run()

        assert PATH in output
        assert "missing=0" in output

    def test_a_missing_row_fails(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = [_entry()]
        _rows(1)

        with pytest.raises(CommandError):
            _run()

    def test_a_stale_row_fails(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = [_entry(changed_field="updated_at")]
        (row,) = _rows(1, updated_at=T0 + timedelta(hours=1))
        _about(row.pk, event_ts=T0.timestamp())

        with pytest.raises(CommandError):
            _run()

    def test_extra_events_do_not_fail(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = [_entry()]
        (row,) = _rows(1)
        _about(row.pk)
        _about(10_000)

        output = _run()

        assert "extra=1" in output

    def test_exit_status_is_non_zero_on_a_gap(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = [_entry()]
        _rows(1)

        with pytest.raises(SystemExit) as exc:
            check_stream_coverage.Command().run_from_argv(
                ["manage.py", "check_stream_coverage"]
            )

        assert exc.value.code != 0

    def test_json_output(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = [_entry()]
        rows = _rows(2)
        _about(rows[0].pk)
        out = io.StringIO()

        with pytest.raises(CommandError):
            call_command("check_stream_coverage", "--json", stdout=out)

        (result,) = json.loads(out.getvalue())
        assert result["stream_path"] == PATH
        assert result["rows"] == 2
        assert result["missing"] == 1
        assert result["missing_sample"] == [str(rows[1].pk)]
        assert result["ok"] is False

    def test_a_model_entry_applies_its_filter(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = [_entry(filter={"form": "b"})]
        _rows(2, form="a")
        (row,) = _rows(1, form="b")
        _about(row.pk)

        output = _run("--json")

        (result,) = json.loads(output)
        assert result["rows"] == 1
        assert result["missing"] == 0

    def test_a_queryset_factory_entry_supplies_an_annotation(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = [
            {
                "queryset": f"{__name__}.annotated_rows",
                "stream_path": PATH,
                "subject_key": "submission",
                "changed_field": "last_changed",
            }
        ]
        (row,) = _rows(1)
        _about(row.pk, event_ts=T0.timestamp())
        CoverageRowChange.objects.create(row=row, changed_at=T0 + timedelta(hours=1))
        out = io.StringIO()

        with pytest.raises(CommandError):
            call_command("check_stream_coverage", "--json", stdout=out)

        (result,) = json.loads(out.getvalue())
        assert result["stale"] == 1
        assert result["stale_sample"] == [str(row.pk)]

    def test_every_entry_is_reported(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = [_entry(), _entry(stream_path="other")]
        (row,) = _rows(1)
        _about(row.pk)
        out = io.StringIO()

        with pytest.raises(CommandError):
            call_command("check_stream_coverage", stdout=out)

        assert PATH in out.getvalue()
        assert "other" in out.getvalue()

    @pytest.mark.parametrize(
        ("entry", "match"),
        [
            ({"stream_path": PATH, "subject_key": "submission"}, "exactly one"),
            (_entry(queryset=f"{__name__}.annotated_rows"), "exactly one"),
            (_entry(subject_key=None), "missing 'subject_key'"),
            (_entry(colour="blue"), "unknown keys"),
            (_entry(model="nope.Nothing"), "no model"),
            (_entry(model=None, queryset="tests.no_such_module.rows"), "cannot import"),
        ],
    )
    def test_a_malformed_entry_is_refused(self, settings, entry, match):
        settings.RAKAIA_COVERAGE_CHECKS = [
            {k: v for k, v in entry.items() if v is not None}
        ]

        with pytest.raises(CommandError, match=match):
            _run()

    @pytest.mark.parametrize(
        "bad",
        [{"changed_field": "nope"}, {"row_key": "nope"}, {"filter": {"nope": 1}}],
    )
    def test_a_misspelt_field_is_refused_before_any_entry_runs(self, settings, bad):
        settings.RAKAIA_COVERAGE_CHECKS = [_entry(), _entry(**bad)]

        with (
            CaptureQueriesContext(connections["default"]) as ctx,
            pytest.raises(CommandError, match=r"RAKAIA_COVERAGE_CHECKS\[1\]"),
        ):
            _run()
        assert ctx.captured_queries == []

    def test_an_entry_that_can_match_nothing_is_checked_not_refused(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = [_entry(filter={"pk__in": []})]

        out = _run()

        assert "rows=0" in out

    def test_no_entries_is_an_error(self, settings):
        settings.RAKAIA_COVERAGE_CHECKS = []

        with pytest.raises(CommandError, match="RAKAIA_COVERAGE_CHECKS"):
            _run()


# ---------------------------------------------------------------------------
# one snapshot
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    connections["default"].vendor != "postgresql",
    reason="the isolation level is set on Postgres; run with RAKAIA_TEST_DB=postgres",
)
@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
def test_a_row_and_its_event_committed_mid_check_are_not_counted_missing():
    """The three reads see one snapshot of the queryset's database.

    Between reading the stream and reading the rows, another connection commits
    a new row together with its event. Under READ COMMITTED the row read sees
    the row but the stream read never saw its event, so it reports missing. On
    the overlay alias, so an `atomic()` bound to `default` would leave the reads
    in autocommit and fail the same way.
    """
    import threading

    rows = _rows(2, using="overlay")
    for row in rows:
        _about(row.pk, using="overlay")

    queryset = CoverageRow.objects.using("overlay")
    original = queryset.order_by

    def commit_elsewhere_then_order(*args):
        def writer():
            try:
                (late,) = _rows(1, using="overlay")
                _about(late.pk, using="overlay")
            finally:
                connections.close_all()

        thread = threading.Thread(target=writer)
        thread.start()
        thread.join(timeout=10)
        return original(*args)

    queryset.order_by = commit_elsewhere_then_order  # type: ignore[method-assign]

    report = stream_coverage(queryset, PATH, subject_key="submission")

    assert CoverageRow.objects.using("overlay").count() == 3
    assert (report.rows, report.missing) == (2, 0)
