"""Tests for `manage.py prune_orphan_events` (#291).

An orphan is an event no entry in any stream points at. Deleting a stream now
removes its own orphans; this command is for the ones left behind before that,
or by anything that removed entries some other way. What matters most is what it
keeps: a dry run keeps everything, and an event still in one stream is kept.
"""

from __future__ import annotations

import io

import pytest
from django.core.management import call_command
from django.db import transaction

from django_rakaia.django_store import DjangoStreamStore, write_enveloped_event
from django_rakaia.models import Stream, StreamEntry, StreamEvent


def _orphan(n: int, using: str = "default") -> int:
    return (
        StreamEvent.objects.using(using).create(data={"n": n}, event_type="append").pk
    )


def _kept(using: str = "default") -> set[int]:
    """Events still referenced: one in a stream, and one fanned into two streams
    of which one has since lost its entry."""
    store = DjangoStreamStore(using=using)
    store.create("a")
    store.create("b")
    store.append("a", b'{"in": "a"}')
    with transaction.atomic(using=using):
        streams = list(
            Stream.objects.using(using)
            .select_for_update()
            .filter(stream_id__in=["a", "b"])
            .order_by("stream_id")
        )
        shared, _ = write_enveloped_event(streams, {"shared": True})
    StreamEntry.objects.using(using).filter(
        event=shared, stream__stream_id="a"
    ).delete()
    return set(StreamEvent.objects.using(using).values_list("pk", flat=True))


def _all(using: str = "default") -> set[int]:
    return set(StreamEvent.objects.using(using).values_list("pk", flat=True))


@pytest.mark.django_db
class TestPruneOrphanEventsCommand:
    def test_a_dry_run_changes_nothing_and_reports_the_count(self):
        kept = _kept()
        orphans = {_orphan(1), _orphan(2)}

        out = io.StringIO()
        call_command("prune_orphan_events", "--dry-run", stdout=out)

        assert _all() == kept | orphans
        assert "DRY RUN" in out.getvalue()
        assert "orphans=2" in out.getvalue()

    def test_the_real_run_removes_exactly_the_orphans(self):
        kept = _kept()
        _orphan(1)
        _orphan(2)
        _orphan(3)

        out = io.StringIO()
        call_command("prune_orphan_events", "--batch-size", "2", stdout=out)

        assert _all() == kept
        assert "DELETED" in out.getvalue()
        assert "orphans=3" in out.getvalue()

    def test_nothing_to_prune_is_a_clean_run(self):
        kept = _kept()
        out = io.StringIO()
        call_command("prune_orphan_events", stdout=out)
        assert _all() == kept
        assert "orphans=0" in out.getvalue()

    def test_no_payload_column_is_read(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        _kept()
        for n in range(5):
            _orphan(n)

        with CaptureQueriesContext(connection) as ctx:
            call_command(
                "prune_orphan_events", "--batch-size", "2", stdout=io.StringIO()
            )

        sql = [q["sql"] for q in ctx.captured_queries]
        assert [q for q in sql if '"data"' in q or '"metadata"' in q] == []

    def test_a_batch_size_below_one_is_refused(self):
        from django.core.management.base import CommandError

        _orphan(1)
        with pytest.raises(CommandError):
            call_command(
                "prune_orphan_events", "--batch-size", "0", stdout=io.StringIO()
            )
        assert len(_all()) == 1


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
def test_the_database_option_prunes_that_alias_only():
    kept = _kept("overlay")
    _orphan(1, "overlay")
    on_default = _orphan(2)

    call_command("prune_orphan_events", "--database", "overlay", stdout=io.StringIO())

    assert _all("overlay") == kept
    assert _all() == {on_default}
