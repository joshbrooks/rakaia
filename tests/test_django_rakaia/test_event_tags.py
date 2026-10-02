"""Tags and a correlation id on durable events: where they are stored, how they
are found, and what still has to fail loudly around them.

What every store must do with them on read is in `tests/store_contract.py`. This
file covers what only the database has: the tag table and the column, the two
lookups, and `append_event`.
"""

from __future__ import annotations

import json

import pytest
from django.core.management import call_command
from django.db.models import ProtectedError

from django_rakaia import append_event
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import Stream, StreamEntry, StreamEvent, StreamEventTag
from rakaia import AppendOptions
from rakaia.context import provenance
from tests.test_django_rakaia.models import Area, IncidentRecord


@pytest.fixture
def store() -> DjangoStreamStore:
    return DjangoStreamStore()


def _append(store, path, payload, **options):
    store.create(path)
    return store.append(path, json.dumps(payload).encode(), AppendOptions(**options))


@pytest.mark.django_db
class TestWhereTheyAreStored:
    def test_each_tag_is_a_row_on_the_event(self, store):
        result = _append(store, "s", {"a": 1}, tags=("loss", "audit"))

        assert sorted(
            StreamEventTag.objects.filter(event_id=result.event_id).values_list(
                "tag", flat=True
            )
        ) == ["audit", "loss"]

    def test_the_correlation_id_is_a_column(self, store):
        result = _append(store, "s", {"a": 1}, correlation_id="INC-1")

        assert StreamEvent.objects.get(pk=result.event_id).correlation_id == "INC-1"

    def test_append_many_writes_each_items_tags_to_its_own_event(self, store):
        store.create("s")
        first, second = store.append_many(
            "s",
            [
                (b'{"a": 1}', AppendOptions(tags=("x",))),
                (b'{"a": 2}', AppendOptions(tags=("y",), correlation_id="c")),
            ],
        )

        assert list(
            StreamEvent.objects.get(pk=first.event_id).tags.values_list(
                "tag", flat=True
            )
        ) == ["x"]
        second_event = StreamEvent.objects.get(pk=second.event_id)
        assert list(second_event.tags.values_list("tag", flat=True)) == ["y"]
        assert second_event.correlation_id == "c"

    def test_reading_many_tagged_events_costs_no_query_per_event(
        self, store, django_assert_max_num_queries
    ):
        for i in range(20):
            _append(store, "s", {"i": i}, tags=("t", f"n{i}"))

        with django_assert_max_num_queries(6):
            messages, _ = store.read("s")

        assert len(messages) == 20
        assert messages[3].tags == ("n3", "t")

    def test_a_failed_append_leaves_no_tags_behind(self, store):
        options = AppendOptions(tags=("x",))
        options.correlation_id = "x" * 129
        store.create("s")
        with pytest.raises(ValueError):
            store.append("s", b'{"a": 1}', options)

        assert not StreamEventTag.objects.exists()
        assert not StreamEvent.objects.exists()


@pytest.mark.django_db
class TestFindingThem:
    def test_tagged_wants_every_tag(self, store):
        both = _append(store, "s", {"a": 1}, tags=("loss", "audit"))
        _append(store, "s", {"a": 2}, tags=("loss",))

        assert list(
            StreamEvent.objects.tagged("loss", "audit").values_list("pk", flat=True)
        ) == [both.event_id]
        assert StreamEvent.objects.tagged("loss").count() == 2
        # Either order: a filter on only the last tag given would match both.
        assert list(
            StreamEvent.objects.tagged("audit", "loss").values_list("pk", flat=True)
        ) == [both.event_id]

    def test_tagged_with_no_tags_is_refused(self):
        with pytest.raises(ValueError):
            StreamEvent.objects.tagged()

    def test_correlated(self, store):
        hit = _append(store, "s", {"a": 1}, correlation_id="INC-2")
        _append(store, "s", {"a": 2}, correlation_id="INC-3")
        _append(store, "s", {"a": 3})

        assert list(
            StreamEvent.objects.correlated("INC-2").values_list("pk", flat=True)
        ) == [hit.event_id]


@pytest.mark.django_db
class TestAppendEvent:
    def test_it_returns_the_event_id(self, store):
        result = append_event(store, "s", {"a": 1}, label="incident")

        assert StreamEvent.objects.get(pk=result.event_id).event_type == "incident"

    def test_it_passes_tags_correlation_and_metadata_through(self, store):
        result = append_event(
            store,
            "s",
            {"a": 1},
            label="incident",
            actor=7,
            metadata={"note": "restored from backup", "user": "ignored"},
            tags=("loss",),
            correlation_id="INC-4",
        )

        event = StreamEvent.objects.get(pk=result.event_id)
        assert event.metadata == {"note": "restored from backup", "user": 7}
        assert list(event.tags.values_list("tag", flat=True)) == ["loss"]
        assert event.correlation_id == "INC-4"

    def test_metadata_is_kept_without_an_actor(self, store):
        result = append_event(store, "s", {"a": 1}, label="x", metadata={"note": "n"})

        assert StreamEvent.objects.get(pk=result.event_id).metadata == {"note": "n"}

    def test_a_bad_tag_writes_nothing(self, store):
        with pytest.raises(ValueError):
            append_event(store, "s", {"a": 1}, label="x", tags=("",))

        assert not StreamEvent.objects.exists()


@pytest.mark.django_db(transaction=True)
class TestAProtectedEventStillStopsADelete:
    """A consumer may point at an event with `on_delete=PROTECT` to keep a record
    it must not lose. Removing that event has to raise, not carry on quietly, and
    the rows it would have removed must still be there afterwards."""

    def test_deleting_its_stream_raises_and_removes_nothing(self, store):
        result = _append(store, "s", {"a": 1}, tags=("loss",))
        IncidentRecord.objects.create(event_id=result.event_id)

        with pytest.raises(ProtectedError):
            store.delete("s")

        assert Stream.objects.filter(stream_id="s").exists()
        assert StreamEntry.objects.filter(stream__stream_id="s").count() == 1
        assert StreamEvent.objects.filter(pk=result.event_id).exists()
        assert StreamEventTag.objects.filter(event_id=result.event_id).exists()

    def test_a_model_save_under_a_bad_ambient_correlation_does_not_happen(self):
        """Refused when the block opens, so the save never runs, rather than
        saving the row and then failing to write its event in ``post_save``."""
        with pytest.raises(ValueError), provenance(correlation=5):
            Area.objects.create(name="north")

        assert not Area.objects.exists()
        assert not StreamEvent.objects.exists()

    def test_pruning_it_as_an_orphan_raises(self):
        orphan = StreamEvent.objects.create(data={"a": 1}, event_type="incident")
        IncidentRecord.objects.create(event=orphan)

        with pytest.raises(ProtectedError):
            call_command("prune_orphan_events")

        assert StreamEvent.objects.filter(pk=orphan.pk).exists()
