"""Tags and a correlation id on durable events: where they are stored, how they
are found, and what still has to fail loudly around them.

What every store must do with them on read is in `tests/store_contract.py`. This
file covers what only the database has: the tag table and the column, the two
lookups, `append_event`, the event listing endpoint and the admin.
"""

from __future__ import annotations

import json

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from django.db.models import ProtectedError
from django.test import Client

from django_rakaia import append_event
from django_rakaia.admin import StreamPrefixListFilter
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


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
class TestOnAnotherDatabase:
    """Tag rows go to the store's database, not `default`, on both write paths."""

    def test_append_writes_its_tags_there(self):
        store = DjangoStreamStore(using="overlay")
        result = _append(store, "s", {"a": 1}, tags=("loss",))

        assert list(
            StreamEventTag.objects.using("overlay")
            .filter(event_id=result.event_id)
            .values_list("tag", flat=True)
        ) == ["loss"]
        assert not StreamEventTag.objects.using("default").exists()

    def test_append_many_writes_its_tags_there(self):
        store = DjangoStreamStore(using="overlay")
        store.create("s")
        (result,) = store.append_many(
            "s", [(b'{"a": 1}', AppendOptions(tags=("audit",)))]
        )

        assert list(
            StreamEventTag.objects.using("overlay")
            .filter(event_id=result.event_id)
            .values_list("tag", flat=True)
        ) == ["audit"]
        assert not StreamEventTag.objects.using("default").exists()


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


@pytest.fixture
def client(db) -> Client:  # noqa: ARG001 - needs the database
    client = Client()
    client.force_login(User.objects.create_user("reader"))
    return client


@pytest.mark.django_db
class TestTheEventListing:
    URL = "/streams/api/events/"

    @pytest.fixture(autouse=True)
    def events(self, store):
        self.loss = _append(
            store, "ida/tf611/1", {"a": 1}, tags=("loss",), correlation_id="INC-5"
        ).event_id
        self.audit = _append(
            store, "ida/tf611/2", {"a": 2}, tags=("loss", "audit"), label="incident"
        ).event_id
        self.other = _append(store, "chat/room", {"a": 3}).event_id

    def _ids(self, client, **params):
        """The ids listed, ignoring events this fixture did not write: the test
        app streams a save of a `User`, so logging the client in adds some."""
        response = client.get(self.URL, params)
        assert response.status_code == 200, response.content
        ours = {self.loss, self.audit, self.other}
        return [e["id"] for e in response.json()["events"] if e["id"] in ours]

    def test_unfiltered_lists_everything_oldest_first(self, client):
        assert self._ids(client) == [self.loss, self.audit, self.other]

    def test_each_filter(self, client):
        assert self._ids(client, tag="loss") == [self.loss, self.audit]
        assert self._ids(client, tag=["loss", "audit"]) == [self.audit]
        assert self._ids(client, correlation_id="INC-5") == [self.loss]
        assert self._ids(client, label="incident") == [self.audit]
        assert self._ids(client, stream_prefix="ida/") == [self.loss, self.audit]

    def test_since_and_until(self, client):
        assert self._ids(client, since="2999-01-01") == []
        assert self._ids(client, since="2000-01-01") == [
            self.loss,
            self.audit,
            self.other,
        ]
        assert self._ids(client, until="2000-01-01T00:00:00+00:00") == []
        # A trailing `Z`. Only Python 3.10 rejects it without the view's own
        # handling, so this can go red only on 3.10; later versions pass anyway.
        assert self._ids(client, until="2000-01-01T00:00:00Z") == []
        assert self._ids(client, until="2000-01-01T00:00:00z") == []
        assert self._ids(client, since="2000-01-01T00:00:00Z") == [
            self.loss,
            self.audit,
            self.other,
        ]

    def test_paging(self, client):
        two = client.get(self.URL, {"tag": "loss", "limit": 2}).json()
        assert [e["id"] for e in two["events"]] == [self.loss, self.audit]
        assert two["next_after_id"] == self.audit  # the last on the page

        params = {"stream_prefix": "ida/", "limit": 1}
        first = client.get(self.URL, params).json()
        assert [e["id"] for e in first["events"]] == [self.loss]
        assert first["has_more"] is True

        rest = client.get(
            self.URL, {**params, "limit": 2, "after_id": first["next_after_id"]}
        ).json()
        assert [e["id"] for e in rest["events"]] == [self.audit]
        assert rest["has_more"] is False

    def test_an_event_says_its_tags_correlation_and_streams(self, client):
        (event,) = client.get(self.URL, {"correlation_id": "INC-5"}).json()["events"]

        assert event["tags"] == ["loss"]
        assert event["correlation_id"] == "INC-5"
        assert event["streams"] == ["ida/tf611/1"]

    def test_a_page_is_at_most_200(self, store, client):
        store.create("ida/bulk")
        store.append_many(
            "ida/bulk", [(b'{"a": 1}', AppendOptions(tags=("bulk",)))] * 201
        )

        page = client.get(self.URL, {"tag": "bulk", "limit": 500}).json()

        assert page["count"] == 200
        assert page["has_more"] is True

    @pytest.mark.parametrize(
        "params", [{"limit": "x"}, {"after_id": "x"}, {"since": "yesterday"}]
    )
    def test_a_bad_parameter_is_a_400(self, client, params):
        assert client.get(self.URL, params).status_code == 400

    def test_it_needs_a_login(self):
        assert Client().get(self.URL).status_code == 302


@pytest.mark.django_db
class TestTheStreamListingFilters:
    def test_tag_and_label(self, store, client):
        _append(store, "s", {"a": 1}, tags=("loss",), label="incident")
        _append(store, "s", {"a": 2}, tags=("loss",))
        _append(store, "s", {"a": 3}, label="incident")

        def offsets(**params):
            return [
                e["offset"]
                for e in client.get("/streams/api/streams/s/", params).json()["events"]
            ]

        assert offsets(tag="loss") == [1, 2]
        assert offsets(label="incident") == [1, 3]
        assert offsets(tag="loss", label="incident") == [1]


@pytest.mark.django_db
class TestTheAdmin:
    @pytest.fixture
    def admin(self, db) -> Client:  # noqa: ARG002 - needs the database
        client = Client()
        client.force_login(User.objects.create_superuser("admin"))
        return client

    def test_the_changelist_filters_by_tag_and_stream_prefix(self, store, admin):
        _append(store, "ida/tf611/1", {"a": 1}, tags=("loss",), correlation_id="INC-6")
        _append(store, "chat/room", {"a": 2})
        url = "/admin/django_rakaia/streamevent/"

        assert admin.get(url).status_code == 200
        tagged = admin.get(url, {"tag": "loss"})
        assert tagged.context["cl"].result_count == 1
        changelist = admin.get(url).context["cl"]
        (prefix_filter,) = (
            spec
            for spec in changelist.filter_specs
            if isinstance(spec, StreamPrefixListFilter)
        )
        offered = {value for value, _ in prefix_filter.lookup_choices}
        assert {"ida/", "chat/"} <= offered
        prefixed = admin.get(url, {"stream_prefix": "chat/"})
        assert prefixed.context["cl"].result_count == 1
        searched = admin.get(url, {"q": "INC-6"})
        assert searched.context["cl"].result_count == 1

    def test_the_change_page_shows_the_envelope(self, store, admin):
        result = _append(store, "s", {"a": 1}, tags=("loss",), correlation_id="INC-7")

        page = admin.get(f"/admin/django_rakaia/streamevent/{result.event_id}/change/")

        assert page.status_code == 200
        assert b"INC-7" in page.content
        assert b"loss" in page.content


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

    def test_a_nul_in_the_ambient_correlation_is_refused_before_the_save(self):
        """Postgres refuses a NUL only once the event is being written, after
        the row it audits has been saved; SQLite would store it. Refusing it on
        entry is what keeps the two databases agreeing."""
        with pytest.raises(ValueError), provenance(correlation="a\x00b"):
            Area.objects.create(name="north")

        assert not Area.objects.exists()
        assert not StreamEvent.objects.exists()

    def test_pruning_it_as_an_orphan_raises(self):
        orphan = StreamEvent.objects.create(data={"a": 1}, event_type="incident")
        IncidentRecord.objects.create(event=orphan)

        with pytest.raises(ProtectedError):
            call_command("prune_orphan_events")

        assert StreamEvent.objects.filter(pk=orphan.pk).exists()
