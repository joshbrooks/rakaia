"""`RAKAIA_PERMANENT_STREAMS`, and deleting a stream's orphaned events (#291).

With the switch on, nothing a client does and nothing a clock does can make a
durable stream's history disappear: a create carrying an expiry is refused, a
protocol DELETE is refused, and a stream that already had an expiry is served as
live rather than reaped. A deliberate `DjangoStreamStore.delete()` from Python
still works — it is how an application rebuilds a stream on purpose.

Separately, and whether the switch is on or off, `delete()` removes the events
no remaining entry in *any* stream points at. The two-stream fan-out case is the
one that matters: an event shared with a surviving stream must be kept.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
import pytest_asyncio
from asgiref.sync import sync_to_async
from django.db import connection, transaction
from django.db.models.signals import pre_delete
from django.test.utils import CaptureQueriesContext

from django_rakaia.django_store import DjangoStreamStore, write_enveloped_event
from django_rakaia.models import Stream, StreamEntry, StreamEvent
from rakaia.types import DeleteNotAllowed, ExpiryNotAllowed
from tests.asgi_client import asgi_client

JSON = {"content-type": "application/json"}


@pytest.fixture
def permanent(settings) -> None:
    settings.RAKAIA_PERMANENT_STREAMS = True


def _expired(store: DjangoStreamStore, path: str) -> None:
    """Leave an already-expired stream at `path`, created before the switch."""
    store.create(path, content_type="application/json", ttl_seconds=1)
    store.append(path, b'{"id": 1}')
    Stream.objects.filter(stream_id=path).update(last_activity_at=time.time() - 60)


@pytest_asyncio.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with asgi_client(DjangoStreamStore()) as ac:
        yield ac


# =============================================================================
# Switch on: the refusals
# =============================================================================


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("permanent")
class TestThePermanentStore:
    def test_a_create_with_a_ttl_is_refused(self):
        with pytest.raises(ExpiryNotAllowed):
            DjangoStreamStore().create("s", ttl_seconds=60)
        assert not Stream.objects.filter(stream_id="s").exists()

    def test_a_create_with_an_expiry_is_refused(self):
        with pytest.raises(ExpiryNotAllowed):
            DjangoStreamStore().create("s", expires_at="2099-01-01T00:00:00Z")
        assert not Stream.objects.filter(stream_id="s").exists()

    def test_a_create_without_an_expiry_still_works(self):
        store = DjangoStreamStore()
        store.create("s", initial_data=b'{"id": 1}')
        assert [m.data for m in store.read("s")[0]] == [b'{"id": 1}']

    def test_a_plain_python_delete_is_refused(self):
        """The hand-run `--reset` this switch exists to stop is a Python call.

        Until #310 the switch refused a client's DELETE and carried out a
        Python one, on the reading that a call from Python is an operator's
        decision. A management command run by hand on a pod is a Python call
        too, and is exactly the operation someone switching this on assumes it
        covers, so the decision now has to be expressed rather than assumed.
        """
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')
        with pytest.raises(DeleteNotAllowed):
            store.delete("s")

    def test_a_refused_delete_leaves_the_stream_and_its_events(self):
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')
        with pytest.raises(DeleteNotAllowed):
            store.delete("s")
        assert store.has("s")
        assert [m.data for m in store.read("s")[0]] == [b'{"id": 1}']
        assert StreamEvent.objects.count() == 1

    def test_a_forced_delete_still_works(self):
        """Application code rebuilding a stream on purpose relies on this."""
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')
        assert store.delete("s", force=True) is True
        assert not store.has("s")
        assert StreamEvent.objects.count() == 0

    def test_forcing_a_delete_of_a_missing_stream_is_still_false(self):
        assert DjangoStreamStore().delete("never-existed", force=True) is False

    def test_an_expired_stream_is_read_not_reaped(self, settings):
        settings.RAKAIA_PERMANENT_STREAMS = False
        store = DjangoStreamStore()
        _expired(store, "s")
        settings.RAKAIA_PERMANENT_STREAMS = True

        assert store.has("s")
        assert store.get("s") is not None
        assert [m.data for m in store.read("s")[0]] == [b'{"id": 1}']
        assert Stream.objects.filter(stream_id="s").exists()

    def test_an_expired_stream_still_takes_appends(self, settings):
        """The write path's expiry check is the other one that reaps."""
        settings.RAKAIA_PERMANENT_STREAMS = False
        store = DjangoStreamStore()
        _expired(store, "s")
        settings.RAKAIA_PERMANENT_STREAMS = True

        store.append("s", b'{"id": 2}')
        assert [m.data for m in store.read("s")[0]] == [b'{"id": 1}', b'{"id": 2}']

    def test_a_stream_given_a_ttl_earlier_recreates_without_one(self, settings):
        """It is treated as having no expiry, by `create` as well as by `get`."""
        settings.RAKAIA_PERMANENT_STREAMS = False
        store = DjangoStreamStore()
        store.create("a", ttl_seconds=3600)
        store.create("b", expires_at="2099-01-01T00:00:00Z")
        settings.RAKAIA_PERMANENT_STREAMS = True

        assert store.create("a").stream_id == "a"
        assert store.create("b").stream_id == "b"
        assert Stream.objects.count() == 2

    def test_the_setting_is_read_on_every_call(self, settings):
        store = DjangoStreamStore()
        with pytest.raises(ExpiryNotAllowed):
            store.create("s", ttl_seconds=60)
        settings.RAKAIA_PERMANENT_STREAMS = False
        store.create("s", ttl_seconds=60)
        assert store.has("s")


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("permanent")
class TestThePermanentStoreOverTheProtocol:
    async def test_a_put_with_a_ttl_is_a_400(self, client: httpx.AsyncClient):
        resp = await client.put("/s", headers={**JSON, "stream-ttl": "60"})
        assert resp.status_code == 400
        assert b"permanent" in resp.content
        assert not await sync_to_async(DjangoStreamStore().has)("/s")

    async def test_a_put_with_an_expiry_is_a_400(self, client: httpx.AsyncClient):
        resp = await client.put(
            "/s", headers={**JSON, "stream-expires-at": "2099-01-01T00:00:00Z"}
        )
        assert resp.status_code == 400
        assert b"permanent" in resp.content
        assert not await sync_to_async(DjangoStreamStore().has)("/s")

    async def test_a_plain_put_is_still_created(self, client: httpx.AsyncClient):
        assert (await client.put("/s", headers=JSON)).status_code == 201

    async def test_a_delete_is_a_405_and_the_stream_survives(
        self, client: httpx.AsyncClient
    ):
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"id": 1}', headers=JSON)

        resp = await client.delete("/s")

        assert resp.status_code == 405
        assert "DELETE" not in resp.headers["allow"]
        assert (await client.get("/s")).json() == [{"id": 1}]

    async def test_a_delete_of_a_missing_stream_is_still_a_404(
        self, client: httpx.AsyncClient
    ):
        assert (await client.delete("/nope")).status_code == 404

    async def test_an_expired_stream_no_longer_advertises_its_expiry(
        self, client: httpx.AsyncClient, settings
    ):
        """Served as if it had none, so a client is not told it will lapse."""
        settings.RAKAIA_PERMANENT_STREAMS = False
        await sync_to_async(_expired)(DjangoStreamStore(), "/ttl")
        await sync_to_async(DjangoStreamStore().create)(
            "/abs", expires_at="2000-01-01T00:00:00Z"
        )
        settings.RAKAIA_PERMANENT_STREAMS = True

        for path in ("/ttl", "/abs"):
            head = await client.head(path)
            assert head.status_code == 200
            assert "stream-ttl" not in head.headers
            assert "stream-expires-at" not in head.headers

    async def test_a_stream_given_a_ttl_earlier_takes_a_plain_put(
        self, client: httpx.AsyncClient, settings
    ):
        settings.RAKAIA_PERMANENT_STREAMS = False
        assert (
            await client.put("/s", headers={**JSON, "stream-ttl": "3600"})
        ).status_code == 201
        settings.RAKAIA_PERMANENT_STREAMS = True

        assert "stream-ttl" not in (await client.head("/s")).headers
        assert (await client.put("/s", headers=JSON)).status_code == 200

    async def test_an_expired_stream_is_served(
        self, client: httpx.AsyncClient, settings
    ):
        settings.RAKAIA_PERMANENT_STREAMS = False
        # The protocol's stream id is the request path, leading slash included.
        await sync_to_async(_expired)(DjangoStreamStore(), "/s")
        settings.RAKAIA_PERMANENT_STREAMS = True

        resp = await client.get("/s")

        assert resp.status_code == 200
        assert resp.json() == [{"id": 1}]


# =============================================================================
# Switch off: unchanged
# =============================================================================


@pytest.mark.django_db(transaction=True)
class TestTheDefaultIsUnchanged:
    def test_the_switch_is_off_by_default(self):
        from django.conf import settings

        assert not hasattr(settings, "RAKAIA_PERMANENT_STREAMS")

    def test_a_delete_needs_no_force_while_the_switch_is_off(self):
        store = DjangoStreamStore()
        store.create("s")
        assert store.delete("s") is True

    def test_force_is_accepted_and_changes_nothing_while_the_switch_is_off(self):
        """So a caller may pass it unconditionally rather than branch on a setting."""
        store = DjangoStreamStore()
        store.create("s")
        assert store.delete("s", force=True) is True

    def test_a_create_with_an_expiry_is_accepted(self):
        store = DjangoStreamStore()
        store.create("a", ttl_seconds=60)
        store.create("b", expires_at="2099-01-01T00:00:00Z")
        assert store.has("a") and store.has("b")

    def test_an_expired_stream_is_reaped(self):
        store = DjangoStreamStore()
        _expired(store, "s")
        assert not store.has("s")
        assert not Stream.objects.filter(stream_id="s").exists()

    async def test_the_protocol_accepts_a_ttl_and_a_delete(
        self, client: httpx.AsyncClient
    ):
        resp = await client.put("/s", headers={**JSON, "stream-ttl": "60"})
        assert resp.status_code == 201
        assert (await client.delete("/s")).status_code == 204
        assert (await client.get("/s")).status_code == 404


# =============================================================================
# delete() removes orphaned events, on or off
# =============================================================================


def _fan_out(*paths: str, using: str | None = None) -> StreamEvent:
    """One event appearing in every one of `paths`, as `@stream_model` writes it."""
    with transaction.atomic(using=using):
        streams = list(
            Stream.objects.using(using)
            .select_for_update()
            .filter(stream_id__in=paths)
            .order_by("stream_id")
        )
        event, _entries = write_enveloped_event(streams, {"shared": True})
    return event


@pytest.fixture(params=[False, True], ids=["switch-off", "switch-on"])
def either_way(request, settings) -> None:
    settings.RAKAIA_PERMANENT_STREAMS = request.param


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("either_way")
class TestDeleteRemovesOrphanedEvents:
    """The orphan rule holds whether the switch is on or off.

    Every delete here passes `force=True` unconditionally, which is the
    call shape an application should use: it is inert while the switch is
    off, so the same line works either way without branching on a setting.
    """

    def test_a_deleted_streams_events_are_gone(self):
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')
        store.append("s", b'{"id": 2}')
        assert StreamEvent.objects.count() == 2

        store.delete("s", force=True)

        assert StreamEvent.objects.count() == 0

    def test_a_fanned_out_event_survives_deleting_one_of_its_streams(self):
        store = DjangoStreamStore()
        store.create("a")
        store.create("b")
        store.append("a", b'{"only": "a"}')
        shared = _fan_out("a", "b")

        store.delete("a", force=True)

        assert StreamEvent.objects.filter(pk=shared.pk).exists()
        assert [m.data for m in store.read("b")[0]] == [b'{"shared": true}']
        assert StreamEvent.objects.count() == 1, "a's own event should be gone"

        store.delete("b", force=True)

        assert not StreamEvent.objects.filter(pk=shared.pk).exists()

    def test_other_streams_events_are_untouched(self):
        store = DjangoStreamStore()
        store.create("a")
        store.create("b")
        store.append("a", b'{"id": 1}')
        store.append("b", b'{"id": 2}')

        store.delete("a", force=True)

        assert [m.data for m in store.read("b")[0]] == [b'{"id": 2}']
        assert StreamEvent.objects.count() == 1

    def test_an_event_that_was_already_orphaned_is_left_to_the_command(self):
        """`delete()` cleans up after its own stream, not the whole table."""
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')
        stray = StreamEvent.objects.create(data={"stray": True}, event_type="append")

        store.delete("s", force=True)

        assert list(StreamEvent.objects.values_list("pk", flat=True)) == [stray.pk]


def _stream_of(n: int, path: str = "s") -> DjangoStreamStore:
    store = DjangoStreamStore()
    store.create(path)
    store.append_many(path, [(b'{"big": "%d"}' % i, None) for i in range(n)])
    return store


def _delete_queries(store: DjangoStreamStore, path: str = "s") -> list[str]:
    with CaptureQueriesContext(connection) as ctx:
        store.delete(path)
    return [q["sql"] for q in ctx.captured_queries]


@pytest.mark.django_db(transaction=True)
class TestDeleteCost:
    """A delete must not load the stream's payloads, nor cost a query per event.

    A backfill reset deletes streams of many thousands of events, and so does
    every reap. The events are removed in fixed-size chunks of ids.
    """

    @pytest.fixture(autouse=True)
    def _small_chunks(self, monkeypatch):
        import django_rakaia.django_store as module

        monkeypatch.setattr(module, "_ORPHAN_DELETE_CHUNK", 10)

    def test_no_payload_column_is_read(self):
        store = _stream_of(25)
        queries = _delete_queries(store)
        assert StreamEvent.objects.count() == 0
        read_payload = [q for q in queries if '"data"' in q or '"metadata"' in q]
        assert read_payload == []

    def test_the_cost_grows_with_chunks_not_events(self):
        costs = {}
        for n in (11, 20, 40):
            StreamEvent.objects.all().delete()
            costs[n] = len(_delete_queries(_stream_of(n, f"s{n}"), f"s{n}"))
        # 11 and 20 events are both two chunks; 40 is four.
        assert costs[11] == costs[20]
        per_chunk = (costs[40] - costs[20]) / 2
        assert per_chunk > 0
        assert costs[40] - costs[20] == 2 * per_chunk
        assert costs[40] < 40, costs

    def test_the_ids_are_read_a_chunk_at_a_time(self):
        """The stream's ids are never held in one piece: one read per chunk."""
        import re

        reads = {}
        for n in (25, 45):
            queries = _delete_queries(_stream_of(n, f"s{n}"), f"s{n}")
            reads[n] = sum(
                1
                for q in queries
                if q.startswith("SELECT") and re.search(r'FROM "rakaia_streamentry"', q)
            )
        # Three chunks and a final empty read, then five and one.
        assert reads == {25: 4, 45: 6}

    def test_a_subclass_row_goes_with_its_event(self):
        from .models import AppStreamEvent

        store = DjangoStreamStore()
        store.create("s")
        stream = Stream.objects.get(stream_id="s")
        event = AppStreamEvent.objects.create(data={"x": 1}, event_type="append")
        StreamEntry.objects.create(stream=stream, event=event, offset=1)

        store.delete("s")

        assert not AppStreamEvent.objects.filter(pk=event.pk).exists()
        assert not StreamEvent.objects.filter(pk=event.pk).exists()


@pytest.mark.django_db(transaction=True)
def test_a_reap_removes_the_expired_streams_events_too():
    store = DjangoStreamStore()
    _expired(store, "s")
    assert not store.has("s")
    assert StreamEvent.objects.count() == 0
    assert StreamEntry.objects.count() == 0


class _Boom(Exception):
    pass


@pytest.fixture
def event_delete_fails() -> Iterator[None]:
    """Make deleting an event raise, once the first chunk's entries are gone.

    By then the delete has already removed entries, so a rollback has work to
    undo; the stream row, deleted last, must still be there afterwards.
    """

    def _raise(**_kwargs):
        raise _Boom

    pre_delete.connect(_raise, sender=StreamEvent, dispatch_uid="test-291-boom")
    try:
        yield
    finally:
        pre_delete.disconnect(sender=StreamEvent, dispatch_uid="test-291-boom")


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
class TestDeleteIsOneTransactionOnTheStoresAlias:
    def test_the_overlay_delete_removes_orphans_there_only(self):
        overlay, default = DjangoStreamStore(using="overlay"), DjangoStreamStore()
        for store in (overlay, default):
            store.create("s")
            store.append("s", b'{"id": 1}')

        overlay.delete("s")

        assert StreamEvent.objects.using("overlay").count() == 0
        assert StreamEvent.objects.using("default").count() == 1
        assert default.has("s")

    def test_the_overlay_fan_out_event_survives(self):
        overlay = DjangoStreamStore(using="overlay")
        overlay.create("a")
        overlay.create("b")
        shared = _fan_out("a", "b", using="overlay")

        overlay.delete("a")

        assert StreamEvent.objects.using("overlay").filter(pk=shared.pk).exists()

    @pytest.mark.usefixtures("event_delete_fails")
    def test_a_failed_overlay_delete_keeps_the_events(self):
        """The orphan delete and the stream delete commit together or not at all.

        Opened on the default alias instead, the overlay's stream delete runs in
        autocommit and is kept when the event delete then fails.
        """
        overlay = DjangoStreamStore(using="overlay")
        overlay.create("s")
        overlay.append("s", b'{"id": 1}')

        with pytest.raises(_Boom):
            overlay.delete("s")

        assert StreamEvent.objects.using("overlay").count() == 1
        assert [m.data for m in overlay.read("s")[0]] == [b'{"id": 1}']

    @pytest.mark.usefixtures("event_delete_fails")
    def test_a_failed_default_delete_keeps_the_events(self):
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')

        with pytest.raises(_Boom):
            store.delete("s")

        assert StreamEvent.objects.count() == 1
        assert [m.data for m in store.read("s")[0]] == [b'{"id": 1}']


def test_the_refusals_are_the_documented_statuses():
    from rakaia.protocol_server import STORE_FAILURE_STATUS

    assert STORE_FAILURE_STATUS[ExpiryNotAllowed][0] == 400
    assert STORE_FAILURE_STATUS[DeleteNotAllowed][0] == 405
