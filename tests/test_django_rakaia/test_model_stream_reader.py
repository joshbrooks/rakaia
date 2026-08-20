"""Tests for django_rakaia.streams readers."""

from __future__ import annotations

import json

import pytest
from django.db import transaction

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.models import Stream, StreamEntry, StreamEvent
from django_rakaia.streams import ModelStreamReader
from rakaia.effects import Upsert
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay

from .models import Area


def _to_payload(area: Area) -> dict:
    return {"id": area.id, "name": area.name}  # type: ignore[attr-defined]


@pytest.mark.django_db
class TestModelStreamReaderBasics:
    def test_read_returns_ordered_messages(self):
        Area.objects.create(name="A")
        Area.objects.create(name="B")
        Area.objects.create(name="C")

        reader = ModelStreamReader(
            queryset_for=lambda _path: Area.objects.all(),
            order_by="id",
            to_payload=_to_payload,
        )

        messages, up_to_date = reader.read("anything")

        assert up_to_date is True
        names = [json.loads(m.data)["name"] for m in messages]
        assert names == ["A", "B", "C"]

    def test_queryset_for_filters_by_path(self):
        Area.objects.create(name="A1")
        Area.objects.create(name="A2")
        Area.objects.create(name="B1")

        def qs_for(path: str):
            prefix = path.split(":")[-1]
            return Area.objects.filter(name__startswith=prefix)

        reader = ModelStreamReader(
            queryset_for=qs_for,
            order_by="id",
            to_payload=_to_payload,
        )

        messages_a, _ = reader.read("areas:A")
        messages_b, _ = reader.read("areas:B")

        assert {json.loads(m.data)["name"] for m in messages_a} == {"A1", "A2"}
        assert {json.loads(m.data)["name"] for m in messages_b} == {"B1"}

    def test_empty_queryset(self):
        reader = ModelStreamReader(
            queryset_for=lambda _path: Area.objects.none(),
            order_by="id",
            to_payload=_to_payload,
        )
        messages, up_to_date = reader.read("anything")
        assert messages == []
        assert up_to_date is True

    def test_iter_payloads(self):
        Area.objects.create(name="A")
        Area.objects.create(name="B")
        reader = ModelStreamReader(
            queryset_for=lambda _path: Area.objects.all(),
            order_by="id",
            to_payload=_to_payload,
        )
        payloads = list(reader.iter_payloads("anything"))
        assert [p["name"] for p in payloads] == ["A", "B"]


@pytest.mark.django_db
class TestReplayAgainstModelStream:
    """End-to-end: replay() driven by a ModelStreamReader produces effects
    that the DjangoExecutor applies to a different model."""

    def test_replay_through_virtual_stream(self):
        # Source: rows in Area, used as the virtual event stream.
        for n in ["alpha", "beta", "gamma"]:
            Area.objects.create(name=n)

        # Handler upserts into a different "downstream" Area row (prefixed)
        # — using the same model to avoid adding a new migration.
        reg = HandlerRegistry()

        def project_handler(event):
            return Upsert(
                model_label="test_django_rakaia.Area",
                lookup={"name": f"projected:{event['name']}"},
                defaults={},
            )

        reg.register("project", "virt", project_handler, 0, None)

        reader = ModelStreamReader(
            queryset_for=lambda _path: Area.objects.exclude(
                name__startswith="projected:"
            ),
            order_by="id",
            to_payload=_to_payload,
        )

        result = replay(
            reader,  # type: ignore[arg-type]
            "virt",
            DjangoExecutor(),
            handler_registry=reg,
        )

        assert result.events_processed == 3
        assert result.effects_applied == 3
        for n in ["alpha", "beta", "gamma"]:
            assert Area.objects.filter(name=f"projected:{n}").exists()

    def test_replay_is_idempotent(self):
        Area.objects.create(name="once")
        reg = HandlerRegistry()

        def h(event):
            return Upsert(
                model_label="test_django_rakaia.Area",
                lookup={"name": f"projected:{event['name']}"},
                defaults={},
            )

        reg.register("h", "virt", h, 0, None)
        reader = ModelStreamReader(
            queryset_for=lambda _path: Area.objects.exclude(
                name__startswith="projected:"
            ),
            order_by="id",
            to_payload=_to_payload,
        )

        replay(reader, "virt", DjangoExecutor(), handler_registry=reg)  # type: ignore[arg-type]
        replay(reader, "virt", DjangoExecutor(), handler_registry=reg)  # type: ignore[arg-type]

        assert Area.objects.filter(name="projected:once").count() == 1


# ---------------------------------------------------------------------------
# Stored events: read them with the store, not with a second reader
# ---------------------------------------------------------------------------


def _octet_stream():
    """`AppendOptions` declaring a non-JSON content type, so the body is stored raw."""
    from rakaia import AppendOptions

    return AppendOptions(content_type="application/octet-stream")


def _append_event(stream_id: str, data: dict) -> None:
    """Helper: write one event to a Django-backed stream."""
    with transaction.atomic():
        stream, _ = Stream.objects.get_or_create(stream_id=stream_id)
        event = StreamEvent.objects.create(data=data, event_type="test")
        next_offset = stream.get_next_offset()
        StreamEntry.objects.create(stream=stream, event=event, offset=next_offset)


@pytest.mark.django_db
class TestTheStoreIsTheReaderForStoredEvents:
    """Why `DjangoStreamReader` was deleted, stated as a test.

    It read the same three tables the store reads and returned the payload
    alone — no label, no metadata, no event timestamp, no offset, and no inverse
    for `payload_encoding`. So a replay driven through it silently lost
    everything `rakaia.history.envelope_actor`, `merge_replay(order_key=...)` and
    `history_effects(version_of=lambda m: m.offset)` read, and mangled any body
    that was not JSON.

    `DjangoStreamStore.read()` was already the correct reading of those tables,
    already held to `tests/server_store_contract.py`, and — since #180 — already
    able to take a database alias, which the deleted reader never could. It had
    no callers in this repository or in the one consumer (verified: no import, no
    dynamic reference, and an exact-minor pin between them), only its own tests,
    which pinned the lossy shape.

    These cases are the properties the deletion depends on. If the store ever
    stopped carrying them, deleting the alternative would have cost something.
    """

    def test_the_store_carries_the_whole_envelope(self):
        from django_rakaia.django_store import DjangoStreamStore
        from rakaia import AppendOptions

        store = DjangoStreamStore()
        store.create("s")
        store.append(
            "s",
            b'{"n": 1}',
            AppendOptions(label="update", metadata={"user": 7}, event_ts=1234.0),
        )

        message = store.read("s")[0][0]
        assert message.data == b'{"n": 1}'
        assert message.label == "update"
        assert message.metadata == {"user": 7}
        assert message.event_ts == 1234.0
        assert message.offset  # the deleted reader had no offset at all

    def test_the_store_inverts_a_non_json_body(self):
        from django_rakaia.django_store import DjangoStreamStore
        from django_rakaia.models import StreamEvent

        store = DjangoStreamStore()
        store.create("bin")
        store.append("bin", b"\xff\xfe\x00binary", _octet_stream())

        assert StreamEvent.objects.get().payload_encoding == "base64"
        assert store.read("bin")[0][0].data == b"\xff\xfe\x00binary"

    def test_the_store_reads_a_stream_written_by_the_decorator_path(self):
        # The deleted reader's stated purpose was reading the tables
        # `@stream_model` / `create_stream_event` populate. The store reads those
        # same rows, which is what makes it a replacement rather than a
        # substitute.
        #
        # Through the real `create_stream_event`, not a raw ORM insert. The first
        # version of this test built the rows by hand while being named for the
        # helper, so the one case carrying the "replacement, not substitute"
        # claim never exercised the path it claimed — and the deleted class's own
        # test was the only cover that path had.
        from django_rakaia.decorators import create_stream_event
        from django_rakaia.django_store import DjangoStreamStore

        from .models import AreaData

        for name in ("one", "two"):
            area = Area.objects.create(name=name)
            create_stream_event(
                stream_paths="decorated",
                to_dataclass=lambda obj: AreaData(id=obj.id, name=obj.name),
                instance=area,
                action="create",
            )

        messages, _ = DjangoStreamStore().read("decorated")
        assert [json.loads(m.data)["name"] for m in messages] == ["one", "two"]
        # The envelope the helper stamps, which the deleted reader dropped.
        assert [m.label for m in messages] == ["create", "create"]
        assert all(m.event_ts is not None for m in messages)

    def test_a_missing_stream_raises_where_the_deleted_reader_returned_empty(self):
        # Worth knowing now the module docstring sends people to the store: the
        # deleted reader answered `([], True)` for a stream that does not exist,
        # which a replay would read as "no events" rather than as an error.
        from django_rakaia.django_store import DjangoStreamStore
        from rakaia.types import StreamNotFound

        with pytest.raises(StreamNotFound):
            DjangoStreamStore().read("never-existed")

    def test_a_read_writes_only_when_the_stream_has_a_ttl(self):
        # `read()` calls `_touch`, so it *can* write — but `_touch` is a no-op
        # unless the stream has a TTL, so reading an ordinary stream is pure.
        # Both halves are asserted because "a read is a write" holds only for the
        # TTL case, and an earlier version of this test asserted it
        # unconditionally and was simply wrong.
        import time

        from django_rakaia.django_store import DjangoStreamStore

        store = DjangoStreamStore()

        store.create("plain")
        store.append("plain", b"{}")
        Stream.objects.filter(stream_id="plain").update(last_activity_at=0.0)
        store.read("plain")
        assert Stream.objects.get(stream_id="plain").last_activity_at == 0.0

        store.create("ttl", ttl_seconds=3600)
        store.append("ttl", b"{}")
        # Backdated *within* the window — far enough to see the change, not far
        # enough to expire, which is a different code path (below).
        stale = time.time() - 60
        Stream.objects.filter(stream_id="ttl").update(last_activity_at=stale)
        store.read("ttl")
        assert Stream.objects.get(stream_id="ttl").last_activity_at > stale

    def test_a_read_reaps_a_stream_that_has_aged_out(self):
        # The third thing the deleted reader did not do: it was expiry-blind, so
        # it would happily replay a stream past its TTL. The store deletes it
        # mid-read and reports it absent.
        import time

        from django_rakaia.django_store import DjangoStreamStore
        from rakaia.types import StreamNotFound

        store = DjangoStreamStore()
        store.create("aged", ttl_seconds=60)
        store.append("aged", b"{}")
        Stream.objects.filter(stream_id="aged").update(
            last_activity_at=time.time() - 3600
        )

        with pytest.raises(StreamNotFound):
            store.read("aged")
        assert not Stream.objects.filter(stream_id="aged").exists()
