"""Tests for django_rakaia.streams readers."""

from __future__ import annotations

import json

import pytest

from django_rakaia.decorators import create_stream_event
from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.models import Stream, StreamEntry, StreamEvent
from django_rakaia.streams import DjangoStreamReader, ModelStreamReader
from rakaia.effects import Effect
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
            return Effect(
                op="update_or_create",
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
            return Effect(
                op="update_or_create",
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
# DjangoStreamReader — reads from real Stream/StreamEvent/StreamEntry rows
# ---------------------------------------------------------------------------


def _append_event(stream_id: str, data: dict) -> None:
    """Helper: write one event to a Django-backed stream."""
    stream, _ = Stream.objects.get_or_create(stream_id=stream_id)
    event = StreamEvent.objects.create(data=data, event_type="test")
    next_offset = stream.get_next_offset()
    StreamEntry.objects.create(stream=stream, event=event, offset=next_offset)


@pytest.mark.django_db
class TestDjangoStreamReader:
    def test_missing_stream_returns_empty(self):
        reader = DjangoStreamReader()
        messages, up_to_date = reader.read("nope")
        assert messages == []
        assert up_to_date is True

    def test_has_missing_stream(self):
        reader = DjangoStreamReader()
        assert reader.has("nope") is False

    def test_reads_events_in_offset_order(self):
        _append_event("s", {"id": 1, "name": "first"})
        _append_event("s", {"id": 2, "name": "second"})
        _append_event("s", {"id": 3, "name": "third"})

        reader = DjangoStreamReader()
        messages, up_to_date = reader.read("s")
        assert up_to_date is True
        payloads = [json.loads(m.data) for m in messages]
        assert [p["name"] for p in payloads] == ["first", "second", "third"]

    def test_has_existing_stream(self):
        _append_event("s", {"x": 1})
        reader = DjangoStreamReader()
        assert reader.has("s") is True

    def test_replay_through_django_stream_reader(self):
        _append_event("s", {"name": "alpha"})
        _append_event("s", {"name": "beta"})

        reg = HandlerRegistry()

        def h(event):
            return Effect(
                op="update_or_create",
                model_label="test_django_rakaia.Area",
                lookup={"name": f"from-stream:{event['name']}"},
                defaults={},
            )

        reg.register("h", "s", h, 0, None)
        reader = DjangoStreamReader()

        result = replay(reader, "s", DjangoExecutor(), handler_registry=reg)  # type: ignore[arg-type]

        assert result.events_processed == 2
        assert result.effects_applied == 2
        assert Area.objects.filter(name="from-stream:alpha").exists()
        assert Area.objects.filter(name="from-stream:beta").exists()

    def test_reads_multi_stream_events_isolated(self):
        _append_event("s1", {"v": "a"})
        _append_event("s2", {"v": "b"})
        _append_event("s1", {"v": "c"})

        reader = DjangoStreamReader()
        msgs1, _ = reader.read("s1")
        msgs2, _ = reader.read("s2")

        assert [json.loads(m.data)["v"] for m in msgs1] == ["a", "c"]
        assert [json.loads(m.data)["v"] for m in msgs2] == ["b"]

    def test_works_with_create_stream_event_helper(self):
        # End-to-end with the actual helper @stream_model uses internally.
        from dataclasses import dataclass

        @dataclass
        class Payload:
            kind: str
            value: int

        # Pretend we're a model save: use create_stream_event directly.
        # Need a model instance; use Area as a stand-in.
        area = Area.objects.create(name="probe")
        create_stream_event(
            stream_paths="from-helper",
            to_dataclass=lambda _inst: Payload(kind="ping", value=42),
            instance=area,
            action="create",
        )

        reader = DjangoStreamReader()
        messages, _ = reader.read("from-helper")
        assert len(messages) == 1
        payload = json.loads(messages[0].data)
        assert payload == {"kind": "ping", "value": 42}
