"""One writer for the enveloped event, whichever door you came in by (#131).

There are two ways to write an event into the Django models — `store.append`
and `@stream_model` / `create_stream_event` — and they used to be two
*implementations* of it, each with its own copy of the envelope rules. The
copies disagreed:

* `event_type` — the store records a labelless append under the stable
  ``"append"`` sentinel, which `read()` inverts back to ``label=""``. The
  decorator wrote the label through raw, so an event written by that door could
  never be recognised as labelless.
* `event_ts` / `metadata` / offset allocation — three more rules, restated.

`django_rakaia/envelope.py` opens by warning that exactly this second copy
"produces events that replay differently from every other event in the same
stream, and no test anywhere is looking at the difference". These tests look at
the difference: they drive both doors with the same envelope and compare the
rows and the read-back messages.

What they deliberately do *not* pin is that the two doors choose the same
envelope. They don't: a raw protocol append carries no `event_ts` unless the
producer sets one, while `@stream_model` always stamps `time.time()` (ADR 0002
item 5, `test_decorators_provenance.TestEnvelopeTimestamp`). That is a
difference of *input*, decided by the caller. Everything below is about what
the writer does with the input it is given.
"""

from __future__ import annotations

import json

import pytest

from django_rakaia.decorators import create_stream_event
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import Stream, StreamEvent
from rakaia import AppendOptions, provenance

from .models import AreaData

pytestmark = pytest.mark.django_db


PAYLOAD = {"id": 1, "name": "a"}


class _Instance:
    """Stand-in for a model instance; `create_stream_event` only passes it on."""


def _to_dataclass(_instance: object) -> AreaData:
    return AreaData(id=PAYLOAD["id"], name=PAYLOAD["name"])


def _store_door(path: str, *, label: str = "", **envelope: object) -> StreamEvent:
    """Write one event through `DjangoStreamStore.append`."""
    store = DjangoStreamStore()
    store.create(path)
    store.append(
        path,
        json.dumps(PAYLOAD).encode(),
        AppendOptions(label=label, **envelope),  # type: ignore[arg-type]
    )
    return Stream.objects.get(stream_id=path).entries.get().event


def _decorator_door(path: str, *, label: str = "") -> StreamEvent:
    """Write one event through `create_stream_event`, the `@stream_model` door."""
    return create_stream_event(
        stream_paths=path,
        to_dataclass=_to_dataclass,
        instance=_Instance(),  # type: ignore[arg-type]
        action=label,
    )


def _shape(event: StreamEvent) -> dict[str, object]:
    """The fields the *writer* decides, as opposed to the caller."""
    return {
        "data": event.data,
        "event_type": event.event_type,
        "metadata": event.metadata,
        "payload_encoding": event.payload_encoding,
        "event_ts_is_set": event.event_ts is not None,
    }


class TestTheSentinel:
    """A labelless event is recorded as ``"append"`` — from either door."""

    def test_the_store_door_writes_the_sentinel(self):
        assert _store_door("store/labelless").event_type == "append"

    def test_the_decorator_door_writes_the_sentinel(self):
        assert _decorator_door("decorator/labelless").event_type == "append"

    def test_a_label_is_written_through_unchanged(self):
        assert _store_door("store/labelled", label="create").event_type == "create"
        assert _decorator_door("dec/labelled", label="create").event_type == "create"

    def test_the_sentinel_inverts_on_read_from_either_door(self):
        """`read()` maps the sentinel back to "no label". An event the decorator
        wrote must be recognisable as labelless too, or it replays with a
        phantom ``"append"`` label that no producer ever set."""
        _store_door("store/invert")
        _decorator_door("decorator/invert")

        store = DjangoStreamStore()
        for path in ("store/invert", "decorator/invert"):
            messages, _ = store.read(path)
            assert [m.label for m in messages] == [""], path


class TestTheDoorsAgree:
    """Same envelope in, same row out."""

    def test_the_shape_matches_with_no_ambient_provenance(self):
        store_event = _store_door("store/plain", label="create", event_ts=1.0)
        decorator_event = _decorator_door("decorator/plain", label="create")

        assert _shape(store_event) == _shape(decorator_event)

    def test_the_shape_matches_inside_a_provenance_block(self):
        with provenance(user=7, url="/areas/"):
            store_event = _store_door("store/prov", label="create", event_ts=1.0)
            decorator_event = _decorator_door("decorator/prov", label="create")

        assert _shape(store_event) == _shape(decorator_event)
        assert store_event.metadata == {"user": 7, "url": "/areas/"}

    def test_metadata_is_an_empty_dict_not_null_from_either_door(self):
        assert _store_door("store/meta").metadata == {}
        assert _decorator_door("decorator/meta").metadata == {}

    def test_an_explicit_event_ts_is_passed_through_unchanged(self):
        assert _store_door("store/ts", event_ts=1234.5).event_ts == 1234.5

    def test_offsets_start_at_one_from_either_door(self):
        _store_door("store/offset")
        _decorator_door("decorator/offset")

        for path in ("store/offset", "decorator/offset"):
            offsets = list(
                Stream.objects.get(stream_id=path).entries.values_list(
                    "offset", flat=True
                )
            )
            assert offsets == [1], path


class TestFanOut:
    """A fan-out is one event in several streams, not several events."""

    def test_one_event_shared_by_every_entry(self):
        before = StreamEvent.objects.count()

        event = create_stream_event(
            stream_paths=["fanout/one", "fanout/two", "fanout/three"],
            to_dataclass=_to_dataclass,
            instance=_Instance(),  # type: ignore[arg-type]
            action="create",
        )

        assert StreamEvent.objects.count() == before + 1
        assert sorted(e.stream.stream_id for e in event.entries.all()) == [
            "fanout/one",
            "fanout/three",
            "fanout/two",
        ]
        assert {e.offset for e in event.entries.all()} == {1}
