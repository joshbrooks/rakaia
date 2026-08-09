"""Consumer-facing contract of ``@stream_model`` / ``create_stream_event``.

The three findings from integrating rakaia as formkit-ninja's audit log
(issue #80) — each one something *every* Django consumer hits:

1. ``raw=True`` saves (``loaddata``, ``serialized_rollback``) must not append
   phantom events.
2. ``StreamEvent.data`` must accept the types Django models actually hold —
   ``UUID``, ``datetime``, ``Decimal`` — without the consumer pre-stringifying.
3. The ``post_delete`` event must be suppressible or re-typed, because on a
   soft-delete model the row is still there.
"""

import datetime as dt
import uuid
from dataclasses import dataclass
from decimal import Decimal

import pytest
from django.core import serializers
from django.db.models.signals import post_save

from django_rakaia.decorators import create_stream_event, stream_model
from django_rakaia.models import Stream, StreamEvent
from tests.test_django_rakaia.models import ArchivedDoc, Area, Measure, SoftDeleteDoc


def _entries(stream_id: str):
    stream = Stream.objects.filter(stream_id=stream_id).first()
    return list(stream.entries.order_by("offset")) if stream else []


@pytest.mark.django_db
class TestRawSavesAreIgnored:
    """Item 1: fixtures must not corrupt streams."""

    def test_raw_signal_appends_nothing(self):
        area = Area.objects.create(name="Fixture Area")
        stream_id = f"area:{area.id}:projects"
        assert len(_entries(stream_id)) == 1

        post_save.send(
            sender=Area,
            instance=area,
            created=True,
            raw=True,
            using="default",
            update_fields=None,
        )

        assert len(_entries(stream_id)) == 1, "raw save appended a phantom event"

    def test_deserialized_save_appends_nothing(self):
        """The real ``loaddata`` path: ``DeserializedObject.save()``."""
        area = Area.objects.create(name="Fixture Area")
        stream_id = f"area:{area.id}:projects"
        payload = serializers.serialize("json", [area])

        for deserialized in serializers.deserialize("json", payload):
            deserialized.save()

        assert len(_entries(stream_id)) == 1

    def test_repeated_fixture_restores_do_not_multiply_events(self):
        area = Area.objects.create(name="Fixture Area")
        stream_id = f"area:{area.id}:projects"
        payload = serializers.serialize("json", [area])

        for _ in range(3):
            for deserialized in serializers.deserialize("json", payload):
                deserialized.save()

        assert [e.offset for e in _entries(stream_id)] == [1]

    def test_non_raw_save_still_appends(self):
        """The guard must not swallow ordinary saves."""
        area = Area.objects.create(name="Fixture Area")
        area.name = "Renamed"
        area.save()

        entries = _entries(f"area:{area.id}:projects")
        assert [e.event.event_type for e in entries] == ["create", "update"]


@dataclass
class MeasureData:
    """Payload holding exactly the types a plain ``JSONField`` chokes on."""

    ref: uuid.UUID
    amount: Decimal
    recorded_at: dt.datetime


@pytest.mark.django_db
class TestJsonEncoding:
    """Item 2: ``StreamEvent.data`` carries UUID / datetime / Decimal."""

    def test_stream_event_data_accepts_django_types(self):
        ref = uuid.uuid4()
        moment = dt.datetime(2026, 8, 9, 12, 30, tzinfo=dt.timezone.utc)

        event = StreamEvent.objects.create(
            data={"ref": ref, "amount": Decimal("12.50"), "recorded_at": moment},
            event_type="create",
        )

        stored = StreamEvent.objects.get(pk=event.pk).data
        assert stored["ref"] == str(ref)
        assert stored["amount"] == "12.50"
        assert stored["recorded_at"].startswith("2026-08-09T12:30:00")

    def test_stream_event_metadata_accepts_django_types(self):
        moment = dt.datetime(2026, 8, 9, 12, 30, tzinfo=dt.timezone.utc)

        event = StreamEvent.objects.create(
            data={},
            event_type="create",
            metadata={"actor": uuid.uuid4(), "at": moment},
        )

        stored = StreamEvent.objects.get(pk=event.pk).metadata
        assert stored["at"].startswith("2026-08-09T12:30:00")

    def test_create_stream_event_needs_no_prestringify(self):
        """A transformer may return the model's own field values verbatim."""
        measure = Measure.objects.create(ref=uuid.uuid4(), amount=Decimal("3.25"))

        event = create_stream_event(
            stream_paths="measure:events",
            to_dataclass=lambda obj: MeasureData(
                ref=obj.ref,
                amount=obj.amount,
                recorded_at=dt.datetime(2026, 8, 9, tzinfo=dt.timezone.utc),
            ),
            instance=measure,
            action="create",
        )

        stored = StreamEvent.objects.get(pk=event.pk).data
        assert stored["ref"] == str(measure.ref)
        assert stored["amount"] == "3.25"
        assert stored["recorded_at"].startswith("2026-08-09T00:00:00")


@pytest.mark.django_db
class TestSoftDeleteAwareDelete:
    """Item 3: the ``post_delete`` event is customizable."""

    def test_default_still_emits_delete(self):
        area = Area.objects.create(name="Doomed")
        stream_id = f"area:{area.id}:projects"
        area.delete()

        assert [e.event.event_type for e in _entries(stream_id)] == ["create", "delete"]

    def test_on_delete_none_suppresses_the_event(self):
        doc = SoftDeleteDoc.objects.create(name="Survivor")
        stream_id = f"softdeletedoc:{doc.id}:events"
        doc.delete()

        assert [e.event.event_type for e in _entries(stream_id)] == ["create"]

    def test_on_delete_none_still_streams_the_soft_delete_update(self):
        """The ``is_active`` flip a soft delete really performs is an update."""
        doc = SoftDeleteDoc.objects.create(name="Survivor")
        stream_id = f"softdeletedoc:{doc.id}:events"

        doc.is_active = False
        doc.save()

        entries = _entries(stream_id)
        assert [e.event.event_type for e in entries] == ["create", "update"]
        assert entries[-1].event.data["is_active"] is False

    def test_on_delete_update_emits_an_update(self):
        doc = ArchivedDoc.objects.create(name="Archivable")
        stream_id = f"archiveddoc:{doc.id}:events"
        doc.delete()

        entries = _entries(stream_id)
        assert [e.event.event_type for e in entries] == ["create", "update"]

    def test_delete_to_dataclass_overrides_the_payload(self):
        """Django hands ``post_delete`` the stale pre-delete snapshot."""
        doc = ArchivedDoc.objects.create(name="Archivable")
        stream_id = f"archiveddoc:{doc.id}:events"
        doc.delete()

        entries = _entries(stream_id)
        assert entries[0].event.data["is_active"] is True
        assert entries[-1].event.data["is_active"] is False
        assert entries[-1].event.data["name"] == "Archivable"

    def test_invalid_on_delete_is_rejected_at_decoration_time(self):
        with pytest.raises(ValueError, match="on_delete"):
            stream_model(
                stream_paths="x",
                to_dataclass=lambda obj: obj,
                on_delete="soft-delete",  # type: ignore[arg-type]
            )

    def test_delete_to_dataclass_without_delete_event_is_rejected(self):
        with pytest.raises(ValueError, match="delete_to_dataclass"):
            stream_model(
                stream_paths="x",
                to_dataclass=lambda obj: obj,
                on_delete=None,
                delete_to_dataclass=lambda obj: obj,
            )
