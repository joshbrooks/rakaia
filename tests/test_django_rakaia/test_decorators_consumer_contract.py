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

import contextlib
import datetime as dt
import json
import uuid
from dataclasses import dataclass
from decimal import Decimal

import pytest
from django.core import serializers
from django.db import connection, models
from django.db.models.signals import post_save

from django_rakaia import channels_signals
from django_rakaia.decorators import create_stream_event, stream_model
from django_rakaia.models import Stream, StreamEvent
from tests.test_django_rakaia.models import ArchivedDoc, Area, Measure, SoftDeleteDoc


def _entries(stream_id: str):
    stream = Stream.objects.filter(stream_id=stream_id).first()
    return list(stream.entries.order_by("offset")) if stream else []


# The emulated trigger below is SQLite dialect (`SELECT RAISE(IGNORE)`). The
# test settings are SQLite everywhere today; skip rather than fail with a
# syntax error if a run is ever pointed at another backend.
requires_sqlite = pytest.mark.skipif(
    connection.vendor != "sqlite",
    reason="emulated soft-delete trigger uses SQLite-specific RAISE(IGNORE)",
)


@contextlib.contextmanager
def db_level_soft_delete(model_cls: type[models.Model]):
    """Emulate ``pgtrigger.SoftDelete`` for the duration of the block.

    pgtrigger installs a ``BEFORE DELETE`` trigger that flips ``is_active`` and
    returns ``NULL``, cancelling the row delete. SQLite's equivalent is the same
    ``UPDATE`` followed by ``RAISE(IGNORE)``, which abandons the statement that
    fired the trigger. Either way the row survives, the delete is performed
    entirely inside the database, and Django — which issued a plain ``DELETE``
    and never inspects the row count — still fires ``post_delete`` and never
    ``post_save``. That asymmetry is the whole of issue #80 item 3.
    """
    table = model_cls._meta.db_table
    trigger = f"{table}_emulated_soft_delete"
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE TRIGGER {trigger} BEFORE DELETE ON {table} "
            f"BEGIN "
            f"UPDATE {table} SET is_active = 0 WHERE id = OLD.id; "
            f"SELECT RAISE(IGNORE); "
            f"END"
        )
    try:
        yield
    finally:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TRIGGER IF EXISTS {trigger}")


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
        assert stored["recorded_at"] == "2026-08-09T12:30:00Z"

    def test_stream_event_metadata_accepts_django_types(self):
        moment = dt.datetime(2026, 8, 9, 12, 30, tzinfo=dt.timezone.utc)

        event = StreamEvent.objects.create(
            data={},
            event_type="create",
            metadata={"actor": uuid.uuid4(), "at": moment},
        )

        stored = StreamEvent.objects.get(pk=event.pk).metadata
        assert stored["at"] == "2026-08-09T12:30:00Z"

    @pytest.mark.parametrize(
        ("microsecond", "expected"),
        [
            (123456, "2026-08-09T12:30:00.123Z"),
            (123999, "2026-08-09T12:30:00.123Z"),
            (4, "2026-08-09T12:30:00.000Z"),
            (0, "2026-08-09T12:30:00Z"),
        ],
    )
    def test_datetimes_truncate_to_milliseconds(self, microsecond, expected):
        """Pin the encoder's lossy case so it cannot change silently.

        ``DjangoJSONEncoder`` truncates — does not round — to three fractional
        digits, and drops the fractional part entirely when ``microsecond`` is
        0. Django stores microseconds, so a payload timestamp never compares
        equal to the column it was lifted from.
        """
        moment = dt.datetime(
            2026, 8, 9, 12, 30, microsecond=microsecond, tzinfo=dt.timezone.utc
        )

        event = StreamEvent.objects.create(
            data={"recorded_at": moment}, event_type="create"
        )

        assert StreamEvent.objects.get(pk=event.pk).data["recorded_at"] == expected

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
        assert stored["recorded_at"] == "2026-08-09T00:00:00Z"

    def test_in_memory_event_carries_the_encoded_payload(self):
        """The field encoder only covers the INSERT.

        ``StreamEvent.data``'s ``DjangoJSONEncoder`` never writes the encoded
        form back onto the instance, and the SSE fan-out broadcasts that
        in-memory object — so the payload has to be primitives before it is
        handed to ``StreamEvent.objects.create``, not just on the way to disk.
        """
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

        # No refetch — this is the object the signal receivers see.
        assert event.data["ref"] == str(measure.ref)
        json.dumps(event.data)

    def test_sse_broadcast_of_a_django_typed_payload_does_not_raise(self, monkeypatch):
        """The real crash path: ``handle_stream_entry_created`` broadcasts the
        in-memory payload from inside the consumer's own ``post_save``.

        A raw ``UUID`` there takes down the save being audited — msgpack under
        ``channels_redis``, ``json.dumps`` under the SSE view.
        """
        sent: list[dict] = []

        class RecordingLayer:
            async def group_send(self, group, message):  # noqa: ARG002
                sent.append(message)

        monkeypatch.setattr(
            channels_signals, "get_channel_layer", lambda: RecordingLayer()
        )
        measure = Measure.objects.create(ref=uuid.uuid4(), amount=Decimal("3.25"))

        create_stream_event(
            stream_paths="measure:sse",
            to_dataclass=lambda obj: MeasureData(
                ref=obj.ref,
                amount=obj.amount,
                recorded_at=dt.datetime(2026, 8, 9, tzinfo=dt.timezone.utc),
            ),
            instance=measure,
            action="create",
        )

        assert sent, "the SSE receiver did not fire"
        for message in sent:
            json.dumps(message)  # what the SSE view does
        assert sent[-1]["event"]["data"]["ref"] == str(measure.ref)

    def test_raw_stream_entry_save_is_not_broadcast(self, monkeypatch):
        """Fixture rows are replayed history — they must not reach subscribers.

        The dereferences in the receiver (``instance.stream``, ``instance.event``)
        are also separate queries that raise mid-``loaddata`` when the parent
        rows have not been restored yet.
        """
        sent: list[dict] = []

        class RecordingLayer:
            async def group_send(self, group, message):  # noqa: ARG002
                sent.append(message)

        monkeypatch.setattr(
            channels_signals, "get_channel_layer", lambda: RecordingLayer()
        )
        area = Area.objects.create(name="Fixture Area")
        entry = _entries(f"area:{area.id}:projects")[0]
        sent.clear()

        post_save.send(
            sender=type(entry),
            instance=entry,
            created=True,
            raw=True,
            using="default",
            update_fields=None,
        )

        assert sent == [], "a fixture row was broadcast to subscribers"


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

    @requires_sqlite
    def test_db_level_soft_delete_with_on_delete_none_streams_nothing(self):
        """The trap: ``on_delete=None`` loses a DB-level soft delete entirely.

        Under a real soft-delete trigger the flip happens inside the database,
        so Django fires only ``post_delete``. Suppressing that receiver leaves
        the stream with no record of the delete at all — a projection replayed
        from it shows the row active forever. ``on_delete=None`` is for models
        that soft-delete in Python (where ``post_save`` really does carry the
        flip), not for this shape.
        """
        doc = SoftDeleteDoc.objects.create(name="Survivor")
        pk, stream_id = doc.pk, f"softdeletedoc:{doc.id}:events"

        with db_level_soft_delete(SoftDeleteDoc):
            doc.delete()

        survivor = SoftDeleteDoc.objects.get(pk=pk)
        assert survivor.is_active is False, "trigger did not perform the flip"
        assert [e.event.event_type for e in _entries(stream_id)] == ["create"], (
            "post_save fired after a DB-level soft delete — it does not; the "
            "premise of the on_delete=None guidance would be sound if it did"
        )

    @requires_sqlite
    def test_db_level_soft_delete_with_on_delete_update_records_the_flip(self):
        """``on_delete="update"`` is what actually captures a DB-level soft delete."""
        doc = ArchivedDoc.objects.create(name="Archivable")
        pk, stream_id = doc.pk, f"archiveddoc:{doc.id}:events"

        with db_level_soft_delete(ArchivedDoc):
            doc.delete()

        survivor = ArchivedDoc.objects.get(pk=pk)
        assert survivor.is_active is False

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
