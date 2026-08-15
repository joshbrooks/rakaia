"""`@stream_model` must record the ambient envelope, like every other append path.

`provenance(actor=..., url=...)` sets envelope metadata for the duration of a
block, and `ProvenanceMiddleware` opens such a block around every request. Both
stores merge it on append (`store.py`, `django_store.py` both call
`merge_provenance`). `@stream_model` — the most-used append path in the Django
integration, and the only one a typical consumer touches — did not: it wrote
`StreamEvent` rows directly, so `metadata` was always `{}` and `event_ts` always
`NULL`.

The effect is that the middleware built to stamp actor and URL onto envelopes
could not reach the appends it exists for. `rakaia.history.envelope_actor` then
silently fell back to the payload's owner FK for every `@stream_model` event —
"who saved this" quietly became "who owns this", which is a different question
and often a different answer.

ADR 0002 item 7 makes the envelope first-class. These tests hold the decorator
to that.
"""

from __future__ import annotations

import pytest

from django_rakaia.models import StreamEvent
from rakaia import provenance

from .models import Area

pytestmark = pytest.mark.django_db


def _latest() -> StreamEvent:
    return StreamEvent.objects.latest("id")


class TestAmbientProvenanceIsRecorded:
    """The RED core: `metadata` is `{}` today, whatever the ambient block says."""

    def test_a_save_inside_provenance_records_the_actor(self):
        with provenance(user=1):
            Area.objects.create(name="a")
        assert _latest().metadata["user"] == 1

    def test_every_ambient_field_is_recorded(self):
        with provenance(user=7, url="/areas/", causation="req-abc"):
            Area.objects.create(name="a")
        metadata = _latest().metadata
        assert metadata["user"] == 7
        assert metadata["url"] == "/areas/"
        assert metadata["causation"] == "req-abc"

    def test_an_update_records_provenance_too(self):
        area = Area.objects.create(name="a")
        with provenance(user=2):
            area.name = "b"
            area.save()
        event = _latest()
        assert event.event_type == "update"
        assert event.metadata["user"] == 2

    def test_a_delete_records_provenance_too(self):
        area = Area.objects.create(name="a")
        with provenance(user=3):
            area.delete()
        event = _latest()
        assert event.event_type == "delete"
        assert event.metadata["user"] == 3


class TestNoAmbientProvenance:
    """Outside a `provenance()` block nothing changes — this is additive."""

    def test_metadata_is_empty_without_an_ambient_block(self):
        Area.objects.create(name="a")
        assert _latest().metadata == {}

    def test_the_event_is_otherwise_unchanged(self):
        area = Area.objects.create(name="solo")
        event = _latest()
        assert event.event_type == "create"
        assert event.data["name"] == "solo"
        assert event.data["id"] == area.pk


class TestEnvelopeTimestamp:
    """`event_ts` is the deterministic merge key. A path that leaves it NULL
    forces `merge_replay` onto transport time, which is the ordering trap ADR
    0002 item 5 closed."""

    def test_event_ts_is_set(self):
        Area.objects.create(name="a")
        assert _latest().event_ts is not None

    def test_event_ts_is_monotonic_across_successive_saves(self):
        Area.objects.create(name="a")
        first = _latest().event_ts
        Area.objects.create(name="b")
        second = _latest().event_ts
        assert second >= first


class TestFanOutSharesOneEnvelope:
    """A `@stream_model` event can appear in several streams. It is one event, so
    it carries one envelope — the metadata lives on `StreamEvent`, which the
    entries share."""

    def test_a_multi_stream_event_records_provenance_once(self):
        from django.contrib.auth import get_user_model

        from .models import Project

        user = get_user_model().objects.create(username="u")
        area = Area.objects.create(name="a")
        before = StreamEvent.objects.count()

        with provenance(user=99):
            Project.objects.create(name="p", area=area, created_by=user)

        # One new StreamEvent, fanned out to several StreamEntry rows.
        event = _latest()
        assert StreamEvent.objects.count() == before + 1
        assert event.metadata["user"] == 99
        assert event.entries.count() > 1
