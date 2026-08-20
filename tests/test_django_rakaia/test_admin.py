"""Tests for the Django admin display helpers (``django_rakaia.admin``).

Exercises the custom ``ModelAdmin`` display/formatter methods directly against
model instances: counts, badges, data previews (including truncation and the
short-data path), link builders, translation status, and the dynamic
``register_stream_event_admin`` guard.
"""

import pytest
from django.contrib import admin as django_admin

from django_rakaia.admin import (
    StreamAdmin,
    StreamEntryAdmin,
    StreamEventAdmin,
    register_stream_event_admin,
)
from django_rakaia.models import Stream, StreamEntry, StreamEvent

pytestmark = pytest.mark.django_db


def _entry(stream_id: str, offset: int, event_type: str = "create", **data):
    stream, _ = Stream.objects.get_or_create(stream_id=stream_id)
    event = StreamEvent.objects.create(event_type=event_type, data=data or {"k": "v"})
    entry = StreamEntry.objects.create(stream=stream, event=event, offset=offset)
    return stream, event, entry


class TestStreamAdmin:
    def setup_method(self) -> None:
        self.admin = StreamAdmin(Stream, django_admin.site)

    def test_event_count_and_last_offset(self) -> None:
        stream, _, _ = _entry("s:1", 1)
        _entry("s:1", 7)
        assert self.admin.event_count(stream) == 2
        assert self.admin.last_entry_offset(stream) == 7

    def test_last_offset_none_when_empty(self) -> None:
        stream = Stream.objects.create(stream_id="empty")
        assert self.admin.event_count(stream) == 0
        assert self.admin.last_entry_offset(stream) is None


class TestStreamEventAdmin:
    def setup_method(self) -> None:
        self.admin = StreamEventAdmin(StreamEvent, django_admin.site)

    def test_badge_known_and_unknown_type(self) -> None:
        create_evt = StreamEvent.objects.create(event_type="create", data={})
        weird_evt = StreamEvent.objects.create(event_type="weird", data={})
        assert "CREATE" in self.admin.event_type_badge(create_evt)
        assert "#28a745" in self.admin.event_type_badge(create_evt)  # green
        # Unknown type falls back to the neutral grey.
        assert "#6c757d" in self.admin.event_type_badge(weird_evt)

    def test_data_preview_short_returns_json(self) -> None:
        evt = StreamEvent.objects.create(event_type="create", data={"a": 1})
        preview = self.admin.data_preview(evt)
        assert preview is not None
        assert '"a": 1' in preview

    def test_data_preview_truncates_long(self) -> None:
        evt = StreamEvent.objects.create(event_type="create", data={"blob": "x" * 500})
        preview = self.admin.data_preview(evt)
        assert preview.endswith("...")
        assert len(preview) == 100

    def test_stream_count_and_streams_list(self) -> None:
        _, event, _ = _entry("s:1", 1)
        StreamEntry.objects.create(
            stream=Stream.objects.get_or_create(stream_id="s:2")[0],
            event=event,
            offset=1,
        )
        assert self.admin.stream_count(event) == 2
        listed = self.admin.streams_list(event)
        assert "s:1" in listed and "s:2" in listed

    def test_streams_list_empty(self) -> None:
        event = StreamEvent.objects.create(event_type="create", data={})
        assert self.admin.streams_list(event) == "-"


class TestStreamEntryAdmin:
    def setup_method(self) -> None:
        self.admin = StreamEntryAdmin(StreamEntry, django_admin.site)

    def test_links_and_badge(self) -> None:
        _, _, entry = _entry("s:1", 3, event_type="delete")
        stream_link = self.admin.stream_link(entry)
        assert "s:1" in stream_link and "/admin/django_rakaia/stream/" in stream_link
        event_link = self.admin.event_link(entry)
        assert "/admin/django_rakaia/streamevent/" in event_link
        badge = self.admin.event_type_badge(entry)
        assert "DELETE" in badge and "#dc3545" in badge  # red


class TestRegisterStreamEventAdmin:
    def test_base_streamevent_is_noop(self) -> None:
        before = set(django_admin.site._registry)
        register_stream_event_admin(StreamEvent)
        # Registering the base model must not touch the registry.
        assert set(django_admin.site._registry) == before


class TestTheAdminAgreesWithEveryOtherReader:
    """What the admin shows must be what `read()` reports.

    The admin is a read surface like the SSE view and the channel frame, and
    #153 made the primitives those surfaces need single: `event_label` inverts
    the stored `"append"` sentinel back to the empty string, and `decode_payload`
    inverts `payload_encoding`. The admin called neither, so it rendered the
    sentinel as a label and printed a base64 body as base64.

    Anyone comparing the admin against the audit trail saw two different answers
    for one event, which is the whole failure #153 exists to prevent — in a
    surface `event_message.py`'s own docstring names as fixed.
    """

    def _admin(self):
        return StreamEventAdmin(StreamEvent, django_admin.site)

    def test_a_labelless_append_shows_no_label_not_the_sentinel(self):
        from django_rakaia.django_store import DjangoStreamStore

        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"n": 1}')
        event = StreamEvent.objects.get()

        badge = self._admin().event_type_badge(event)
        assert store.read("s")[0][0].label == ""
        assert "APPEND" not in badge
        # And says *something* — a blank cell reads as a rendering bug, so the
        # absence of a label is shown deliberately. Asserted because "not APPEND"
        # alone is satisfied by an empty badge.
        assert "\u2014" in badge

    def test_a_real_label_is_still_shown(self):
        # The sentinel is the only value that inverts; a genuine label must
        # survive, or the badge becomes useless.
        from django_rakaia.django_store import DjangoStreamStore
        from rakaia import AppendOptions

        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b"{}", AppendOptions(label="update"))
        event = StreamEvent.objects.get()

        assert "UPDATE" in self._admin().event_type_badge(event)

    def test_a_base64_body_is_not_previewed_as_base64(self):
        from django_rakaia.event_message import decode_payload

        event = StreamEvent.objects.create(
            data="//4AYmluYXJ5", event_type="append", payload_encoding="base64"
        )
        assert (
            decode_payload(event.data, event.payload_encoding) == b"\xff\xfe\x00binary"
        )

        preview = self._admin().data_preview(event)
        assert "//4AYmluYXJ5" not in preview, (
            "the preview shows the stored base64 rather than saying what it is"
        )

    def test_a_text_body_is_previewed_as_its_text(self):
        event = StreamEvent.objects.create(
            data="hello, world", event_type="append", payload_encoding="utf-8"
        )
        assert "hello, world" in self._admin().data_preview(event)

    def test_an_ordinary_json_payload_previews_exactly_as_before(self):
        # The common case must not change shape.
        event = StreamEvent.objects.create(data={"n": 1}, event_type="create")
        preview = self._admin().data_preview(event)
        assert '"n"' in preview and "1" in preview
