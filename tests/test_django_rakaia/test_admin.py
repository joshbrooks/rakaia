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
    TranslatableAdmin,
    register_stream_event_admin,
)
from django_rakaia.models import Stream, StreamEntry, StreamEvent, Translatable

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


class TestTranslatableAdmin:
    def setup_method(self) -> None:
        self.admin = TranslatableAdmin(Translatable, django_admin.site)

    def test_msgstr_preview_variants(self) -> None:
        empty = Translatable.objects.create(msgid="a", msgstr="", langcode="pt")
        assert "Not translated" in self.admin.msgstr_preview(empty)

        short = Translatable.objects.create(msgid="b", msgstr="hi", langcode="pt")
        assert self.admin.msgstr_preview(short) == "hi"

        longt = Translatable.objects.create(msgid="c", msgstr="y" * 80, langcode="pt")
        assert self.admin.msgstr_preview(longt).endswith("...")

    def test_langcode_badge_known_and_unknown(self) -> None:
        known = Translatable.objects.create(msgid="a", msgstr="x", langcode="pt")
        assert "#6f42c1" in self.admin.langcode_badge(known)  # purple
        unknown = Translatable.objects.create(msgid="b", msgstr="x", langcode="zz")
        assert "#6c757d" in self.admin.langcode_badge(unknown)  # grey fallback

    def test_deleted_status(self) -> None:
        from django.utils import timezone

        active = Translatable.objects.create(msgid="a", msgstr="x", langcode="pt")
        assert "Active" in self.admin.deleted_status(active)
        gone = Translatable.objects.create(
            msgid="b", msgstr="x", langcode="pt", deleted=timezone.now()
        )
        assert "Deleted" in self.admin.deleted_status(gone)

    def test_get_queryset(self, rf) -> None:
        Translatable.objects.create(msgid="a", msgstr="x", langcode="pt")
        request = rf.get("/admin/")
        assert self.admin.get_queryset(request).count() == 1


class TestRegisterStreamEventAdmin:
    def test_base_streamevent_is_noop(self) -> None:
        before = set(django_admin.site._registry)
        register_stream_event_admin(StreamEvent)
        # Registering the base model must not touch the registry.
        assert set(django_admin.site._registry) == before
