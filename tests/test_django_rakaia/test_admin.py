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
from tests.test_django_rakaia.models import AppStreamEvent

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
        # Equality, not containment: the JSON form `'"hello, world"'` contains
        # the text too, so containment passes on a screen that ignores the
        # encoding entirely.
        assert self._admin().data_preview(event) == "hello, world"

    def test_a_long_text_body_is_cut_to_the_same_budget_as_json(self):
        # The two branches share `_truncate` so a list cell cannot blow out on
        # one and not the other; only the JSON branch was pinned, so dropping
        # the call on this one was invisible.
        event = StreamEvent.objects.create(
            data="x" * 500, event_type="append", payload_encoding="utf-8"
        )
        preview = self._admin().data_preview(event)
        assert len(preview) == 100 and preview.endswith("...")

    def test_an_ordinary_json_payload_previews_exactly_as_before(self):
        # The common case must not change shape.
        event = StreamEvent.objects.create(data={"n": 1}, event_type="create")
        preview = self._admin().data_preview(event)
        assert '"n"' in preview and "1" in preview


class TestEveryAdminScreenAgrees:
    """The same two questions, asked of each of the three screens.

    #195 fixed the answer on the event list and left the entries list and the
    factory an app registers its own event model with untouched — so a labelless
    append still showed `APPEND` on two screens out of three, and the factory's
    preview still printed a base64 body as base64 (#201). One test per screen,
    because a test of the shared helper is exactly what would have missed this:
    #195 had one, and both other screens were green throughout.

    Each asks what the screen *renders*, not what it calls.
    """

    def _subclass_admin(self):
        # The admin an app actually gets: `models.py` registers `AppStreamEvent`
        # through `register_stream_event_admin` at import, so this is the same
        # instance a consumer's site would serve.
        return django_admin.site._registry[AppStreamEvent]

    def _appended(self, label: str | None = None):
        """One raw append through the store, and the entry it produced."""
        from django_rakaia.django_store import DjangoStreamStore
        from rakaia import AppendOptions

        store = DjangoStreamStore()
        store.create("s")
        options = AppendOptions(label=label) if label is not None else None
        store.append("s", b'{"n": 1}', options)
        return store, StreamEntry.objects.get()

    def test_the_entries_list_shows_no_label_for_a_labelless_append(self) -> None:
        store, entry = self._appended()
        badge = StreamEntryAdmin(StreamEntry, django_admin.site).event_type_badge(entry)

        assert store.read("s")[0][0].label == ""
        assert "APPEND" not in badge
        # Deliberately absent, not blank — a blank badge reads as a bug.
        assert "—" in badge

    def test_the_entries_list_still_shows_a_real_label(self) -> None:
        _, entry = self._appended(label="update")
        badge = StreamEntryAdmin(StreamEntry, django_admin.site).event_type_badge(entry)
        assert "UPDATE" in badge and "#ffc107" in badge

    def test_a_registered_event_model_shows_no_label_for_a_labelless_append(
        self,
    ) -> None:
        # The worst of the three: this is the copy a consumer inherits.
        event = AppStreamEvent.objects.create(event_type="append", data={"n": 1})
        badge = self._subclass_admin().event_type_badge(event)

        assert "APPEND" not in badge
        assert "—" in badge

    def test_a_registered_event_model_still_shows_a_real_label(self) -> None:
        event = AppStreamEvent.objects.create(event_type="delete", data={})
        badge = self._subclass_admin().event_type_badge(event)
        assert "DELETE" in badge and "#dc3545" in badge

    def test_a_registered_event_model_does_not_preview_a_body_as_base64(self) -> None:
        event = AppStreamEvent.objects.create(
            data="//4AYmluYXJ5", event_type="append", payload_encoding="base64"
        )
        preview = self._subclass_admin().data_preview(event)

        assert "//4AYmluYXJ5" not in preview, (
            "the preview shows the stored base64 rather than saying what it is"
        )
        assert "base64" in preview and "9 bytes" in preview

    def test_a_registered_event_model_previews_a_text_body_as_its_text(self) -> None:
        event = AppStreamEvent.objects.create(
            data="hello, world", event_type="append", payload_encoding="utf-8"
        )
        # Exactly the text, not `json.dumps` of it. `"hello, world" in ...` is
        # satisfied by the quoted JSON form, so it cannot see a screen that
        # ignores `payload_encoding` — a green mutation found that gap.
        assert self._subclass_admin().data_preview(event) == "hello, world"

    def test_a_registered_event_model_does_not_emit_raw_markup(self) -> None:
        # It used to hand back `<br>`/`&nbsp;` in a plain string, which Django
        # escapes — so the viewer read the tags rather than seeing the layout.
        event = AppStreamEvent.objects.create(event_type="create", data={"a": 1})
        preview = self._subclass_admin().data_preview(event)
        assert "<br>" not in preview and "&nbsp;" not in preview
        assert '"a": 1' in preview

    def test_all_three_screens_render_one_append_the_same_way(self) -> None:
        # The point of the shared helper: the next screen cannot disagree.
        _, entry = self._appended()
        event = entry.event

        from_events = StreamEventAdmin(StreamEvent, django_admin.site)
        from_entries = StreamEntryAdmin(StreamEntry, django_admin.site)
        assert from_events.event_type_badge(event) == from_entries.event_type_badge(
            entry
        )

        same = AppStreamEvent.objects.create(
            event_type=event.event_type,
            data=event.data,
            payload_encoding=event.payload_encoding,
        )
        subclass = self._subclass_admin()
        assert subclass.event_type_badge(same) == from_events.event_type_badge(event)
        assert subclass.data_preview(same) == from_events.data_preview(event)
