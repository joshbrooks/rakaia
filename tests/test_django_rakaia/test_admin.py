"""Tests for the Django admin display helpers (``django_rakaia.admin``).

Exercises the custom ``ModelAdmin`` display/formatter methods directly against
model instances: counts, badges, data previews (including truncation and the
short-data path), link builders, translation status, and the dynamic
``register_stream_event_admin`` guard.
"""

import pytest
from django.contrib import admin as django_admin
from django.test import RequestFactory

from django_rakaia.admin import (
    StreamAdmin,
    StreamEntryAdmin,
    StreamEventAdmin,
    register_stream_event_admin,
)
from django_rakaia.event_message import NO_LABEL_DISPLAY
from django_rakaia.models import Stream, StreamEntry, StreamEvent
from tests.test_django_rakaia.models import AppStreamEvent

pytestmark = pytest.mark.django_db


def _entry(stream_id: str, offset: int, event_type: str = "create", **data):
    stream, _ = Stream.objects.get_or_create(stream_id=stream_id)
    event = StreamEvent.objects.create(event_type=event_type, data=data or {"k": "v"})
    entry = StreamEntry.objects.create(stream=stream, event=event, offset=offset)
    return stream, event, entry


class _AdminUser:
    """Enough of a staff user for `get_changelist_instance`, without a row.

    Deliberately not a real `auth.User`: saving one fires the test app's
    `post_save` receiver, which appends a `create` event — so the very screen
    under test would gain rows, and its filter an extra choice.
    """

    is_active = True
    is_staff = True
    is_superuser = True

    def has_perm(self, perm, obj=None) -> bool:  # noqa: ARG002
        return True

    def has_module_perms(self, app_label) -> bool:  # noqa: ARG002
        return True


def _changelist(model_admin, **query: str):
    """The changelist a staff user's GET would build, filters and all."""
    request = RequestFactory().get("/", query)
    request.user = _AdminUser()  # type: ignore[assignment]
    return request, model_admin.get_changelist_instance(request)


def _filter_choices(model_admin, parameter_name: str, **query: str):
    """`(display, query_string)` per choice of one filter, as the sidebar shows.

    Goes through the real `ChangeList`, so it sees what the screen renders rather
    than what a helper returns — the distinction that let #195 and #208 each fix
    one surface and leave another printing the sentinel.
    """
    _, changelist = _changelist(model_admin, **query)
    for spec in changelist.filter_specs:
        if parameter_name in spec.expected_parameters():
            return [
                (str(choice["display"]), choice["query_string"])
                for choice in spec.choices(changelist)
            ]
    raise AssertionError(f"no filter for {parameter_name!r} on {model_admin}")


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

    def test_the_smaller_surfaces_agree_with_the_badge(self) -> None:
        # The two places #208 left behind, side by side with the badge that was
        # fixed: the sidebar filter and the change-form header. All three
        # describe one event, so all three must call it the same thing.
        _, entry = self._appended()
        event = entry.event

        assert NO_LABEL_DISPLAY in StreamEventAdmin(
            StreamEvent, django_admin.site
        ).event_type_badge(event)
        assert str(event) == f"Event #{event.id} ({NO_LABEL_DISPLAY})"
        assert (NO_LABEL_DISPLAY, "?event_type=append") in _filter_choices(
            StreamEventAdmin(StreamEvent, django_admin.site), "event_type"
        )

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


class TestTheTypeFilterShowsLabelsAndFiltersOnTheColumn:
    """The sidebar beside the badge, and the header above the change form.

    #208 fixed the badges and deliberately left these: a filter needs the
    *stored* value to build its query, so the label can only be the text of the
    link. That made the two disagree — one event read as an em-dash in the table
    and `append` in the filter next to it (#210).

    So each case asserts both halves: the display is the label, and the value in
    the querystring is still the column's. Rendered through a real `ChangeList`,
    because a test of the filter class in isolation is the shape that missed this
    twice.
    """

    def _events(self):
        """One labelless append and one labelled event, through the store."""
        from django_rakaia.django_store import DjangoStreamStore
        from rakaia import AppendOptions

        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"n": 1}')
        store.append("s", b'{"n": 2}', AppendOptions(label="create"))
        appended, created = StreamEvent.objects.order_by("id")
        return appended, created

    def _event_admin(self):
        return StreamEventAdmin(StreamEvent, django_admin.site)

    def _entry_admin(self):
        return StreamEntryAdmin(StreamEntry, django_admin.site)

    def test_the_event_filter_shows_the_label_not_the_stored_type(self) -> None:
        self._events()
        assert _filter_choices(self._event_admin(), "event_type") == [
            ("All", "?"),
            (NO_LABEL_DISPLAY, "?event_type=append"),
            ("create", "?event_type=create"),
        ]

    def test_the_entries_filter_shows_the_label_not_the_stored_type(self) -> None:
        self._events()
        assert _filter_choices(self._entry_admin(), "event__event_type") == [
            ("All", "?"),
            (NO_LABEL_DISPLAY, "?event__event_type=append"),
            ("create", "?event__event_type=create"),
        ]

    def test_a_registered_event_models_filter_shows_the_label(self) -> None:
        # The factory an app registers its own event model through: the copy
        # #201 found still printing the sentinel after #195 fixed the built-in
        # screens. Same mistake, same place to make it again.
        AppStreamEvent.objects.create(event_type="append", data={"n": 1})
        AppStreamEvent.objects.create(event_type="create", data={"n": 2})
        admin = django_admin.site._registry[AppStreamEvent]

        assert _filter_choices(admin, "event_type") == [
            ("All", "?"),
            (NO_LABEL_DISPLAY, "?event_type=append"),
            ("create", "?event_type=create"),
        ]

    def test_the_querystring_is_exactly_what_the_plain_field_filter_built(
        self,
    ) -> None:
        # The constraint from the issue: a display-name change, not a swap of the
        # value. Proved against the filter that was there before — a throwaway
        # admin with `list_filter = ["event_type"]`, i.e. Django's
        # `AllValuesFieldListFilter` — so an existing bookmark still selects the
        # same rows.
        self._events()

        class PlainFieldFilterAdmin(django_admin.ModelAdmin):
            list_filter = ["event_type"]

        before = _filter_choices(
            PlainFieldFilterAdmin(StreamEvent, django_admin.site), "event_type"
        )
        after = _filter_choices(self._event_admin(), "event_type")

        assert [q for _, q in before] == [q for _, q in after]
        # And the display is the only thing that moved.
        assert [d for d, _ in before] == ["All", "append", "create"]

    def test_the_event_filter_still_selects_the_labelless_append(self) -> None:
        appended, _ = self._events()
        _, changelist = _changelist(self._event_admin(), event_type="append")
        assert list(changelist.queryset) == [appended]

    def test_the_event_filter_still_selects_a_labelled_event(self) -> None:
        _, created = self._events()
        _, changelist = _changelist(self._event_admin(), event_type="create")
        assert list(changelist.queryset) == [created]

    def test_the_event_filter_selects_everything_when_unset(self) -> None:
        appended, created = self._events()
        _, changelist = _changelist(self._event_admin())
        assert set(changelist.queryset) == {appended, created}

    def test_the_entries_filter_still_selects_the_labelless_append(self) -> None:
        appended, _ = self._events()
        _, changelist = _changelist(self._entry_admin(), event__event_type="append")
        assert [entry.event for entry in changelist.queryset] == [appended]

    def test_the_change_form_header_shows_the_label_not_the_stored_type(self) -> None:
        appended, created = self._events()
        # `StreamEvent.__str__` — the change-form header, and what a
        # `readonly_fields` reference to an event renders. Exact equality: the
        # sentinel is not a substring of the label here, so containment would
        # pass against the raw column too.
        assert str(appended) == f"Event #{appended.id} ({NO_LABEL_DISPLAY})"
        assert str(created) == f"Event #{created.id} (create)"


class TestRelabellingTheFilterChangedNothingElseAboutIt:
    """Everything a link selects is Django's answer, not ours.

    The first attempt at #210 rebuilt the filter as a `SimpleListFilter`, which
    means re-implementing every behaviour of the one it replaced. Two came out
    wrong: a repeated parameter — which Django ORs — selected only the last
    value, and the entries sidebar listed its choices by joining the fan-out
    table instead of reading the event table directly.

    Each case here pins one of those against the plain field filter, which is
    what the screens used before, so "display-only" is a tested claim rather
    than an intention.
    """

    def _plain(self, model, list_filter):
        class PlainFieldFilterAdmin(django_admin.ModelAdmin):
            pass

        PlainFieldFilterAdmin.list_filter = list_filter
        return PlainFieldFilterAdmin(model, django_admin.site)

    def _events(self):
        for event_type in ("append", "create", "delete"):
            event = StreamEvent.objects.create(event_type=event_type, data={})
            if event_type != "delete":
                # `delete` deliberately has no entry: the entries sidebar must
                # still offer it, which is what says its choices come from the
                # event table rather than a join.
                StreamEntry.objects.create(
                    stream=Stream.objects.get_or_create(stream_id="s")[0],
                    event=event,
                    offset=event.id,
                )

    def test_a_repeated_parameter_still_selects_both_types(self) -> None:
        self._events()
        query = "?event_type=append&event_type=create"
        request = RequestFactory().get(f"/{query}")
        request.user = _AdminUser()  # type: ignore[assignment]

        admin = StreamEventAdmin(StreamEvent, django_admin.site)
        changelist = admin.get_changelist_instance(request)
        assert sorted(event.event_type for event in changelist.queryset) == [
            "append",
            "create",
        ]

    def test_a_repeated_parameter_selects_the_same_rows_as_before(self) -> None:
        self._events()
        query = "?event__event_type=append&event__event_type=create"
        request = RequestFactory().get(f"/{query}")
        request.user = _AdminUser()  # type: ignore[assignment]
        other = RequestFactory().get(f"/{query}")
        other.user = _AdminUser()  # type: ignore[assignment]

        plain = self._plain(StreamEntry, ["event__event_type"])
        admin = StreamEntryAdmin(StreamEntry, django_admin.site)
        # Sorted: the two admins order their lists differently by design, and it
        # is *which rows* the link selects that must not have moved.
        now = sorted(e.pk for e in admin.get_changelist_instance(request).queryset)
        before = sorted(e.pk for e in plain.get_changelist_instance(other).queryset)
        assert now == before

    def test_the_entries_sidebar_offers_a_type_no_entry_carries(self) -> None:
        # Django's field filter reads the *event* table for a related path. A
        # hand-rolled `lookups()` over the entries queryset both dropped this
        # choice and made the sidebar scan the fan-out table.
        self._events()
        displays = [
            display
            for display, _ in _filter_choices(
                StreamEntryAdmin(StreamEntry, django_admin.site), "event__event_type"
            )
        ]
        assert displays == ["All", NO_LABEL_DISPLAY, "create", "delete"]

    def test_facet_counts_survive_the_relabelling(self) -> None:
        # With `?_facets=True` Django appends " (N)" to each choice. Relabelling
        # replaces the value and keeps the count.
        self._events()
        assert _filter_choices(
            StreamEventAdmin(StreamEvent, django_admin.site),
            "event_type",
            _facets="True",
        ) == [
            ("All", "?_facets=True"),
            (f"{NO_LABEL_DISPLAY} (1)", "?_facets=True&event_type=append"),
            ("create (1)", "?_facets=True&event_type=create"),
            ("delete (1)", "?_facets=True&event_type=delete"),
        ]
