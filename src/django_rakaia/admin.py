"""
Django admin configuration for normalized stream events.

Provides an interface for viewing and managing streams, events, and entries
in the Django admin.
"""

import json
from typing import Any

from django.contrib import admin
from django.utils.html import format_html, format_html_join
from django.utils.safestring import SafeString, mark_safe

from django_rakaia.event_message import (
    decode_payload,
    event_label,
    event_label_display,
)
from django_rakaia.models import Stream, StreamEntry, StreamEvent


@admin.register(Stream)
class StreamAdmin(admin.ModelAdmin):
    """Admin interface for Stream model."""

    list_display = ["stream_id", "event_count", "last_entry_offset", "created_at"]
    search_fields = ["stream_id"]
    readonly_fields = ["stream_id", "created_at"]
    ordering = ["-created_at"]
    list_per_page = 50

    @admin.display(description="Events")
    def event_count(self, obj):
        return obj.entries.count()

    @admin.display(description="Last Offset")
    def last_entry_offset(self, obj):
        last = obj.entries.order_by("-offset").first()
        return last.offset if last else None


_PREVIEW_CHARS = 100


def _truncate(text: str) -> str:
    """`text`, cut to the preview budget. One rule, so the two branches agree."""
    return text if len(text) <= _PREVIEW_CHARS else text[: _PREVIEW_CHARS - 3] + "..."


_BADGE_COLORS = {
    "create": "#28a745",
    "update": "#ffc107",
    "delete": "#dc3545",
}


def _event_badge(event_type: str) -> SafeString:
    """The label badge for a stored ``event_type``. One home for three screens.

    Goes through `event_label`, not the column: `event_type` holds a sentinel for
    "a raw append, which carried no label", and rendering it raw showed `APPEND`
    where every other reader of the same event reports no label at all.
    `event_label`'s own docstring says callers rendering an event must use it
    rather than the column (#153).

    This exists as a function because the admin had three copies of the badge and
    #195 fixed one of them, leaving the entries list and — worse — the badge a
    consumer inherits when they register their own event model still printing the
    sentinel (#201). The colour table was duplicated three times alongside it.
    A labelless append shows an em-dash rather than a blank cell, because an
    empty badge reads as a rendering fault.
    """
    label = event_label(event_type)
    return format_html(
        '<span style="background-color: {}; color: white; padding: 3px 8px; '
        'border-radius: 3px; font-size: 11px; font-weight: bold;">{}</span>',
        _BADGE_COLORS.get(label, "#6c757d"),
        event_label_display(event_type).upper(),
    )


class EventTypeFilter(admin.SimpleListFilter):
    """The event-type sidebar filter, showing labels rather than the column.

    A *display-name* change, not a swap of the value. A filter has to put the
    stored value in the querystring and match on the stored column, or it selects
    nothing; only the text beside the link is presentational. So `lookups()`
    pairs each stored `event_type` with what `event_label_display` says a reader
    sees, and `queryset()` filters on the raw column.

    Django's own `AllValuesFieldListFilter` — what `list_filter = ["event_type"]`
    resolves to — prints the column, so the screen said `append` in the sidebar
    while the badge one column over said "no label" for the same event (#210).
    `parameter_name` is deliberately the field path, which is exactly the key
    that filter used, so an existing bookmark or link keeps working.
    """

    title = "event type"
    parameter_name = "event_type"

    def lookups(self, request, model_admin):
        stored = (
            model_admin.get_queryset(request)
            .order_by(self.parameter_name)
            .values_list(self.parameter_name, flat=True)
            .distinct()
        )
        return [(value, event_label_display(value)) for value in stored]

    def queryset(self, request, queryset):  # noqa: ARG002 - Django's signature
        value = self.value()
        if value is None:
            return queryset
        return queryset.filter(**{self.parameter_name: value})


class EntryEventTypeFilter(EventTypeFilter):
    """`EventTypeFilter` for the entries list, which reaches its event.

    Only the path differs, and it is one attribute because `parameter_name` is
    both the querystring key and the ORM path — the same two roles Django's
    field filter gives `field_path`.
    """

    parameter_name = "event__event_type"


def _event_data_preview(data: Any, payload_encoding: str | None) -> str:
    """The payload as a reader sees it, not as the column happens to hold it.

    A body that is not JSON is stored as text — or base64, when it is not valid
    UTF-8 — and marked with `payload_encoding`, which `read()` inverts. Dumping
    the column ignored that, so a binary payload rendered as its base64 with
    nothing on screen saying so. `decode_payload` is the same inverse `read()`
    uses, so the two now agree.

    Shared with the badge's motive: the event-model factory carried its own stale
    copy of this, still ignoring the encoding and still truncating by its own
    rule, so the fix in #195 never reached the screens an app registers (#201).
    """
    if payload_encoding is not None:
        payload = decode_payload(data, payload_encoding)
        try:
            shown = payload.decode("utf-8")
        except UnicodeDecodeError:
            # Genuinely not text. Say what it is rather than printing an
            # encoding the viewer has no way to recognise.
            return f"<{len(payload)} bytes, {payload_encoding}>"
        return _truncate(shown)
    try:
        return _truncate(json.dumps(data, indent=2))
    except (TypeError, ValueError):
        return str(data)


@admin.register(StreamEvent)
class StreamEventAdmin(admin.ModelAdmin):
    """Admin interface for StreamEvent model."""

    list_display = [
        "id",
        "event_type_badge",
        "data_preview",
        "stream_count",
        "created_at",
    ]
    list_filter = [EventTypeFilter, "created_at"]
    search_fields = ["data"]
    readonly_fields = ["data", "event_type", "created_at", "streams_list"]
    ordering = ["-created_at"]
    list_per_page = 50

    @admin.display(description="Type")
    def event_type_badge(self, obj) -> SafeString:
        return _event_badge(obj.event_type)

    @admin.display(description="Data")
    def data_preview(self, obj: StreamEvent) -> str:
        return _event_data_preview(obj.data, obj.payload_encoding)

    @admin.display(description="Streams")
    def stream_count(self, obj):
        return obj.entries.count()

    @admin.display(description="Streams")
    def streams_list(self, obj):
        streams = obj.get_streams()
        if not streams:
            return "-"
        return format_html_join(
            mark_safe("<br>"), "<code>{}</code>", ((s,) for s in streams)
        )


@admin.register(StreamEntry)
class StreamEntryAdmin(admin.ModelAdmin):
    """Admin interface for StreamEntry model."""

    list_display = [
        "stream_link",
        "offset",
        "event_link",
        "event_type_badge",
        "created_at",
    ]
    list_filter = ["stream", EntryEventTypeFilter, "created_at"]
    search_fields = ["stream__stream_id", "event__data"]
    readonly_fields = ["stream", "event", "offset", "created_at"]
    ordering = ["-created_at"]
    list_per_page = 50
    date_hierarchy = "created_at"

    @admin.display(description="Stream")
    def stream_link(self, obj):
        url = f"/admin/django_rakaia/stream/{obj.stream.id}/change/"
        return format_html('<a href="{}">{}</a>', url, obj.stream.stream_id)

    @admin.display(description="Event")
    def event_link(self, obj):
        url = f"/admin/django_rakaia/streamevent/{obj.event.id}/change/"
        return format_html('<a href="{}">Event #{}</a>', url, obj.event.id)

    @admin.display(description="Type")
    def event_type_badge(self, obj) -> SafeString:
        return _event_badge(obj.event.event_type)


def register_stream_event_admin(event_model_class):
    """
    Register a concrete StreamEvent subclass with the admin.

    Usage:
        class AppStreamEvent(StreamEvent):
            class Meta:
                app_label = 'myapp'

        register_stream_event_admin(AppStreamEvent)
    """
    # Don't register the base StreamEvent model twice
    if event_model_class is StreamEvent:
        return

    # Don't register abstract models
    if (
        hasattr(event_model_class._meta, "abstract")
        and event_model_class._meta.abstract
    ):
        return

    class StreamEventSubclassAdmin(admin.ModelAdmin):
        list_display = [
            "id",
            "event_type_badge",
            "data_preview",
            "stream_count",
            "created_at",
        ]
        list_filter = [EventTypeFilter, "created_at"]
        search_fields = ["data"]
        readonly_fields = ["data", "event_type", "created_at", "streams_list"]
        ordering = ["-created_at"]
        list_per_page = 50

        @admin.display(description="Type")
        def event_type_badge(self, obj) -> SafeString:
            return _event_badge(obj.event_type)

        @admin.display(description="Data")
        def data_preview(self, obj) -> str:
            return _event_data_preview(obj.data, obj.payload_encoding)

        @admin.display(description="Streams")
        def stream_count(self, obj):
            return obj.entries.count()

        @admin.display(description="Streams")
        def streams_list(self, obj):
            streams = obj.get_streams()
            if not streams:
                return "-"
            return format_html_join(
                mark_safe("<br>"), "<code>{}</code>", ((s,) for s in streams)
            )

    admin.site.register(event_model_class, StreamEventSubclassAdmin)
