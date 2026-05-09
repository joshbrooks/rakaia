"""
Django admin configuration for normalized stream events.

Provides an interface for viewing and managing streams, events, and entries
in the Django admin.
"""

import json
from typing import Any

from django.contrib import admin
from django.utils.html import format_html

from django_rakaia.models import Stream, StreamEntry, StreamEvent, Translatable


@admin.register(Stream)
class StreamAdmin(admin.ModelAdmin):
    """Admin interface for Stream model."""

    list_display = ["stream_id", "event_count", "last_entry_offset", "created_at"]
    search_fields = ["stream_id"]
    readonly_fields = ["stream_id", "created_at"]
    ordering = ["-created_at"]
    list_per_page = 50

    def event_count(self, obj):
        return obj.entries.count()

    event_count.short_description = "Events"

    def last_entry_offset(self, obj):
        last = obj.entries.order_by("-offset").first()
        return last.offset if last else None

    last_entry_offset.short_description = "Last Offset"


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
    list_filter = ["event_type", "created_at"]
    search_fields = ["data"]
    readonly_fields = ["data", "event_type", "created_at", "streams_list"]
    ordering = ["-created_at"]
    list_per_page = 50

    def event_type_badge(self, obj):
        colors = {
            "create": "#28a745",
            "update": "#ffc107",
            "delete": "#dc3545",
        }
        color = colors.get(obj.event_type, "#6c757d")
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 8px; '
            'border-radius: 3px; font-size: 11px; font-weight: bold;">{}</span>',
            color,
            obj.event_type.upper(),
        )

    event_type_badge.short_description = "Type"

    def data_preview(self, obj: StreamEvent) -> str:
        try:
            # Explicitly type the JSONField data
            data: Any = obj.data  # type: ignore[assignment]
            data_str = json.dumps(data, indent=2)
            if len(data_str) > 100:
                return data_str[:97] + "..."
        except (TypeError, ValueError):
            return str(obj.data)  # type: ignore[call-overload]

    data_preview.short_description = "Data"

    def stream_count(self, obj):
        return obj.entries.count()

    stream_count.short_description = "Streams"

    def streams_list(self, obj):
        streams = obj.get_streams()
        if not streams:
            return "-"
        return format_html("<br>".join(f"<code>{s}</code>" for s in streams))

    streams_list.short_description = "Streams"


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
    list_filter = ["stream", "event__event_type", "created_at"]
    search_fields = ["stream__stream_id", "event__data"]
    readonly_fields = ["stream", "event", "offset", "created_at"]
    ordering = ["-created_at"]
    list_per_page = 50
    date_hierarchy = "created_at"

    def stream_link(self, obj):
        url = f"/admin/django_rakaia/stream/{obj.stream.id}/change/"
        return format_html('<a href="{}">{}</a>', url, obj.stream.stream_id)

    stream_link.short_description = "Stream"

    def event_link(self, obj):
        url = f"/admin/django_rakaia/streamevent/{obj.event.id}/change/"
        return format_html('<a href="{}">Event #{}</a>', url, obj.event.id)

    event_link.short_description = "Event"

    def event_type_badge(self, obj):
        colors = {
            "create": "#28a745",
            "update": "#ffc107",
            "delete": "#dc3545",
        }
        color = colors.get(obj.event.event_type, "#6c757d")
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 8px; '
            'border-radius: 3px; font-size: 11px; font-weight: bold;">{}</span>',
            color,
            obj.event.event_type.upper(),
        )

    event_type_badge.short_description = "Type"


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
        list_filter = ["event_type", "created_at"]
        search_fields = ["data"]
        readonly_fields = ["data", "event_type", "created_at", "streams_list"]
        ordering = ["-created_at"]
        list_per_page = 50

        def event_type_badge(self, obj):
            colors = {
                "create": "#28a745",
                "update": "#ffc107",
                "delete": "#dc3545",
            }
            color = colors.get(obj.event_type, "#6c757d")
            return format_html(
                '<span style="background-color: {}; color: white; padding: 3px 8px; '
                'border-radius: 3px; font-size: 11px; font-weight: bold;">{}</span>',
                color,
                obj.event_type.upper(),
            )

        event_type_badge.short_description = "Type"

        def data_preview(self, obj):
            try:
                data_str = json.dumps(obj.data, indent=2)
                if len(data_str) > 100:
                    data_str = data_str[:97] + "..."
                return data_str.replace("\n", "<br>").replace(" ", "&nbsp;")
            except (TypeError, ValueError):
                return str(obj.data)

        data_preview.short_description = "Data"

        def stream_count(self, obj):
            return obj.entries.count()

        stream_count.short_description = "Streams"

        def streams_list(self, obj):
            streams = obj.get_streams()
            if not streams:
                return "-"
            return format_html("<br>".join(f"<code>{s}</code>" for s in streams))

        streams_list.short_description = "Streams"

    admin.site.register(event_model_class, StreamEventSubclassAdmin)


@admin.register(Translatable)
class TranslatableAdmin(admin.ModelAdmin):
    """Admin interface for Translatable model."""

    list_display = [
        "msgid",
        "msgstr_preview",
        "langcode_badge",
        "domain",
        "msgctxt",
        "deleted_status",
    ]
    list_filter = ["langcode", "domain", "deleted"]
    search_fields = ["msgid", "msgstr", "msgctxt"]
    ordering = ["msgid", "langcode"]
    list_per_page = 50

    fieldsets = (
        (None, {"fields": ("msgid", "msgstr", "langcode")}),
        (
            "Advanced Options",
            {"fields": ("domain", "msgctxt", "deleted"), "classes": ("collapse",)},
        ),
    )

    def msgstr_preview(self, obj):
        if not obj.msgstr:
            return format_html(
                '<span style="color: #999; font-style: italic;">Not translated</span>'
            )

        # Truncate long translations
        if len(obj.msgstr) > 50:
            return obj.msgstr[:47] + "..."
        return obj.msgstr

    msgstr_preview.short_description = "Translation"

    def langcode_badge(self, obj):
        colors = {
            "en": "#007bff",  # Blue
            "fr": "#28a745",  # Green
            "es": "#fd7e14",  # Orange
            "pt": "#6f42c1",  # Purple
            "id": "#20c997",  # Teal
            "tet": "#e83e8c",  # Pink
        }
        color = colors.get(obj.langcode, "#6c757d")
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 8px; '
            'border-radius: 3px; font-size: 11px; font-weight: bold;">{}</span>',
            color,
            obj.langcode.upper(),
        )

    langcode_badge.short_description = "Language"

    def deleted_status(self, obj):
        if obj.deleted:
            return format_html(
                '<span style="color: #dc3545; font-weight: bold;">Deleted</span>'
            )
        return format_html('<span style="color: #28a745;">Active</span>')

    deleted_status.short_description = "Status"

    def get_queryset(self, request):
        # Optimize queries for better performance
        return super().get_queryset(request).select_related()
