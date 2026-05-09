"""
Django views for the Data Streams dashboard.

Provides a web interface for viewing and monitoring stream events in real-time.
Uses the normalized Stream/StreamEvent/StreamEntry model structure.
"""

import json
from typing import Any

from django.contrib.auth.decorators import login_required
from django.db.models import Count, Max, Min
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from .models import Stream, StreamEntry, StreamEvent, Translatable


@require_GET
def streams_index(_request: Any) -> HttpResponse:
    """
    Display a list of all active streams.

    Shows stream ID, event count, first/last offset, and last update time.
    """
    # Get stream statistics
    stream_stats = (
        Stream.objects.annotate(
            event_count=Count("entries"),
            min_offset=Min("entries__offset"),
            max_offset=Max("entries__offset"),
            last_event=Max("entries__created_at"),
        )
        .filter(event_count__gt=0)
        .order_by("-last_event")
    )

    # Get event type breakdown
    event_types = (
        StreamEvent.objects.values("event_type")
        .annotate(count=Count("id"))
        .order_by("-count")
    )

    # Get recent events
    recent_entries = (
        StreamEntry.objects.select_related("event", "stream")
        .order_by("-created_at")[:10]
        .values(
            "stream__stream_id",
            "offset",
            "event__event_type",
            "event__data",
            "created_at",
        )
    )

    context = {
        "streams": [
            {
                "stream_id": s["stream_id"],
                "event_count": s["event_count"],
                "min_offset": s["min_offset"],
                "max_offset": s["max_offset"],
                "last_event": s["last_event"],
            }
            for s in stream_stats.values(
                "stream_id", "event_count", "min_offset", "max_offset", "last_event"
            )
        ],
        "event_types": list(event_types),
        "recent_events": [
            {
                "stream_id": e["stream__stream_id"],
                "offset": e["offset"],
                "event_type": e["event__event_type"],
                "created_at": e["created_at"].isoformat() if e["created_at"] else None,
                "data": e["event__data"],
            }
            for e in recent_entries
        ],
        "total_streams": stream_stats.count(),
        "total_events": StreamEvent.objects.count(),
    }

    return render(_request, "django_rakaia/streams_index.html", context)


@require_GET
def stream_detail(_request: Any, stream_id: str) -> Any:
    """
    Display details for a specific stream with real-time updates.

    Shows all events for the stream and uses Server-Sent Events (SSE)
    to push new events as they occur.
    """
    # Get or check stream exists
    try:
        stream = Stream.objects.get(stream_id=stream_id)
    except Stream.DoesNotExist:
        context = {
            "stream_id": stream_id,
            "exists": False,
            "stats": {},
            "events": [],
        }
        return render(_request, "django_rakaia/stream_detail.html", context)

    # Get stream statistics
    stats = stream.entries.aggregate(
        event_count=Count("id"),
        min_offset=Min("offset"),
        max_offset=Max("offset"),
        first_event=Min("created_at"),
        last_event=Max("created_at"),
    )

    if stats["event_count"] == 0:
        context = {
            "stream_id": stream_id,
            "exists": True,
            "stats": {
                "event_count": 0,
                "min_offset": None,
                "max_offset": None,
                "first_event": None,
                "last_event": None,
            },
            "events": [],  # Keep empty array for compatibility
        }
        return render(_request, "django_rakaia/stream_detail.html", context)

    # Don't load entries in the template - let JavaScript load them via API
    context = {
        "stream_id": stream_id,
        "exists": True,
        "stats": {
            "event_count": stats["event_count"],
            "min_offset": stats["min_offset"],
            "max_offset": stats["max_offset"],
            "first_event": stats["first_event"].isoformat()
            if stats["first_event"]
            else None,
            "last_event": stats["last_event"].isoformat()
            if stats["last_event"]
            else None,
        },
        "events": [],  # Empty - will be loaded via AJAX
    }

    return render(_request, "django_rakaia/stream_detail.html", context)


@require_GET
def stream_events_api(_request: Any, stream_id: str) -> Any:
    """
    API endpoint to get events for a stream.

    Supports pagination and filtering by offset.
    """
    # Get parameters
    after_offset = _request.GET.get("after_offset")
    limit = int(_request.GET.get("limit", 50))

    # Get stream
    try:
        stream = Stream.objects.get(stream_id=stream_id)
    except Stream.DoesNotExist:
        return JsonResponse({"stream_id": stream_id, "events": [], "count": 0})

    # Build query
    query = stream.entries.select_related("event").order_by("offset")

    if after_offset:
        query = query.filter(offset__gt=int(after_offset))

    # Get entries
    entries = list(
        query[:limit].values(
            "id",
            "offset",
            "event__id",
            "event__event_type",
            "event__data",
            "created_at",
        )
    )

    # Serialize
    serialized_events = [
        {
            "id": e["event__id"],
            "offset": e["offset"],
            "event_type": e["event__event_type"],
            "created_at": e["created_at"].isoformat() if e["created_at"] else None,
            "data": e["event__data"],
        }
        for e in entries
    ]

    return JsonResponse(
        {
            "stream_id": stream_id,
            "events": serialized_events,
            "count": len(serialized_events),
            "has_more": len(entries) == limit,
        }
    )


@require_GET
def streams_api(_request: Any) -> Any:
    """
    API endpoint to get all streams with statistics.
    """
    streams = (
        Stream.objects.annotate(
            event_count=Count("entries"),
            min_offset=Min("entries__offset"),
            max_offset=Max("entries__offset"),
            last_event=Max("entries__created_at"),
        )
        .filter(event_count__gt=0)
        .order_by("-last_event")
    )

    # Serialize
    serialized_streams = [
        {
            "stream_id": s.stream_id,
            "event_count": s.event_count,
            "min_offset": s.min_offset,
            "max_offset": s.max_offset,
            "last_event": s.last_event.isoformat() if s.last_event else None,
        }
        for s in streams
    ]

    return JsonResponse(
        {"streams": serialized_streams, "total": len(serialized_streams)}
    )


def _filtered_translations(request: Any) -> Any:
    """Build the filtered translations queryset shared by HTMX views."""
    langcode = request.GET.get("langcode") or request.POST.get("langcode_filter") or ""
    msgid_filter = request.GET.get("msgid") or request.POST.get("msgid_filter") or ""

    query = Translatable.objects.all()
    if langcode:
        query = query.filter(langcode=langcode)
    if msgid_filter:
        query = query.filter(msgid__icontains=msgid_filter)
    return query.order_by("msgid", "langcode")[:100]


@require_GET
@login_required
def translations_table_htmx(request: Any) -> HttpResponse:
    """Return the rendered <tr> rows for HTMX swap-in."""
    translations = _filtered_translations(request)
    return render(
        request,
        "django_rakaia/_translation_rows.html",
        {"translations": translations},
    )


@require_POST
@login_required
def translation_create_htmx(request: Any) -> HttpResponse:
    """Form-encoded create/update via HTMX. Returns the refreshed table body.

    Also creates a StreamEvent so the channels signal handler broadcasts the
    change to the `translations` group, which the SSE activity feed consumes.
    """
    msgid = (request.POST.get("msgid") or "").strip()
    msgstr = (request.POST.get("msgstr") or "").strip()
    langcode = request.POST.get("langcode") or "en"
    domain = request.POST.get("domain") or None
    msgctxt = request.POST.get("msgctxt") or None

    if not msgid:
        return HttpResponse("msgid is required", status=400)

    translatable, created = Translatable.objects.update_or_create(
        msgid=msgid,
        langcode=langcode,
        msgctxt=msgctxt or "",
        defaults={"msgstr": msgstr, "domain": domain},
    )

    # Emit a StreamEvent so the channels signal broadcasts to the
    # `translations` group. The HTML SSE view consumes that group.
    StreamEvent.objects.create(
        event_type="create" if created else "update",
        data={
            "translatable_id": translatable.id,
            "msgid": translatable.msgid,
            "msgstr": translatable.msgstr,
            "langcode": translatable.langcode,
            "username": request.user.get_username(),
            "url": request.path,
        },
    )

    translations = _filtered_translations(request)
    return render(
        request,
        "django_rakaia/_translation_rows.html",
        {"translations": translations},
    )


@require_GET
@login_required
def translations_index(request: Any) -> Any:
    """
    Display translation management interface.
    """
    # Get available languages
    languages = Translatable.objects.values_list("langcode", flat=True).distinct()

    # Get recent translation events
    recent_streams = StreamEvent.objects.filter(
        data__has_key="translatable_id"
    ).order_by(  # Filter for translation events
        "-created_at"
    )[:10]

    context = {
        "languages": list(languages),
        "recent_streams": [
            {
                "id": stream.id,
                "user": stream.data.get("username", "Unknown"),
                "langcode": stream.data.get("langcode", ""),
                "url": stream.data.get("url", ""),
                "translatable": {
                    "msgid": stream.data.get("msgid", ""),
                    "msgstr": stream.data.get("msgstr", ""),
                },
                "action": stream.event_type,
                "created_at": stream.created_at.isoformat()
                if stream.created_at
                else None,
            }
            for stream in recent_streams
        ],
    }

    return render(request, "django_rakaia/translations_index.html", context)


@require_GET
def translations_api(request: Any) -> Any:
    """
    API endpoint to get all translations with filtering options.
    """
    langcode = request.GET.get("langcode")
    msgid_filter = request.GET.get("msgid")

    query = Translatable.objects.all()

    if langcode:
        query = query.filter(langcode=langcode)
    if msgid_filter:
        query = query.filter(msgid__icontains=msgid_filter)

    translations = query.order_by("msgid", "langcode")[:100]

    serialized = [
        {
            "id": t.id,
            "msgid": t.msgid,
            "msgstr": t.msgstr,
            "langcode": t.langcode,
            "domain": t.domain,
            "msgctxt": t.msgctxt,
            "deleted": t.deleted.isoformat() if t.deleted else None,
        }
        for t in translations
    ]

    return JsonResponse(
        {
            "translations": serialized,
            "total": len(serialized),
            "languages": list(
                Translatable.objects.values_list("langcode", flat=True).distinct()
            ),
        }
    )


@require_POST
@csrf_exempt
@login_required
def translation_create_update_api(request: Any) -> Any:
    """
    API endpoint to create or update translations.
    """
    try:
        data = json.loads(request.body)
        msgid = data.get("msgid")
        msgstr = data.get("msgstr")
        langcode = data.get("langcode", "en")
        domain = data.get("domain")
        msgctxt = data.get("msgctxt")

        if not msgid:
            return JsonResponse({"error": "msgid is required"}, status=400)

        # Get or create translation
        translatable, created = Translatable.objects.get_or_create(
            msgid=msgid,
            langcode=langcode,
            msgctxt=msgctxt or "",
            defaults={
                "msgstr": msgstr,
                "domain": domain,
            },
        )

        if not created:
            translatable.msgstr = msgstr
            if domain:
                translatable.domain = domain
            translatable.save()
            action = "update"
        else:
            action = "create"

        # The Translatable.save() method automatically creates StreamEvent and StreamEntry
        # No need to manually create TranslationStream anymore

        return JsonResponse(
            {
                "success": True,
                "translation": {
                    "id": translatable.id,
                    "msgid": translatable.msgid,
                    "msgstr": translatable.msgstr,
                    "langcode": translatable.langcode,
                    "domain": translatable.domain,
                    "msgctxt": translatable.msgctxt,
                },
                "action": action,
            }
        )

    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
