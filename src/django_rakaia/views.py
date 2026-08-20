"""
Django views for the Data Streams dashboard.

Provides a web interface for viewing and monitoring stream events in real-time.
Uses the normalized Stream/StreamEvent/StreamEntry model structure.
"""

import logging
from collections.abc import Iterable
from datetime import datetime
from typing import Any, Protocol, cast

from django.contrib.auth.decorators import login_required
from django.db.models import Count, Max, Min
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from rakaia.types import InvalidOffset

from .event_message import event_label, event_view
from .models import Stream, StreamEntry, StreamEvent
from .offsets import parse_offset

_log = logging.getLogger("django_rakaia.views")


class _AnnotatedStream(Protocol):
    """A ``Stream`` row as it comes back from the statistics ``annotate()``.

    ``QuerySet.annotate()`` attaches its aliases to each row at runtime, so they
    exist on the instance but not on the model class, and no static checker can
    infer them. Naming the shape here — rather than reaching for ``Any`` — keeps
    the aliases and their types written down next to the query that produces
    them, so a rename in one place is caught in the other.
    """

    stream_id: str
    event_count: int
    min_offset: int | None
    max_offset: int | None
    last_event: datetime | None


@login_required
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

    # Get event type breakdown. Inverted like every other event_type this page
    # renders, so the breakdown and the recent-events table below name the same
    # events the same way rather than one saying "append" and the other "" (#153).
    event_types = [
        {"event_type": event_label(row["event_type"]), "count": row["count"]}
        for row in StreamEvent.objects.values("event_type")
        .annotate(count=Count("id"))
        .order_by("-count")
    ]

    # Get recent events
    recent_entries = (
        StreamEntry.objects.select_related("event", "stream")
        .order_by("-created_at")[:10]
        .values(
            "stream__stream_id",
            "offset",
            "event__event_type",
            "event__data",
            "event__payload_encoding",
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
            event_view(
                event_type=e["event__event_type"],
                data=e["event__data"],
                payload_encoding=e["event__payload_encoding"],
                created_at=e["created_at"],
                stream_id=e["stream__stream_id"],
                offset=e["offset"],
            )
            for e in recent_entries
        ],
        "total_streams": stream_stats.count(),
        "total_events": StreamEvent.objects.count(),
    }

    return render(_request, "django_rakaia/streams_index.html", context)


@login_required
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


@login_required
@require_GET
def stream_events_api(_request: Any, stream_id: str) -> Any:
    """
    API endpoint to get events for a stream.

    Supports pagination and filtering by offset.
    """
    # Get parameters
    after_offset = _request.GET.get("after_offset")
    try:
        limit = int(_request.GET.get("limit", 50))
    except (TypeError, ValueError):
        return JsonResponse({"error": "limit must be an integer"}, status=400)
    limit = max(1, min(limit, 200))

    # Get stream
    try:
        stream = Stream.objects.get(stream_id=stream_id)
    except Stream.DoesNotExist:
        return JsonResponse({"stream_id": stream_id, "events": [], "count": 0})

    # Build query
    query = stream.entries.select_related("event").order_by("offset")

    if after_offset:
        # Parsed by the durable store's own strict check, not `int()`: this
        # endpoint reads durable rows, and `int("0_5")` is 5 in Python, so an
        # in-memory-store offset would resolve to an unrelated position and
        # return the wrong window with a 200.
        try:
            after_offset_int = parse_offset(after_offset)
        except InvalidOffset:
            return JsonResponse(
                {"error": "after_offset must be an integer"}, status=400
            )
        query = query.filter(offset__gt=after_offset_int)

    # Get entries
    entries = list(
        query[:limit].values(
            "id",
            "offset",
            "event__id",
            "event__event_type",
            "event__data",
            "event__payload_encoding",
            "created_at",
        )
    )

    # Serialize
    serialized_events = [
        event_view(
            event_type=e["event__event_type"],
            data=e["event__data"],
            payload_encoding=e["event__payload_encoding"],
            created_at=e["created_at"],
            event_id=e["event__id"],
            offset=e["offset"],
        )
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


@login_required
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
        # `cast` is a no-op at runtime; it only records that `annotate()` put
        # the alias attributes on each row.
        for s in cast(Iterable[_AnnotatedStream], streams)
    ]

    return JsonResponse(
        {"streams": serialized_streams, "total": len(serialized_streams)}
    )
