"""
Django views for the Data Streams dashboard.

Provides a web interface for viewing and monitoring stream events in real-time.
Uses the normalized Stream/StreamEvent/StreamEntry model structure.
"""

import json
import time
from typing import Any

from django.db.models import Count, Max, Min
from django.http import JsonResponse, StreamingHttpResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from django_rakaia.models import Stream, StreamEntry, StreamEvent


@require_GET
def streams_index(_request: Any) -> Any:
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
            "events": [],
        }
        return render(_request, "django_rakaia/stream_detail.html", context)

    # Get entries for initial display
    entries = (
        stream.entries.select_related("event")
        .order_by("offset")[:100]
        .values(
            "id",
            "offset",
            "event__id",
            "event__event_type",
            "event__data",
            "created_at",
        )
    )

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
        "events": [
            {
                "id": e["event__id"],
                "offset": e["offset"],
                "event_type": e["event__event_type"],
                "created_at": e["created_at"].isoformat() if e["created_at"] else None,
                "data": e["event__data"],
            }
            for e in entries
        ],
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
    entries = list(query[:limit].values("id", "offset", "event__event_type", "event__data", "created_at"))

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

    return JsonResponse({"streams": serialized_streams, "total": len(serialized_streams)})


@require_GET
def stream_events_sse(_request: Any, stream_id: str) -> Any:
    """
    Server-Sent Events endpoint for real-time stream event updates.

    Streams new events as they are created using SSE protocol.
    """

    def event_generator():
        """Generate SSE events for new stream events."""
        last_offset = 0

        # Get the current max offset for this stream
        try:
            stream = Stream.objects.get(stream_id=stream_id)
            stats = stream.entries.aggregate(max_offset=Max("offset"))
            if stats["max_offset"]:
                last_offset = stats["max_offset"]
        except Stream.DoesNotExist:
            pass

        while True:
            # Check for new events
            try:
                stream = Stream.objects.get(stream_id=stream_id)
                new_entries = (
                    stream.entries.select_related("event")
                    .filter(offset__gt=last_offset)
                    .order_by("offset")
                )

                for entry in new_entries:
                    last_offset = entry.offset
                    data = {
                        "event": {
                            "id": entry.event.id,
                            "offset": entry.offset,
                            "event_type": entry.event.event_type,
                            "created_at": entry.event.created_at.isoformat()
                            if entry.event.created_at
                            else None,
                            "data": entry.event.data,
                        }
                    }
                    yield f"data: {json.dumps(data)}\n\n"
            except Stream.DoesNotExist:
                pass

            # Wait before polling again
            time.sleep(1)

    response = StreamingHttpResponse(
        event_generator(),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response
