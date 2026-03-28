"""
Django views for the Data Streams dashboard.

Provides a web interface for viewing and monitoring stream events in real-time.
Uses the normalized Stream/StreamEvent/StreamEntry model structure.
"""

import asyncio
import json
from typing import Any

from django.db.models import Count, Max, Min, Q
from django.http import JsonResponse, StreamingHttpResponse
from django.shortcuts import render, get_object_or_404
from django.views.decorators.http import require_GET, require_POST
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required

from django_rakaia.models import Stream, StreamEntry, StreamEvent, Translatable


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
    entries = list(query[:limit].values("id", "offset", "event__id", "event__event_type", "event__data", "created_at"))

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
async def stream_events_sse(_request: Any, stream_id: str) -> Any:
    """
    Server-Sent Events endpoint for real-time stream event updates.

    Streams new events as they are created using SSE protocol.
    """

    async def event_generator():
        """Generate SSE events for new stream events."""
        last_offset = 0

        try:
            latest_entry = await (
                StreamEntry.objects.filter(stream__stream_id=stream_id)
                .order_by("-offset")
                .afirst()
            )
            if latest_entry is not None:
                last_offset = latest_entry.offset
        except Stream.DoesNotExist:
            pass

        while True:
            try:
                new_entries = (
                    StreamEntry.objects.select_related("event")
                    .filter(stream__stream_id=stream_id)
                    .filter(offset__gt=last_offset)
                    .order_by("offset")
                )

                async for entry in new_entries:
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
                    yield f"data: {json.dumps(data)}\n\n".encode('utf-8')
            except asyncio.CancelledError:
                return
            except Stream.DoesNotExist:
                pass

            await asyncio.sleep(0.1)

    response = StreamingHttpResponse(
        event_generator(),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


# Translation Management Views
@require_GET
@login_required
def translations_index(request: Any) -> Any:
    """
    Display translation management interface.
    """
    # Get available languages
    languages = Translatable.objects.values_list('langcode', flat=True).distinct()
    
    # Get recent translation events
    recent_streams = (
        StreamEvent.objects
        .filter(data__has_key="translatable_id")  # Filter for translation events
        .order_by("-created_at")[:10]
    )
    
    context = {
        'languages': list(languages),
        'recent_streams': [
            {
                'id': stream.id,
                'user': stream.data.get('username', 'Unknown'),
                'langcode': stream.data.get('langcode', ''),
                'url': stream.data.get('url', ''),
                'translatable': {
                    'msgid': stream.data.get('msgid', ''),
                    'msgstr': stream.data.get('msgstr', ''),
                },
                'action': stream.event_type,
                'created_at': stream.created_at.isoformat() if stream.created_at else None,
            }
            for stream in recent_streams
        ],
    }
    
    return render(request, 'django_rakaia/translations_index.html', context)


@require_GET
def translations_api(request: Any) -> Any:
    """
    API endpoint to get all translations with filtering options.
    """
    langcode = request.GET.get('langcode')
    msgid_filter = request.GET.get('msgid')
    
    query = Translatable.objects.all()
    
    if langcode:
        query = query.filter(langcode=langcode)
    if msgid_filter:
        query = query.filter(msgid__icontains=msgid_filter)
    
    translations = query.order_by('msgid', 'langcode')[:100]
    
    serialized = [
        {
            'id': t.id,
            'msgid': t.msgid,
            'msgstr': t.msgstr,
            'langcode': t.langcode,
            'domain': t.domain,
            'msgctxt': t.msgctxt,
            'deleted': t.deleted.isoformat() if t.deleted else None,
        }
        for t in translations
    ]
    
    return JsonResponse({
        'translations': serialized,
        'total': len(serialized),
        'languages': list(Translatable.objects.values_list('langcode', flat=True).distinct())
    })


@require_POST
@csrf_exempt
@login_required
def translation_create_update_api(request: Any) -> Any:
    """
    API endpoint to create or update translations.
    """
    try:
        data = json.loads(request.body)
        msgid = data.get('msgid')
        msgstr = data.get('msgstr')
        langcode = data.get('langcode', 'en')
        domain = data.get('domain')
        msgctxt = data.get('msgctxt')
        url = data.get('url', '/translations/')
        
        if not msgid:
            return JsonResponse({'error': 'msgid is required'}, status=400)
        
        # Get or create translation
        translatable, created = Translatable.objects.get_or_create(
            msgid=msgid,
            langcode=langcode,
            msgctxt=msgctxt or '',
            defaults={
                'msgstr': msgstr,
                'domain': domain,
            }
        )
        
        if not created:
            translatable.msgstr = msgstr
            if domain:
                translatable.domain = domain
            translatable.save()
            action = 'update'
        else:
            action = 'create'
        
        # The Translatable.save() method automatically creates StreamEvent and StreamEntry
        # No need to manually create TranslationStream anymore
        
        return JsonResponse({
            'success': True,
            'translation': {
                'id': translatable.id,
                'msgid': translatable.msgid,
                'msgstr': translatable.msgstr,
                'langcode': translatable.langcode,
                'domain': translatable.domain,
                'msgctxt': translatable.msgctxt,
            },
            'action': action
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_GET
async def translation_streams_sse(request: Any) -> Any:
    """
    Server-Sent Events endpoint for real-time translation updates.
    """
    
    async def event_generator():
        """Generate SSE events for new translation streams."""
        last_id = 0
        
        try:
            latest_stream = await (
                StreamEvent.objects.filter(data__has_key="translatable_id")
                .order_by("-id")
                .afirst()
            )
            if latest_stream is not None:
                last_id = latest_stream.id
        except Exception:
            pass

        while True:
            try:
                new_streams = (
                    StreamEvent.objects
                    .filter(data__has_key="translatable_id")
                    .filter(id__gt=last_id)
                    .order_by("id")
                )
                
                async for stream in new_streams:
                    last_id = stream.id
                    data = {
                        'stream': {
                            'id': stream.id,
                            'user': stream.data.get('username', 'Unknown'),
                            'langcode': stream.data.get('langcode', ''),
                            'url': stream.data.get('url', ''),
                            'action': stream.event_type,
                            'translatable': {
                                'id': stream.data.get('translatable_id'),
                                'msgid': stream.data.get('msgid', ''),
                                'msgstr': stream.data.get('msgstr', ''),
                                'langcode': stream.data.get('langcode', ''),
                            },
                            'created_at': stream.created_at.isoformat() if stream.created_at else None,
                        }
                    }
                    yield f"data: {json.dumps(data)}\n\n".encode('utf-8')
            except asyncio.CancelledError:
                return
            except Exception:
                pass
            
            await asyncio.sleep(0.1)
    
    response = StreamingHttpResponse(
        event_generator(),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response
