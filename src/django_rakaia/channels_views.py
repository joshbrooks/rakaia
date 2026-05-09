"""
SSE views backed by Django Channels' channel layer.

These async views subscribe to channel layer groups and yield events
as Server-Sent Events, replacing the custom asyncio.Queue/Unix socket/Redis approaches.
"""

import asyncio
import json
from typing import Any

from channels.layers import get_channel_layer
from django.http import StreamingHttpResponse
from django.template.loader import render_to_string
from django.views.decorators.http import require_GET

from .channels_signals import _sanitize_group_name
from .models import StreamEntry


def _format_sse_event(event_name: str, html: str) -> bytes:
    """Format an SSE message with a named event and (possibly multi-line) HTML data."""
    lines = html.splitlines() or [""]
    body = "".join(f"data: {line}\n" for line in lines)
    return f"event: {event_name}\n{body}\n".encode()


@require_GET
async def stream_events_sse(_request: Any, stream_id: str) -> Any:
    """SSE endpoint for real-time stream event updates via channel layer."""

    async def event_generator():
        channel_layer = get_channel_layer()
        assert channel_layer is not None
        channel = await channel_layer.new_channel()
        group_name = _sanitize_group_name(f"stream.{stream_id}")
        await channel_layer.group_add(group_name, channel)

        try:
            # Send existing entries first (one-time query)
            async for entry in (
                StreamEntry.objects.select_related("event")
                .filter(stream__stream_id=stream_id)
                .order_by("offset")[:20]
            ):
                data: dict[str, Any] = {
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
                yield f"data: {json.dumps(data)}\n\n".encode()

            # Wait for new events from channel layer
            while True:
                message = await channel_layer.receive(channel)
                # Extract the payload (exclude the 'type' key used by channel layer)
                payload = {k: v for k, v in message.items() if k != "type"}
                yield f"data: {json.dumps(payload)}\n\n".encode()

        except asyncio.CancelledError:
            return
        finally:
            await channel_layer.group_discard(group_name, channel)

    response = StreamingHttpResponse(
        event_generator(),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


@require_GET
async def translation_activity_sse_html(_request: Any) -> Any:
    """SSE endpoint emitting HTML fragments for HTMX `sse-swap`.

    Subscribes to the `translations` channel group and renders each event as
    an `_activity_item.html` partial, framed as an SSE `activity` event.
    """

    async def event_generator():
        channel_layer = get_channel_layer()
        assert channel_layer is not None
        channel = await channel_layer.new_channel()
        await channel_layer.group_add("translations", channel)

        try:
            while True:
                message = await channel_layer.receive(channel)
                item = message.get("stream", {})
                html = render_to_string(
                    "django_rakaia/_activity_item.html",
                    {"item": item},
                ).strip()
                yield _format_sse_event("activity", html)

        except asyncio.CancelledError:
            return
        finally:
            await channel_layer.group_discard("translations", channel)

    response = StreamingHttpResponse(
        event_generator(),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


@require_GET
async def translation_streams_sse(_request: Any) -> Any:
    """SSE endpoint for real-time translation event updates via channel layer."""

    async def event_generator():
        channel_layer = get_channel_layer()
        assert channel_layer is not None
        channel = await channel_layer.new_channel()
        await channel_layer.group_add("translations", channel)

        try:
            # Wait for translation events from channel layer
            while True:
                message = await channel_layer.receive(channel)
                payload = {k: v for k, v in message.items() if k != "type"}
                yield f"data: {json.dumps(payload)}\n\n".encode()

        except asyncio.CancelledError:
            return
        finally:
            await channel_layer.group_discard("translations", channel)

    response = StreamingHttpResponse(
        event_generator(),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response
