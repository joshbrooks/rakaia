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
from django.views.decorators.http import require_GET

from .channels_signals import _sanitize_group_name
from .models import StreamEntry


@require_GET
async def stream_events_sse(_request: Any, stream_id: str) -> Any:
    """SSE endpoint for real-time stream event updates via channel layer.

    On (re)connect, replays historical entries strictly after the
    `Last-Event-ID` header (sent by the EventSource client when it
    auto-reconnects). On a fresh connect that header is absent, so we
    replay the full history. Each event is tagged with `id: <offset>`
    so the browser knows where to resume.
    """

    last_event_id = _request.headers.get("Last-Event-ID", "")
    try:
        after_offset = int(last_event_id) if last_event_id else 0
    except ValueError:
        after_offset = 0

    async def event_generator():
        channel_layer = get_channel_layer()
        assert channel_layer is not None
        channel = await channel_layer.new_channel()
        group_name = _sanitize_group_name(f"stream.{stream_id}")
        await channel_layer.group_add(group_name, channel)

        try:
            # Replay entries the client hasn't seen.
            entries_qs = (
                StreamEntry.objects.select_related("event")
                .filter(stream__stream_id=stream_id, offset__gt=after_offset)
                .order_by("offset")
            )
            async for entry in entries_qs:
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
                yield f"id: {entry.offset}\ndata: {json.dumps(data)}\n\n".encode()

            # Wait for new events from channel layer
            while True:
                message = await channel_layer.receive(channel)
                # Group names sanitize many-to-one ("a/b" and "a.b" share a
                # group, as do long ids truncated to their last 99 chars), so
                # membership alone doesn't prove the event is this stream's —
                # filter on the exact id in the payload. A message without one
                # predates the field; forward it rather than dropping live
                # traffic during a rolling deploy.
                sender = message.get("stream_id")
                if sender is not None and sender != stream_id:
                    continue
                # Extract the payload (exclude the channel-layer 'type' and the
                # routing-only 'stream_id', keeping the client-visible shape)
                payload = {
                    k: v for k, v in message.items() if k not in ("type", "stream_id")
                }
                event = payload.get("event") or {}
                offset = event.get("offset")
                prefix = f"id: {offset}\n" if offset is not None else ""
                yield f"{prefix}data: {json.dumps(payload)}\n\n".encode()

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
