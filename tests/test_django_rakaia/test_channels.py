"""Tests for Django Channels integration.

Tests that model saves broadcast to the channel layer and that
SSE views consume from the channel layer correctly.
"""

import asyncio
import json

import pytest
from channels.layers import get_channel_layer

from django_rakaia.models import Stream, StreamEntry, StreamEvent


def _extract_data_payload(chunk: bytes | str) -> dict | None:
    """Pull the JSON payload from an SSE chunk.

    A chunk looks like one of:
        b"data: {...}\\n\\n"
        b"id: 17\\ndata: {...}\\n\\n"      # post-Last-Event-ID fix

    Returns None if the chunk has no `data:` line (e.g. a keepalive).
    """
    text = chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
    for line in text.splitlines():
        if line.startswith("data: "):
            return json.loads(line.removeprefix("data: ").strip())
    return None


async def _collect_payloads(streaming_content, count: int, timeout: float = 3.0):
    """Read SSE chunks until `count` payloads collected. Times out fast on hang."""
    payloads: list[dict] = []

    async def _consume():
        async for chunk in streaming_content:
            payload = _extract_data_payload(chunk)
            if payload is not None:
                payloads.append(payload)
            if len(payloads) >= count:
                break

    await asyncio.wait_for(_consume(), timeout=timeout)
    return payloads


@pytest.mark.django_db(transaction=True)
class TestChannelLayerSignals:
    """Test that model saves broadcast to the channel layer."""

    async def test_stream_entry_created_sends_to_stream_group(self):
        """Creating a StreamEntry sends a message to the stream's channel group."""
        channel_layer = get_channel_layer()
        assert channel_layer is not None

        # Subscribe to the stream group
        channel = await channel_layer.new_channel()
        group_name = "stream.test-stream-1"
        await channel_layer.group_add(group_name, channel)

        # Create stream, event, and entry
        stream = await Stream.objects.acreate(stream_id="test-stream-1")
        event = await StreamEvent.objects.acreate(
            event_type="create",
            data={"key": "value"},
        )
        await StreamEntry.objects.acreate(
            stream=stream,
            event=event,
            offset=1,
        )

        # Should receive the broadcast
        message = await asyncio.wait_for(channel_layer.receive(channel), timeout=2.0)
        assert message["type"] == "stream.event"
        assert message["event"]["id"] == event.id
        assert message["event"]["offset"] == 1
        assert message["event"]["event_type"] == "create"
        assert message["event"]["data"] == {"key": "value"}

        await channel_layer.group_discard(group_name, channel)

    async def test_stream_event_with_translatable_sends_to_translations_group(self):
        """Creating a StreamEvent with translatable_id sends to 'translations' group."""
        channel_layer = get_channel_layer()
        assert channel_layer is not None

        channel = await channel_layer.new_channel()
        await channel_layer.group_add("translations", channel)

        await StreamEvent.objects.acreate(
            event_type="create",
            data={
                "translatable_id": 42,
                "msgid": "hello",
                "msgstr": "ola",
                "langcode": "tet",
                "username": "testuser",
                "url": "/admin/",
            },
        )

        message = await asyncio.wait_for(channel_layer.receive(channel), timeout=2.0)
        assert message["type"] == "translation.event"
        assert message["stream"]["translatable"]["id"] == 42
        assert message["stream"]["translatable"]["msgid"] == "hello"
        assert message["stream"]["action"] == "create"

        await channel_layer.group_discard("translations", channel)

    async def test_stream_event_without_translatable_does_not_broadcast(self):
        """Creating a StreamEvent without translatable_id sends nothing to translations."""
        channel_layer = get_channel_layer()
        assert channel_layer is not None

        channel = await channel_layer.new_channel()
        await channel_layer.group_add("translations", channel)

        await StreamEvent.objects.acreate(
            event_type="create",
            data={"key": "value"},
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(channel_layer.receive(channel), timeout=0.5)

        await channel_layer.group_discard("translations", channel)

    async def test_message_format_matches_sse_payload(self):
        """Verify the stream event message payload matches the expected SSE JSON structure."""
        channel_layer = get_channel_layer()
        assert channel_layer is not None

        channel = await channel_layer.new_channel()
        await channel_layer.group_add("stream.format-test", channel)

        stream = await Stream.objects.acreate(stream_id="format-test")
        event = await StreamEvent.objects.acreate(
            event_type="update",
            data={"foo": "bar"},
        )
        await StreamEntry.objects.acreate(
            stream=stream,
            event=event,
            offset=5,
        )

        message = await asyncio.wait_for(channel_layer.receive(channel), timeout=2.0)

        # The "event" key should contain exactly these fields
        evt = message["event"]
        assert set(evt.keys()) == {"id", "offset", "event_type", "created_at", "data"}
        assert evt["offset"] == 5
        assert evt["event_type"] == "update"
        assert evt["data"] == {"foo": "bar"}
        # created_at should be an ISO format string
        assert isinstance(evt["created_at"], str)

        await channel_layer.group_discard("stream.format-test", channel)


@pytest.mark.django_db(transaction=True)
class TestChannelLayerSSEViews:
    """Test that SSE views subscribe to channel layer and yield events."""

    async def test_stream_sse_yields_existing_entries(self):
        """SSE view sends existing StreamEntry records on connect."""
        from django_rakaia.channels_views import stream_events_sse

        # Create existing data
        stream = await Stream.objects.acreate(stream_id="existing-test")
        event = await StreamEvent.objects.acreate(
            event_type="create",
            data={"pre": "existing"},
        )
        await StreamEntry.objects.acreate(stream=stream, event=event, offset=1)

        # Create a fake request
        from django.test import RequestFactory

        factory = RequestFactory()
        request = factory.get("/api/streams/existing-test/sse/")

        response = await stream_events_sse(request, "existing-test")

        assert response["Content-Type"] == "text/event-stream"
        assert response["Cache-Control"] == "no-cache"
        assert response["X-Accel-Buffering"] == "no"

        chunks = await _collect_payloads(response.streaming_content, count=1)
        assert len(chunks) == 1
        assert chunks[0]["event"]["data"] == {"pre": "existing"}
        assert chunks[0]["event"]["offset"] == 1

    async def test_stream_sse_yields_new_events_from_channel_layer(self):
        """SSE view yields events pushed via channel_layer.group_send()."""
        from django_rakaia.channels_views import stream_events_sse

        # Create stream (no existing entries)
        await Stream.objects.acreate(stream_id="live-test")

        from django.test import RequestFactory

        factory = RequestFactory()
        request = factory.get("/api/streams/live-test/sse/")

        response = await stream_events_sse(request, "live-test")

        # Push an event via channel layer after a short delay
        channel_layer = get_channel_layer()
        assert channel_layer is not None

        async def push_event():
            await asyncio.sleep(0.2)
            await channel_layer.group_send(
                "stream.live-test",
                {
                    "type": "stream.event",
                    "event": {
                        "id": 99,
                        "offset": 1,
                        "event_type": "create",
                        "created_at": "2026-01-01T00:00:00+00:00",
                        "data": {"live": True},
                    },
                },
            )

        push_task = asyncio.create_task(push_event())
        chunks = await _collect_payloads(response.streaming_content, count=1)
        await push_task

        assert len(chunks) == 1
        assert chunks[0]["event"]["data"] == {"live": True}

    async def test_translation_sse_yields_translation_events(self):
        """Translation SSE view yields events from 'translations' group."""
        from django.test import RequestFactory

        from django_rakaia.channels_views import translation_streams_sse

        factory = RequestFactory()
        request = factory.get("/api/translations/sse/")

        response = await translation_streams_sse(request)

        assert response["Content-Type"] == "text/event-stream"

        channel_layer = get_channel_layer()
        assert channel_layer is not None

        async def push_translation():
            await asyncio.sleep(0.2)
            await channel_layer.group_send(
                "translations",
                {
                    "type": "translation.event",
                    "stream": {
                        "id": 1,
                        "user": "admin",
                        "langcode": "tet",
                        "url": "/admin/",
                        "action": "create",
                        "translatable": {
                            "id": 42,
                            "msgid": "hello",
                            "msgstr": "ola",
                            "langcode": "tet",
                        },
                        "created_at": "2026-01-01T00:00:00+00:00",
                    },
                },
            )

        push_task = asyncio.create_task(push_translation())
        chunks = await _collect_payloads(response.streaming_content, count=1)
        await push_task

        assert chunks[0]["stream"]["translatable"]["msgid"] == "hello"

    async def test_sse_response_headers(self):
        """SSE response has correct content-type and cache headers."""
        from django_rakaia.channels_views import stream_events_sse

        await Stream.objects.acreate(stream_id="header-test")

        from django.test import RequestFactory

        factory = RequestFactory()
        request = factory.get("/api/streams/header-test/sse/")

        response = await stream_events_sse(request, "header-test")

        assert response["Content-Type"] == "text/event-stream"
        assert response["Cache-Control"] == "no-cache"
        assert response["X-Accel-Buffering"] == "no"
