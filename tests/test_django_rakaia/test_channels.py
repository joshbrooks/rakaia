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

    async def test_stream_sse_filters_out_a_group_mates_events(self):
        """Distinct stream ids that sanitize alike must not cross-deliver.

        Group names replace every disallowed character with "." — "a/b" and
        "a.b" share the group "stream.a.b" — so the consumer filters on the
        exact stream id carried in the payload, not on group membership.
        """
        from django_rakaia.channels_views import stream_events_sse

        await Stream.objects.acreate(stream_id="a.b")

        from django.test import RequestFactory

        factory = RequestFactory()
        request = factory.get("/api/streams/a.b/sse/")
        response = await stream_events_sse(request, "a.b")

        channel_layer = get_channel_layer()
        assert channel_layer is not None

        async def push_events():
            await asyncio.sleep(0.2)
            # Both land in group "stream.a.b"; only the second is this
            # stream's. Same shape the signal sends, stream_id included.
            for sender, marker in (("a/b", "foreign"), ("a.b", "mine")):
                await channel_layer.group_send(
                    "stream.a.b",
                    {
                        "type": "stream.event",
                        "stream_id": sender,
                        "event": {
                            "id": 1,
                            "offset": 1,
                            "event_type": "create",
                            "created_at": "2026-01-01T00:00:00+00:00",
                            "data": {"from": marker},
                        },
                    },
                )

        push_task = asyncio.create_task(push_events())
        chunks = await _collect_payloads(response.streaming_content, count=1)
        await push_task

        assert chunks[0]["event"]["data"] == {"from": "mine"}
        assert "stream_id" not in chunks[0], "routing field must not leak to clients"

    async def test_sse_foreign_last_event_id_is_refused(self):
        """A ``Last-Event-ID`` this store never issued must not resume.

        ``int("0_5")`` is 5 in Python, so the in-memory store's compound
        ``{seq}_{byte}`` offset parses into an unrelated resume point; a
        foreign offset that does not parse at all used to fall back to 0 and
        replay the whole stream. Both are wrong: refuse instead.
        """
        from django_rakaia.channels_views import stream_events_sse

        stream = await Stream.objects.acreate(stream_id="foreign-offset")
        event = await StreamEvent.objects.acreate(event_type="create", data={"n": 1})
        await StreamEntry.objects.acreate(stream=stream, event=event, offset=1)

        from django.test import RequestFactory

        factory = RequestFactory()
        request = factory.get(
            "/api/streams/foreign-offset/sse/", HTTP_LAST_EVENT_ID="0_5"
        )

        response = await stream_events_sse(request, "foreign-offset")

        assert response.status_code == 400
        assert not getattr(response, "streaming", False)

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
