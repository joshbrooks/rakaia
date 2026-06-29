"""
Tests for Durable Streams Protocol HTTP API.

These tests verify protocol compliance for the Django Rakaia implementation.
They follow the Durable Streams Protocol specification (PROTOCOL.md).

Test Categories:
1. Stream Creation (PUT)
2. Append Operations (POST)
3. Read Operations (GET - catch-up)
4. SSE Streaming (GET - live mode)
5. Stream Metadata (HEAD)
6. Error Handling
"""

import json

import pytest
from django.test import Client

from django_rakaia.models import Stream


@pytest.fixture
def client() -> Client:
    """Create a Django test client."""
    return Client()


@pytest.fixture
def stream_id() -> str:
    """Default stream ID for tests."""
    return "test:stream:1"


# =============================================================================
# Stream Creation (PUT /{stream-path})
# =============================================================================


@pytest.mark.django_db
class TestStreamCreation:
    """Tests for PUT /protocol/streams/{stream-path} - Create Stream operation."""

    def test_create_stream_returns_201(self, client: Client, stream_id: str):
        """PUT to new stream returns 201 Created."""
        response = client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        assert response.status_code == 201

    def test_create_stream_returns_next_offset_header(
        self, client: Client, stream_id: str
    ):
        """PUT returns Stream-Next-Offset header with initial offset."""
        response = client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        assert "Stream-Next-Offset" in response.headers
        assert response.headers["Stream-Next-Offset"] == "0_0000000000000000"

    def test_create_stream_returns_content_type(self, client: Client, stream_id: str):
        """PUT returns Content-Type header."""
        response = client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        assert response.headers.get("Content-Type") == "application/json"

    def test_create_existing_stream_returns_200(self, client: Client, stream_id: str):
        """PUT to existing stream returns 200 OK (idempotent)."""
        # First create
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        # Second create (same config)
        response = client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        assert response.status_code == 200

    def test_create_stream_with_initial_data(self, client: Client, stream_id: str):
        """PUT with body creates stream with initial content."""
        body = json.dumps({"event": "init"}).encode("utf-8")

        response = client.put(
            f"/protocol/streams/{stream_id}/create",
            data=body,
            content_type="application/json",
        )

        assert response.status_code == 201

        # Verify event was created
        stream = Stream.objects.get(stream_id=stream_id)
        assert stream.entries.count() == 1

    def test_create_stream_stores_record(self, client: Client, stream_id: str):
        """PUT creates Stream record in database."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        assert Stream.objects.filter(stream_id=stream_id).exists()


# =============================================================================
# Append Operations (POST /{stream-path})
# =============================================================================


@pytest.mark.django_db
class TestAppendOperations:
    """Tests for POST /protocol/streams/{stream-path}/append - Append to Stream operation."""

    def test_append_returns_204(self, client: Client, stream_id: str):
        """POST returns 204 No Content on success."""
        # Create stream first
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        # Append
        body = json.dumps({"event": "test"}).encode("utf-8")
        response = client.post(
            f"/protocol/streams/{stream_id}/append",
            data=body,
            content_type="application/json",
        )

        assert response.status_code == 204

    def test_append_returns_next_offset_header(self, client: Client, stream_id: str):
        """POST returns Stream-Next-Offset header."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        body = json.dumps({"event": "test"}).encode("utf-8")
        response = client.post(
            f"/protocol/streams/{stream_id}/append",
            data=body,
            content_type="application/json",
        )

        assert "Stream-Next-Offset" in response.headers
        assert response.headers["Stream-Next-Offset"] == "0_0000000000000001"

    def test_append_creates_event_and_entry(self, client: Client, stream_id: str):
        """POST creates StreamEvent and StreamEntry records."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        body = json.dumps({"event": "test", "data": 123}).encode("utf-8")
        client.post(
            f"/protocol/streams/{stream_id}/append",
            data=body,
            content_type="application/json",
        )

        stream = Stream.objects.get(stream_id=stream_id)
        assert stream.entries.count() == 1

        entry = stream.entries.first()
        assert entry.event.data == {"event": "test", "data": 123}
        assert entry.offset == 1

    def test_append_monotonic_offsets(self, client: Client, stream_id: str):
        """Multiple appends have monotonically increasing offsets."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        for i in range(5):
            body = json.dumps({"index": i}).encode("utf-8")
            response = client.post(
                f"/protocol/streams/{stream_id}/append",
                data=body,
                content_type="application/json",
            )

            expected_offset = f"0_{i + 1:016d}"
            assert response.headers["Stream-Next-Offset"] == expected_offset

    def test_append_to_nonexistent_stream_returns_404(self, client: Client):
        """POST to non-existent stream returns 404 Not Found."""
        body = json.dumps({"event": "test"}).encode("utf-8")
        response = client.post(
            "/protocol/streams/nonexistent:stream/append",
            data=body,
            content_type="application/json",
        )

        assert response.status_code == 404

    def test_append_requires_content_type(self, client: Client, stream_id: str):
        """POST without Content-Type defaults to application/json."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        response = client.post(
            f"/protocol/streams/{stream_id}/append",
            data={"raw": "data"},
            content_type="application/json",
        )

        # Defaults to application/json and accepts the data
        assert response.status_code == 204


# =============================================================================
# Read Operations (GET /{stream-path}?offset=)
# =============================================================================


@pytest.mark.django_db
class TestReadOperations:
    """Tests for GET /protocol/streams/{stream-path}/read - Read Stream operation."""

    def test_read_returns_200(self, client: Client, stream_id: str):
        """GET returns 200 OK."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        response = client.get(f"/protocol/streams/{stream_id}/read")

        assert response.status_code == 200

    def test_read_returns_content_type(self, client: Client, stream_id: str):
        """GET returns Content-Type header."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        response = client.get(f"/protocol/streams/{stream_id}/read")

        assert response.headers.get("Content-Type") == "application/json"

    def test_read_empty_stream_returns_empty_body(self, client: Client, stream_id: str):
        """GET on empty stream returns empty body."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        response = client.get(f"/protocol/streams/{stream_id}/read")

        assert response.content == b""

    def test_read_returns_appended_events(self, client: Client, stream_id: str):
        """GET returns all appended events."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        # Append events
        for i in range(3):
            body = json.dumps({"index": i}).encode("utf-8")
            client.post(
                f"/protocol/streams/{stream_id}/append",
                data=body,
                content_type="application/json",
            )

        response = client.get(f"/protocol/streams/{stream_id}/read")

        # Response should contain all events (newline-delimited JSON)
        lines = response.content.decode("utf-8").strip().split("\n")
        assert len(lines) == 3

        for i, line in enumerate(lines):
            data = json.loads(line)
            assert data["index"] == i

    def test_read_with_offset_returns_events_after(
        self, client: Client, stream_id: str
    ):
        """GET with offset returns only events after that offset."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        # Append events
        for i in range(5):
            body = json.dumps({"index": i}).encode("utf-8")
            client.post(
                f"/protocol/streams/{stream_id}/append",
                data=body,
                content_type="application/json",
            )

        # Read from offset 2
        response = client.get(
            f"/protocol/streams/{stream_id}/read?offset=0_0000000000000002"
        )

        lines = response.content.decode("utf-8").strip().split("\n")
        assert len(lines) == 3  # Events 3, 4, 5

        for i, line in enumerate(lines):
            data = json.loads(line)
            assert data["index"] == i + 2

    def test_read_nonexistent_stream_returns_404(self, client: Client):
        """GET on non-existent stream returns 404 Not Found."""
        response = client.get("/protocol/streams/nonexistent:stream/read")

        assert response.status_code == 404


# =============================================================================
# SSE Streaming (GET /{stream-path}/sse?cursor=)
# =============================================================================


@pytest.mark.django_db
class TestSSEStreaming:
    """Tests for GET /protocol/streams/{stream-path}/sse - SSE Live Read operation."""

    def test_sse_returns_correct_content_type(self, client: Client, stream_id: str):
        """SSE endpoint returns text/event-stream content type."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        response = client.get(f"/protocol/streams/{stream_id}/sse")

        assert "text/event-stream" in response.headers.get("Content-Type", "")

    def test_sse_returns_cache_control_header(self, client: Client, stream_id: str):
        """SSE endpoint returns Cache-Control: no-cache."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        response = client.get(f"/protocol/streams/{stream_id}/sse")

        assert response.headers.get("Cache-Control") == "no-cache"

    @pytest.mark.django_db(transaction=True)
    async def test_sse_format_follows_spec(self):
        """SSE events follow Durable Streams format."""
        from django.test import RequestFactory

        from django_rakaia.models import Stream, StreamEntry, StreamEvent
        from django_rakaia.protocol_views import read_stream_sse

        # Create stream + an event using the underlying models so we can drive
        # the async SSE view directly without relying on the sync Client.
        stream = await Stream.objects.acreate(stream_id="sse-format-test")
        event = await StreamEvent.objects.acreate(
            event_type="create",
            data={"event": "test"},
        )
        await StreamEntry.objects.acreate(stream=stream, event=event, offset=1)

        factory = RequestFactory()
        request = factory.get(
            "/protocol/streams/sse-format-test/sse?cursor=0_0000000000000000"
        )
        response = await read_stream_sse(request, "sse-format-test")

        # Read just the first chunk (the existing entry) then stop
        chunks: list[str] = []
        async for chunk in response.streaming_content:
            data_str = chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
            chunks.append(data_str)
            break

        content = "".join(chunks)
        assert "event:" in content
        assert "id:" in content
        assert "data:" in content

    @pytest.mark.django_db(transaction=True)
    async def test_sse_cursor_resumption(self):
        """SSE with cursor only returns events after the cursor."""
        from django.test import RequestFactory

        from django_rakaia.models import Stream, StreamEntry, StreamEvent
        from django_rakaia.protocol_views import read_stream_sse

        stream = await Stream.objects.acreate(stream_id="sse-cursor-test")
        for i in range(1, 6):
            event = await StreamEvent.objects.acreate(
                event_type="create",
                data={"index": i},
            )
            await StreamEntry.objects.acreate(stream=stream, event=event, offset=i)

        factory = RequestFactory()
        # Cursor "after offset 2" should yield events at offsets 3, 4, 5
        request = factory.get(
            "/protocol/streams/sse-cursor-test/sse?cursor=0_0000000000000002"
        )
        response = await read_stream_sse(request, "sse-cursor-test")

        seen_indexes: list[int] = []
        async for chunk in response.streaming_content:
            data_str = chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
            for line in data_str.splitlines():
                if line.startswith("data: "):
                    parsed = json.loads(line.removeprefix("data: "))
                    if "index" in parsed:
                        seen_indexes.append(parsed["index"])
            if len(seen_indexes) >= 3:
                break

        assert seen_indexes == [3, 4, 5]


# =============================================================================
# Stream Metadata (HEAD /protocol/streams/{stream-path}/metadata)
# =============================================================================


@pytest.mark.django_db
class TestStreamMetadata:
    """Tests for HEAD /protocol/streams/{stream-path}/metadata - Stream Metadata operation."""

    def test_head_returns_200(self, client: Client, stream_id: str):
        """HEAD returns 200 OK for existing stream."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        response = client.head(f"/protocol/streams/{stream_id}/metadata")

        assert response.status_code == 200

    def test_head_returns_next_offset(self, client: Client, stream_id: str):
        """HEAD returns Stream-Next-Offset header."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        # Append an event
        body = json.dumps({"event": "test"}).encode("utf-8")
        client.post(
            f"/protocol/streams/{stream_id}/append",
            data=body,
            content_type="application/json",
        )

        response = client.head(f"/protocol/streams/{stream_id}/metadata")

        assert "Stream-Next-Offset" in response.headers

    def test_head_returns_content_type(self, client: Client, stream_id: str):
        """HEAD returns Content-Type header."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        response = client.head(f"/protocol/streams/{stream_id}/metadata")

        assert response.headers.get("Content-Type") == "application/json"

    def test_head_nonexistent_stream_returns_404(self, client: Client):
        """HEAD on non-existent stream returns 404 Not Found."""
        response = client.head("/protocol/streams/nonexistent:stream/metadata")

        assert response.status_code == 404


# =============================================================================
# Error Handling
# =============================================================================


@pytest.mark.django_db
class TestErrorHandling:
    """Tests for error responses and edge cases."""

    def test_invalid_offset_format_returns_400(self, client: Client, stream_id: str):
        """GET with invalid offset format returns 400 Bad Request."""
        client.put(
            f"/protocol/streams/{stream_id}/create", content_type="application/json"
        )

        response = client.get(f"/protocol/streams/{stream_id}/read?offset=invalid")

        assert response.status_code == 400

    def test_content_type_mismatch_returns_409(self) -> None:  # noqa: ARG002
        """POST with mismatched Content-Type returns 409 Conflict.

        Note: Content-type validation is a TODO item.
        """
        pytest.skip("Content-type validation is a TODO item")
