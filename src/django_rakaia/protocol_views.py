"""
Durable Streams Protocol HTTP API for Django.

This module implements the Durable Streams Protocol HTTP endpoints,
providing a protocol-compliant interface for frontend clients.

Protocol Specification: https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md

Endpoints:
    PUT    /protocol/streams/{stream_path}      - Create stream
    POST   /protocol/streams/{stream_path}      - Append to stream
    GET    /protocol/streams/{stream_path}      - Read stream (catch-up)
    GET    /protocol/streams/{stream_path}/sse  - Read stream (SSE live)
    HEAD   /protocol/streams/{stream_path}      - Get stream metadata

Usage:
    # urls.py
    from django_rakaia.protocol_views import protocol_urlpatterns

    urlpatterns = [
        path("protocol/", include(protocol_urlpatterns)),
    ]
"""

import asyncio
import json
import re

from django.db import transaction
from django.db.models import Max
from django.http import HttpResponse, StreamingHttpResponse
from django.urls import path
from django.views.decorators.http import require_http_methods

from django_rakaia.models import Stream, StreamEntry, StreamEvent

# =============================================================================
# Constants and Type Aliases
# =============================================================================

# Offset format: {read_seq}_{byte_offset} (both zero-padded to 16 digits)
OFFSET_FORMAT = "0_{:016d}"

# Valid offset pattern per protocol spec
VALID_OFFSET_PATTERN = re.compile(r"^(-1|now|\d+_\d+)$")

# Header names (protocol specification)
STREAM_NEXT_OFFSET_HEADER = "Stream-Next-Offset"
STREAM_CURSOR_HEADER = "Stream-Cursor"
STREAM_CLOSED_HEADER = "Stream-Closed"
CONTENT_TYPE_HEADER = "Content-Type"
CACHE_CONTROL_HEADER = "Cache-Control"


# =============================================================================
# Helper Functions (Single Responsibility)
# =============================================================================


def format_offset(offset: int) -> str:
    """
    Format an integer offset as a protocol-compliant offset string.

    Args:
        offset: Integer offset value.

    Returns:
        Formatted offset string (e.g., "0_0000000000000001").
    """
    return OFFSET_FORMAT.format(offset)


def parse_offset(offset_str: str) -> int:
    """
    Parse a protocol offset string to an integer.

    Args:
        offset_str: Offset string in format "read_seq_byte_offset".

    Returns:
        Integer byte offset.

    Raises:
        ValueError: If offset format is invalid.
    """
    if not offset_str or offset_str == "-1":
        return 0

    if offset_str == "now":
        # Return max offset (will be updated by caller)
        return 0

    try:
        parts = offset_str.split("_")
        if len(parts) != 2:
            raise ValueError(f"Invalid offset format: {offset_str}")
        return int(parts[1])
    except (ValueError, IndexError) as exc:
        raise ValueError(f"Invalid offset format: {offset_str}") from exc


def get_next_offset(stream: Stream) -> int:
    """
    Calculate the next monotonic offset for a stream.

    Must be called inside a transaction: locks the stream row with
    select_for_update() so concurrent appends serialize on offset
    allocation instead of colliding on unique_together(stream, offset).

    Thin wrapper over the canonical ``Stream.get_next_offset`` so every write
    path shares one row-locking implementation.

    Args:
        stream: The stream to calculate offset for.

    Returns:
        Next offset value (1-indexed).
    """
    return stream.get_next_offset()


def create_stream_record(stream_id: str, content_type: str) -> Stream:  # noqa: ARG001
    """
    Create or retrieve a Stream record.

    Args:
        stream_id: Unique stream identifier.
        content_type: Content type for the stream.

    Returns:
        The Stream instance (new or existing).
    """
    stream, _ = Stream.objects.get_or_create(stream_id=stream_id)
    return stream


# =============================================================================
# PUT /{stream-path} - Create Stream
# =============================================================================


@require_http_methods(["PUT"])
@transaction.atomic
def create_stream(request, stream_path: str) -> HttpResponse:
    """
    Create a new stream at the given path.

    Protocol: PUT /{stream-path}

    Request Headers:
        Content-Type: MIME type of the stream (default: application/octet-stream)
        Stream-TTL: Optional TTL in seconds
        Stream-Expires-At: Optional absolute expiry (RFC 3339)
        Stream-Closed: Optional, set to "true" to create closed stream

    Response:
        201 Created: Stream created successfully
        200 OK: Stream already exists with same config (idempotent)
        409 Conflict: Stream exists with different config

    Response Headers:
        Stream-Next-Offset: Initial offset (0_0000000000000000)
        Content-Type: The stream's content type
    """
    content_type = request.headers.get(CONTENT_TYPE_HEADER, "application/json")
    stream_closed = request.headers.get(STREAM_CLOSED_HEADER, "").lower() == "true"

    # Create or get stream
    stream, created = Stream.objects.get_or_create(stream_id=stream_path)

    # If body is provided, create initial event
    if request.body:
        next_offset = get_next_offset(stream)

        # Parse body based on content type
        if content_type == "application/json":
            try:
                data = json.loads(request.body.decode("utf-8"))
            except json.JSONDecodeError:
                data = {"raw": request.body.decode("utf-8", errors="replace")}
        else:
            data = {"raw": request.body.hex()}

        event = StreamEvent.objects.create(
            data=data,
            event_type="create",
        )
        StreamEntry.objects.create(
            stream=stream,
            event=event,
            offset=next_offset,
        )

    # Build response
    status_code = 201 if created else 200
    headers = {
        STREAM_NEXT_OFFSET_HEADER: format_offset(0),
        CONTENT_TYPE_HEADER: content_type,
    }

    if stream_closed:
        headers[STREAM_CLOSED_HEADER] = "true"

    return HttpResponse(status=status_code, headers=headers)


# =============================================================================
# POST /{stream-path} - Append to Stream
# =============================================================================


@require_http_methods(["POST"])
@transaction.atomic
def append_to_stream(request, stream_path: str) -> HttpResponse:
    """
    Append data to an existing stream.

    Protocol: POST /{stream-path}

    Request Headers:
        Content-Type: Must match stream's content type
        Stream-Seq: Optional sequence number for coordination
        Stream-Closed: Set to "true" to close stream after append
        Producer-Id: Optional producer ID for idempotent writes
        Producer-Epoch: Optional producer epoch
        Producer-Seq: Optional producer sequence number

    Response:
        204 No Content: Append successful
        404 Not Found: Stream does not exist
        409 Conflict: Content type mismatch or stream closed

    Response Headers:
        Stream-Next-Offset: New tail offset after append
        Stream-Closed: Present if stream is now closed
    """
    # Check stream exists
    try:
        stream = Stream.objects.get(stream_id=stream_path)
    except Stream.DoesNotExist:
        return HttpResponse(status=404)

    content_type = request.headers.get(CONTENT_TYPE_HEADER, "application/json")
    stream_closed = request.headers.get(STREAM_CLOSED_HEADER, "").lower() == "true"

    # Calculate next offset
    next_offset = get_next_offset(stream)

    # Parse body based on content type
    if content_type == "application/json":
        try:
            data = json.loads(request.body.decode("utf-8"))
        except json.JSONDecodeError:
            data = {"raw": request.body.decode("utf-8", errors="replace")}
    else:
        data = {"raw": request.body.hex() if request.body else ""}

    # Create event and entry
    event = StreamEvent.objects.create(
        data=data,
        event_type="append",
    )
    StreamEntry.objects.create(
        stream=stream,
        event=event,
        offset=next_offset,
    )

    # Build response
    headers = {
        STREAM_NEXT_OFFSET_HEADER: format_offset(next_offset),
    }

    if stream_closed:
        headers[STREAM_CLOSED_HEADER] = "true"

    return HttpResponse(status=204, headers=headers)


# =============================================================================
# GET /{stream-path}?offset= - Read Stream (Catch-up)
# =============================================================================


@require_http_methods(["GET", "HEAD"])
def read_stream(request, stream_path: str) -> HttpResponse:
    """
    Read events from a stream starting at the given offset.

    Protocol: GET /{stream-path}?offset={offset}

    Query Parameters:
        offset: Start reading after this offset (default: -1, meaning from beginning)
        cursor: Alternative to offset (for protocol compatibility)

    Response:
        200 OK: Returns concatenated event data
        404 Not Found: Stream does not exist
        400 Bad Request: Invalid offset format

    Response Headers:
        Content-Type: The stream's content type
        Stream-Next-Offset: Current tail offset
        Stream-Up-To-Date: "true" if all events returned
    """
    # Get offset from query params
    offset_str = request.GET.get("offset", request.GET.get("cursor", "-1"))

    # Validate offset format
    if not VALID_OFFSET_PATTERN.match(offset_str):
        return HttpResponse(
            status=400,
            content=f"Invalid offset format: {offset_str}".encode(),
        )

    # Parse offset
    try:
        offset = parse_offset(offset_str)
    except ValueError as exc:
        return HttpResponse(status=400, content=str(exc).encode("utf-8"))

    # Get stream
    try:
        stream = Stream.objects.get(stream_id=stream_path)
    except Stream.DoesNotExist:
        return HttpResponse(status=404)

    # Get entries after offset
    entries = (
        stream.entries.select_related("event")
        .filter(offset__gt=offset)
        .order_by("offset")
    )

    # Build response body (newline-delimited JSON for JSON streams)
    body_parts = []
    for entry in entries:
        body_parts.append(json.dumps(entry.event.data))

    body = "\n".join(body_parts).encode("utf-8") if body_parts else b""

    # Calculate next offset
    max_offset = stream.entries.aggregate(max_offset=Max("offset"))["max_offset"]
    next_offset = format_offset(max_offset) if max_offset else format_offset(0)

    # Build response
    headers = {
        STREAM_NEXT_OFFSET_HEADER: next_offset,
    }

    if not entries:
        headers["Stream-Up-To-Date"] = "true"

    return HttpResponse(body, content_type="application/json", headers=headers)


# =============================================================================
# GET /{stream-path}/sse?cursor= - Read Stream (SSE Live)
# =============================================================================


async def sse_event_generator(stream_id: str, cursor_offset: int):
    """
    Generate Server-Sent Events for a stream.

    Args:
        stream_id: The stream to stream events from.
        cursor_offset: Start streaming events after this offset.

    Yields:
        SSE-formatted event strings as bytes.
    """
    last_offset = cursor_offset

    try:
        await Stream.objects.aget(stream_id=stream_id)
    except Stream.DoesNotExist:
        yield f"event: error\ndata: {json.dumps({'error': 'Stream not found'})}\n\n".encode()
        return

    while True:
        try:
            entries = (
                StreamEntry.objects.select_related("event")
                .filter(stream__stream_id=stream_id)
                .filter(offset__gt=last_offset)
                .order_by("offset")
            )

            async for entry in entries:
                last_offset = entry.offset

                event_data = json.dumps(entry.event.data)
                yield (
                    f"event: message\n"
                    f"id: {format_offset(entry.offset)}\n"
                    f"data: {event_data}\n\n"
                ).encode()
        except asyncio.CancelledError:
            return

        await asyncio.sleep(0.1)


@require_http_methods(["GET"])
async def read_stream_sse(request, stream_path: str) -> StreamingHttpResponse:
    """
    Read events from a stream using Server-Sent Events (live mode).

    Protocol: GET /{stream-path}/sse?cursor={cursor}

    Query Parameters:
        cursor: Start streaming events after this cursor/offset.

    Response:
        200 OK: Streaming response with text/event-stream content type

    Response Headers:
        Content-Type: text/event-stream
        Cache-Control: no-cache
        X-Accel-Buffering: no (for nginx compatibility)
    """
    cursor_str = request.GET.get("cursor", "0_0")

    try:
        cursor_offset = parse_offset(cursor_str)
    except ValueError:
        cursor_offset = 0

    response = StreamingHttpResponse(
        sse_event_generator(stream_path, cursor_offset),
        content_type="text/event-stream",
    )
    response.headers[CACHE_CONTROL_HEADER] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"

    return response


# =============================================================================
# HEAD /{stream-path} - Stream Metadata
# =============================================================================


@require_http_methods(["HEAD"])
def stream_metadata(request, stream_path: str) -> HttpResponse:  # noqa: ARG001
    """
    Get metadata for a stream without transferring data.

    Protocol: HEAD /{stream-path}

    Response:
        200 OK: Stream exists
        404 Not Found: Stream does not exist

    Response Headers:
        Content-Type: The stream's content type
        Stream-Next-Offset: Current tail offset
    """
    try:
        stream = Stream.objects.get(stream_id=stream_path)
    except Stream.DoesNotExist:
        return HttpResponse(status=404)

    # Calculate next offset
    max_offset = stream.entries.aggregate(max_offset=Max("offset"))["max_offset"]
    next_offset = format_offset(max_offset) if max_offset else format_offset(0)

    headers = {
        CONTENT_TYPE_HEADER: "application/json",
        STREAM_NEXT_OFFSET_HEADER: next_offset,
    }

    return HttpResponse(status=200, headers=headers)


# =============================================================================
# URL Configuration
# =============================================================================

app_name = "protocol"

urlpatterns = [
    # Stream operations - method-based routing handled by decorators
    path("streams/<str:stream_path>/create", create_stream, name="create"),
    path("streams/<str:stream_path>/append", append_to_stream, name="append"),
    path("streams/<str:stream_path>/read", read_stream, name="read"),
    path("streams/<str:stream_path>/metadata", stream_metadata, name="metadata"),
    # SSE endpoint
    path("streams/<str:stream_path>/sse", read_stream_sse, name="sse"),
]
