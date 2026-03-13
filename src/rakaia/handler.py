"""
Plain ASGI HTTP handler for the Durable Streams protocol.

Implements all protocol operations (PUT/POST/GET/HEAD/DELETE) as a raw ASGI
application with zero framework dependencies. Can be run with any ASGI server
(uvicorn, daphne, hypercorn) or mounted in Django/FastAPI/Starlette.
"""

from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

from ._asgi import (
    Receive,
    Scope,
    Send,
    get_all_query_params,
    get_header,
    get_method,
    get_path,
    get_query_param,
    get_query_string,
    read_body,
    send_body_chunk,
    send_response,
    start_streaming_response,
)
from .cursor import CursorOptions, generate_response_cursor
from .json_mode import is_json_content_type
from .store import StreamStore
from .types import (
    PRODUCER_EPOCH_HEADER,
    PRODUCER_EXPECTED_SEQ_HEADER,
    PRODUCER_ID_HEADER,
    PRODUCER_RECEIVED_SEQ_HEADER,
    PRODUCER_SEQ_HEADER,
    SSE_CLOSED_FIELD,
    SSE_CURSOR_FIELD,
    SSE_OFFSET_FIELD,
    SSE_UP_TO_DATE_FIELD,
    STREAM_CLOSED_HEADER,
    STREAM_EXPIRES_AT_HEADER,
    STREAM_SSE_DATA_ENCODING_HEADER,
    STREAM_TTL_HEADER,
    AppendOptions,
    ProducerDuplicate,
    ProducerInvalidEpochSeq,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    ProducerStreamClosed,
)

# Header names used in responses (title case for HTTP convention)
STREAM_OFFSET_HEADER_RESP = "Stream-Next-Offset"
STREAM_CURSOR_HEADER_RESP = "Stream-Cursor"
STREAM_UP_TO_DATE_HEADER_RESP = "Stream-Up-To-Date"
STREAM_CLOSED_HEADER_RESP = "Stream-Closed"

# Valid offset pattern
VALID_OFFSET_PATTERN = re.compile(r"^(-1|now|\d+_\d+)$")

# Strict integer pattern for producer headers
STRICT_INTEGER_PATTERN = re.compile(r"^\d+$")

# Valid TTL pattern (non-negative integer, no leading zeros except for "0")
VALID_TTL_PATTERN = re.compile(r"^(0|[1-9]\d*)$")

# Valid content-type pattern
VALID_CONTENT_TYPE_PATTERN = re.compile(r"^[\w-]+/[\w-]+")


@dataclass
class InjectedFault:
    """Configuration for injected faults (for testing retry/resilience)."""

    count: int = 1
    status: int | None = None
    retry_after: int | None = None
    delay_ms: int | None = None
    drop_connection: bool = False
    truncate_body_bytes: int | None = None
    probability: float | None = None
    method: str | None = None
    corrupt_body: bool = False
    jitter_ms: int | None = None
    inject_sse_event: dict[str, str] | None = None


@dataclass
class ServerOptions:
    """Configuration for the ASGI handler."""

    long_poll_timeout: float = float(os.environ.get("LONG_POLL_TIMEOUT", "30.0"))
    """Default long-poll timeout in seconds."""

    cursor_options: CursorOptions = field(default_factory=CursorOptions)
    """Cursor calculation options."""


def create_app(
    store: StreamStore | None = None,
    options: ServerOptions | None = None,
) -> Any:
    """
    Create a plain ASGI application implementing the Durable Streams protocol.

    Usage:
        app = create_app()
        # Run with: uvicorn rakaia:app --port 4437
        # Mount in Django: path("streams/", app)
        # Mount in FastAPI: fastapi_app.mount("/streams", app)
    """
    actual_store = store or StreamStore()
    opts = options or ServerOptions()
    injected_faults: dict[str, InjectedFault] = {}

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return

        method = get_method(scope)
        path = get_path(scope)

        # CORS headers (applied to all responses via wrapper)
        cors_headers = {
            "access-control-allow-origin": "*",
            "access-control-allow-methods": "GET, POST, PUT, DELETE, HEAD, OPTIONS",
            "access-control-allow-headers": (
                "content-type, authorization, Stream-Seq, Stream-TTL, "
                "Stream-Expires-At, Stream-Closed, Producer-Id, "
                "Producer-Epoch, Producer-Seq"
            ),
            "access-control-expose-headers": (
                "Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, "
                "Stream-Closed, Producer-Epoch, Producer-Seq, "
                "Producer-Expected-Seq, Producer-Received-Seq, "
                "etag, content-type, content-encoding, vary"
            ),
            "x-content-type-options": "nosniff",
            "cross-origin-resource-policy": "cross-origin",
        }

        # Handle CORS preflight
        if method == "OPTIONS":
            await send_response(send, 204, cors_headers)
            return

        # Handle test control endpoints
        if path == "/_test/inject-error":
            await _handle_test_inject(
                method, scope, receive, send, cors_headers, injected_faults
            )
            return

        # Check for injected faults
        fault = _consume_fault(injected_faults, path, method)
        if fault and fault.status is not None:
            headers = {**cors_headers, "content-type": "text/plain"}
            if fault.retry_after is not None:
                headers["retry-after"] = str(fault.retry_after)
            await send_response(
                send, fault.status, headers, b"Injected error for testing"
            )
            return

        try:
            if method == "PUT":
                await _handle_create(
                    path, scope, receive, send, actual_store, cors_headers
                )
            elif method == "HEAD":
                await _handle_head(path, send, actual_store, cors_headers)
            elif method == "GET":
                await _handle_read(
                    path, scope, receive, send, actual_store, opts, cors_headers
                )
            elif method == "POST":
                await _handle_append(
                    path, scope, receive, send, actual_store, cors_headers
                )
            elif method == "DELETE":
                await _handle_delete(path, send, actual_store, cors_headers)
            else:
                await send_response(
                    send,
                    405,
                    {**cors_headers, "content-type": "text/plain"},
                    b"Method not allowed",
                )
        except KeyError as e:
            msg = str(e)
            if "not found" in msg.lower():
                await send_response(
                    send,
                    404,
                    {**cors_headers, "content-type": "text/plain"},
                    b"Stream not found",
                )
            else:
                raise
        except ValueError as e:
            msg = str(e)
            if "already exists with different configuration" in msg:
                await send_response(
                    send,
                    409,
                    {**cors_headers, "content-type": "text/plain"},
                    b"Stream already exists with different configuration",
                )
            elif "Sequence conflict" in msg:
                await send_response(
                    send,
                    409,
                    {**cors_headers, "content-type": "text/plain"},
                    b"Sequence conflict",
                )
            elif "Content-type mismatch" in msg:
                await send_response(
                    send,
                    409,
                    {**cors_headers, "content-type": "text/plain"},
                    b"Content-type mismatch",
                )
            elif "Invalid JSON" in msg:
                await send_response(
                    send,
                    400,
                    {**cors_headers, "content-type": "text/plain"},
                    b"Invalid JSON",
                )
            elif "Empty arrays are not allowed" in msg:
                await send_response(
                    send,
                    400,
                    {**cors_headers, "content-type": "text/plain"},
                    b"Empty arrays are not allowed",
                )
            else:
                raise

    return app


# =============================================================================
# PUT — Create stream
# =============================================================================


async def _handle_create(
    path: str,
    scope: Scope,
    receive: Receive,
    send: Send,
    store: StreamStore,
    cors: dict[str, str],
) -> None:
    content_type = get_header(scope, "content-type")

    # Sanitize content-type
    if (
        not content_type
        or not content_type.strip()
        or not VALID_CONTENT_TYPE_PATTERN.match(content_type)
    ):
        content_type = "application/octet-stream"

    ttl_header = get_header(scope, STREAM_TTL_HEADER)
    expires_at_header = get_header(scope, STREAM_EXPIRES_AT_HEADER)
    closed_header = get_header(scope, STREAM_CLOSED_HEADER)
    create_closed = closed_header == "true"

    # Validate TTL and Expires-At
    if ttl_header and expires_at_header:
        await send_response(
            send,
            400,
            {**cors, "content-type": "text/plain"},
            b"Cannot specify both Stream-TTL and Stream-Expires-At",
        )
        return

    ttl_seconds: int | None = None
    if ttl_header:
        if not VALID_TTL_PATTERN.match(ttl_header):
            await send_response(
                send,
                400,
                {**cors, "content-type": "text/plain"},
                b"Invalid Stream-TTL value",
            )
            return
        ttl_seconds = int(ttl_header)

    # Validate Expires-At
    if expires_at_header:
        try:
            from datetime import datetime

            datetime.fromisoformat(expires_at_header.replace("Z", "+00:00"))
        except ValueError:
            await send_response(
                send,
                400,
                {**cors, "content-type": "text/plain"},
                b"Invalid Stream-Expires-At timestamp",
            )
            return

    # Read body
    body = await read_body(receive)

    is_new = not store.has(path)

    # Create stream (may raise ValueError for config mismatch)
    store.create(
        path,
        content_type=content_type,
        ttl_seconds=ttl_seconds,
        expires_at=expires_at_header,
        initial_data=body if len(body) > 0 else None,
        closed=create_closed,
    )

    stream = store.get(path)
    assert stream is not None

    headers: dict[str, str] = {
        **cors,
        "content-type": content_type,
        STREAM_OFFSET_HEADER_RESP: stream.current_offset,
    }

    # Use ASGI scope to build the absolute location URL if we have it
    base_url = ""
    scheme = scope.get("scheme", "http")
    server = scope.get("server", None)
    headers_dict = dict(scope.get("headers", []))
    host_header = headers_dict.get(b"host", b"").decode("latin-1")

    if host_header:
        base_url = f"{scheme}://{host_header}"
    elif server:
        host, port = server
        if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
            base_url = f"{scheme}://{host}"
        else:
            base_url = f"{scheme}://{host}:{port}"
    else:
        base_url = "http://localhost:4437"

    if is_new:
        headers["location"] = f"{base_url}{path}"

    if stream.closed:
        headers[STREAM_CLOSED_HEADER_RESP] = "true"

    await send_response(send, 201 if is_new else 200, headers)


# =============================================================================
# HEAD — Get metadata
# =============================================================================


async def _handle_head(
    path: str,
    send: Send,
    store: StreamStore,
    cors: dict[str, str],
) -> None:
    stream = store.get(path)
    if stream is None:
        await send_response(send, 404, {**cors, "content-type": "text/plain"}, b"")
        return

    headers: dict[str, str] = {
        **cors,
        STREAM_OFFSET_HEADER_RESP: stream.current_offset,
        "cache-control": "no-store",
    }

    if stream.content_type:
        headers["content-type"] = stream.content_type

    if stream.closed:
        headers[STREAM_CLOSED_HEADER_RESP] = "true"

    # ETag: {path_b64}:-1:{offset}[:c]
    path_b64 = base64.b64encode(path.encode()).decode()
    closed_suffix = ":c" if stream.closed else ""
    headers["etag"] = f'"{path_b64}:-1:{stream.current_offset}{closed_suffix}"'

    await send_response(send, 200, headers)


# =============================================================================
# GET — Read data
# =============================================================================


async def _handle_read(
    path: str,
    scope: Scope,
    receive: Receive,
    send: Send,
    store: StreamStore,
    opts: ServerOptions,
    cors: dict[str, str],
) -> None:
    stream = store.get(path)
    if stream is None:
        await send_response(
            send,
            404,
            {**cors, "content-type": "text/plain"},
            b"Stream not found",
        )
        return

    qs = get_query_string(scope)
    offset = get_query_param(qs, "offset")
    live = get_query_param(qs, "live")
    cursor = get_query_param(qs, "cursor")

    # Validate offset
    if offset is not None:
        if offset == "":
            await send_response(
                send,
                400,
                {**cors, "content-type": "text/plain"},
                b"Empty offset parameter",
            )
            return

        all_offsets = get_all_query_params(qs, "offset")
        if len(all_offsets) > 1:
            await send_response(
                send,
                400,
                {**cors, "content-type": "text/plain"},
                b"Multiple offset parameters not allowed",
            )
            return

        if not VALID_OFFSET_PATTERN.match(offset):
            await send_response(
                send,
                400,
                {**cors, "content-type": "text/plain"},
                b"Invalid offset format",
            )
            return

    # Require offset for long-poll and SSE
    if (live == "long-poll" or live == "sse") and not offset:
        label = "SSE" if live == "sse" else "Long-poll"
        await send_response(
            send,
            400,
            {**cors, "content-type": "text/plain"},
            f"{label} requires offset parameter".encode(),
        )
        return

    # Determine base64 encoding for SSE binary streams
    use_base64 = False
    if live == "sse":
        ct = (stream.content_type or "").lower().split(";")[0].strip()
        is_text_compatible = ct.startswith("text/") or ct == "application/json"
        use_base64 = not is_text_compatible

    # Handle SSE mode
    if live == "sse":
        sse_offset = stream.current_offset if offset == "now" else (offset or "")
        await _handle_sse(
            path,
            stream,
            sse_offset,
            cursor,
            use_base64,
            scope,
            receive,
            send,
            store,
            opts,
            cors,
        )
        return

    # Convert offset=now
    effective_offset = stream.current_offset if offset == "now" else offset

    # Catch-up mode with offset=now: return empty with tail offset
    # For regular GET, return 200 with empty body. For long-poll, fall through to wait.
    if offset == "now" and live != "long-poll":
        headers: dict[str, str] = {
            **cors,
            STREAM_OFFSET_HEADER_RESP: stream.current_offset,
            STREAM_UP_TO_DATE_HEADER_RESP: "true",
            "cache-control": "no-store",
        }
        if stream.content_type:
            headers["content-type"] = stream.content_type
        if stream.closed:
            headers[STREAM_CLOSED_HEADER_RESP] = "true"

        is_json = is_json_content_type(stream.content_type)
        body = b"[]" if is_json else b""
        await send_response(send, 200, headers, body)
        return

    # Read current messages
    messages, up_to_date = store.read(path, effective_offset)

    # Long-poll: wait if caught up and no messages
    client_caught_up = False
    if (
        offset == "now"
        and len(messages) == 0
        or effective_offset is not None
        and effective_offset == stream.current_offset
    ):
        client_caught_up = True

    if live == "long-poll" and client_caught_up and len(messages) == 0:
        # If closed and at tail, return immediately
        if stream.closed:
            await send_response(
                send,
                204,
                {
                    **cors,
                    STREAM_OFFSET_HEADER_RESP: stream.current_offset,
                    STREAM_UP_TO_DATE_HEADER_RESP: "true",
                    STREAM_CLOSED_HEADER_RESP: "true",
                },
            )
            return

        wait_result = await store.wait_for_messages(
            path,
            effective_offset or stream.current_offset,
            opts.long_poll_timeout,
        )
        wait_messages, timed_out, stream_closed = wait_result

        if stream_closed:
            resp_cursor = generate_response_cursor(cursor, opts.cursor_options)
            await send_response(
                send,
                204,
                {
                    **cors,
                    STREAM_OFFSET_HEADER_RESP: effective_offset
                    or stream.current_offset,
                    STREAM_UP_TO_DATE_HEADER_RESP: "true",
                    STREAM_CURSOR_HEADER_RESP: resp_cursor,
                    STREAM_CLOSED_HEADER_RESP: "true",
                },
            )
            return

        if timed_out:
            resp_cursor = generate_response_cursor(cursor, opts.cursor_options)
            timeout_headers: dict[str, str] = {
                **cors,
                STREAM_OFFSET_HEADER_RESP: effective_offset or stream.current_offset,
                STREAM_UP_TO_DATE_HEADER_RESP: "true",
                STREAM_CURSOR_HEADER_RESP: resp_cursor,
            }
            current_stream = store.get(path)
            if current_stream and current_stream.closed:
                timeout_headers[STREAM_CLOSED_HEADER_RESP] = "true"

            # The protocol test expects body to be strictly empty
            await send_response(send, 204, timeout_headers)
            return

        messages = wait_messages
        up_to_date = True

    # Build response
    headers = {**cors}

    if stream.content_type:
        headers["content-type"] = stream.content_type

    last_message = messages[-1] if messages else None
    response_offset = last_message.offset if last_message else stream.current_offset
    headers[STREAM_OFFSET_HEADER_RESP] = response_offset

    if live == "long-poll":
        headers[STREAM_CURSOR_HEADER_RESP] = generate_response_cursor(
            cursor, opts.cursor_options
        )

    if up_to_date:
        headers[STREAM_UP_TO_DATE_HEADER_RESP] = "true"

    # Stream-Closed when closed, at tail, and up-to-date
    current_stream = store.get(path)
    client_at_tail = (
        current_stream is not None and response_offset == current_stream.current_offset
    )
    if current_stream and current_stream.closed and client_at_tail and up_to_date:
        headers[STREAM_CLOSED_HEADER_RESP] = "true"

    # ETag
    start_offset = offset or "-1"
    closed_suffix = (
        ":c"
        if (current_stream and current_stream.closed and client_at_tail and up_to_date)
        else ""
    )
    path_b64 = base64.b64encode(path.encode()).decode()
    etag = f'"{path_b64}:{start_offset}:{response_offset}{closed_suffix}"'
    headers["etag"] = etag

    # Conditional GET
    if_none_match = get_header(scope, "if-none-match")
    if if_none_match and if_none_match == etag:
        await send_response(send, 304, {"etag": etag})
        return

    # Format response
    response_data = store.format_response(path, messages)
    await send_response(send, 200, headers, response_data)


# =============================================================================
# SSE Mode
# =============================================================================


def _encode_sse_data(payload: str) -> str:
    """Encode data for SSE format, handling multi-line payloads and preventing CRLF injection."""
    # Replace carriage returns with a safe alternative or strip them
    # Protocol Section 5.7: "Implementations MUST prevent CRLF injection in SSE events."
    # We replace \r\n with \n, and then stray \r with \n so that they are formatted as separate lines.
    # The tests explicitly check that `\r` becomes a newline `\n` which manifests as `data: `
    # wait the test does "expect(controlContent.cr_injected).toBeUndefined()" so maybe replacing \r with nothing or space is better so it doesn't break JSON?
    # Actually the spec says "Any \r\n or \r characters in payload data MUST be normalized to \n before framing."
    sanitized = payload.replace("\r\n", "\n").replace("\r", "\n")
    lines = sanitized.split("\n")
    result = ""
    for line in lines:
        result += f"data:{line}\n"
    result += "\n"
    return result


async def _handle_sse(
    path: str,
    stream: Any,
    initial_offset: str,
    cursor: str | None,
    use_base64: bool,
    _scope: Scope,
    _receive: Receive,
    send: Send,
    store: StreamStore,
    opts: ServerOptions,
    cors: dict[str, str],
) -> None:
    """Handle SSE (Server-Sent Events) mode."""
    sse_headers: dict[str, str] = {
        **cors,
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
        "connection": "keep-alive",
    }

    if use_base64:
        sse_headers[STREAM_SSE_DATA_ENCODING_HEADER.title().replace("_", "-")] = (
            "base64"
        )
        sse_headers["Stream-Sse-Data-Encoding"] = "base64"

    await start_streaming_response(send, 200, sse_headers)

    current_offset = initial_offset
    is_json_stream = is_json_content_type(stream.content_type)

    while True:
        # Read messages from offset
        try:
            messages, up_to_date = store.read(path, current_offset)
        except KeyError:
            break

        buffer = b""

        # Send data events
        for message in messages:
            if use_base64:
                data_payload = base64.b64encode(message.data).decode()
            elif is_json_stream:
                json_bytes = store.format_response(path, [message])
                data_payload = json_bytes.decode("utf-8")
            else:
                data_payload = message.data.decode("utf-8")

            sse_data = f"event: data\n{_encode_sse_data(data_payload)}"
            buffer += sse_data.encode("utf-8")
            current_offset = message.offset

        # Build control event
        current_stream = store.get(path)
        if current_stream is None:
            if buffer:
                await send_body_chunk(send, buffer)
            break

        control_offset = (
            messages[-1].offset if messages else current_stream.current_offset
        )
        stream_is_closed = current_stream.closed
        client_at_tail = control_offset == current_stream.current_offset

        control_data: dict[str, str | bool] = {
            SSE_OFFSET_FIELD: control_offset,
        }

        if stream_is_closed and client_at_tail:
            control_data[SSE_CLOSED_FIELD] = True
        else:
            resp_cursor = generate_response_cursor(cursor, opts.cursor_options)
            control_data[SSE_CURSOR_FIELD] = resp_cursor
            if up_to_date:
                control_data[SSE_UP_TO_DATE_FIELD] = True

        control_json = json.dumps(control_data)
        sse_control = f"event: control\n{_encode_sse_data(control_json)}"
        buffer += sse_control.encode("utf-8")

        await send_body_chunk(send, buffer)

        # If closed and at tail, end connection
        if stream_is_closed and client_at_tail:
            break

        current_offset = control_offset

        # Wait for new data if caught up
        if up_to_date:
            if current_stream.closed:
                # Send final control
                final_data: dict[str, str | bool] = {
                    SSE_OFFSET_FIELD: current_offset,
                    SSE_CLOSED_FIELD: True,
                }
                sse_final = (
                    f"event: control\n{_encode_sse_data(json.dumps(final_data))}"
                )
                await send_body_chunk(send, sse_final.encode("utf-8"))
                break

            try:
                (
                    _wait_messages,
                    timed_out,
                    stream_closed,
                ) = await store.wait_for_messages(
                    path, current_offset, opts.long_poll_timeout
                )
            except KeyError:
                break

            if stream_closed:
                final_data = {
                    SSE_OFFSET_FIELD: current_offset,
                    SSE_CLOSED_FIELD: True,
                }
                sse_final = (
                    f"event: control\n{_encode_sse_data(json.dumps(final_data))}"
                )
                await send_body_chunk(send, sse_final.encode("utf-8"))
                break

            if timed_out:
                # Keep-alive control event
                keep_cursor = generate_response_cursor(cursor, opts.cursor_options)
                stream_after = store.get(path)
                if stream_after and stream_after.closed:
                    closed_data: dict[str, str | bool] = {
                        SSE_OFFSET_FIELD: current_offset,
                        SSE_CLOSED_FIELD: True,
                    }
                    sse_closed = (
                        f"event: control\n{_encode_sse_data(json.dumps(closed_data))}"
                    )
                    await send_body_chunk(send, sse_closed.encode("utf-8"))
                    break

                keep_data: dict[str, str | bool] = {
                    SSE_OFFSET_FIELD: current_offset,
                    SSE_CURSOR_FIELD: keep_cursor,
                    SSE_UP_TO_DATE_FIELD: True,
                }
                sse_keep = f"event: control\n{_encode_sse_data(json.dumps(keep_data))}"
                await send_body_chunk(send, sse_keep.encode("utf-8"))
            # Loop continues to read new messages

    # End the SSE connection
    await send_body_chunk(send, b"", more_body=False)


# =============================================================================
# POST — Append data
# =============================================================================


async def _handle_append(
    path: str,
    scope: Scope,
    receive: Receive,
    send: Send,
    store: StreamStore,
    cors: dict[str, str],
) -> None:
    content_type = get_header(scope, "content-type")
    seq = get_header(scope, "stream-seq")
    closed_header = get_header(scope, STREAM_CLOSED_HEADER)
    close_stream = closed_header == "true"

    # Extract producer headers
    producer_id = get_header(scope, PRODUCER_ID_HEADER)
    producer_epoch_str = get_header(scope, PRODUCER_EPOCH_HEADER)
    producer_seq_str = get_header(scope, PRODUCER_SEQ_HEADER)

    # Validate producer headers - all three together or none
    has_any = any(
        h is not None for h in [producer_id, producer_epoch_str, producer_seq_str]
    )
    has_all = all(
        h is not None for h in [producer_id, producer_epoch_str, producer_seq_str]
    )

    if has_any and not has_all:
        await send_response(
            send,
            400,
            {**cors, "content-type": "text/plain"},
            b"All producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided together",
        )
        return

    if has_all and producer_id == "":
        await send_response(
            send,
            400,
            {**cors, "content-type": "text/plain"},
            b"Invalid Producer-Id: must not be empty",
        )
        return

    # Parse producer epoch/seq
    producer_epoch: int | None = None
    producer_seq: int | None = None
    if has_all:
        assert producer_epoch_str is not None
        assert producer_seq_str is not None

        if not STRICT_INTEGER_PATTERN.match(producer_epoch_str):
            await send_response(
                send,
                400,
                {**cors, "content-type": "text/plain"},
                b"Invalid Producer-Epoch: must be a non-negative integer",
            )
            return
        producer_epoch = int(producer_epoch_str)

        if not STRICT_INTEGER_PATTERN.match(producer_seq_str):
            await send_response(
                send,
                400,
                {**cors, "content-type": "text/plain"},
                b"Invalid Producer-Seq: must be a non-negative integer",
            )
            return
        producer_seq = int(producer_seq_str)

    body = await read_body(receive)

    # Close-only request (empty body + Stream-Closed: true)
    if len(body) == 0 and close_stream:
        if has_all:
            assert producer_id is not None
            assert producer_epoch is not None
            assert producer_seq is not None
            close_result = await store.close_stream_with_producer(
                path, producer_id, producer_epoch, producer_seq
            )
            if close_result is None:
                await send_response(
                    send,
                    404,
                    {**cors, "content-type": "text/plain"},
                    b"Stream not found",
                )
                return

            pr = close_result.producer_result
            if isinstance(pr, ProducerDuplicate):
                await send_response(
                    send,
                    204,
                    {
                        **cors,
                        STREAM_OFFSET_HEADER_RESP: close_result.final_offset,
                        STREAM_CLOSED_HEADER_RESP: "true",
                        PRODUCER_EPOCH_HEADER: str(producer_epoch),
                        PRODUCER_SEQ_HEADER: str(pr.last_seq),
                    },
                )
                return
            if isinstance(pr, ProducerStaleEpoch):
                await send_response(
                    send,
                    403,
                    {
                        **cors,
                        "content-type": "text/plain",
                        PRODUCER_EPOCH_HEADER: str(pr.current_epoch),
                    },
                    b"Stale producer epoch",
                )
                return
            if isinstance(pr, ProducerInvalidEpochSeq):
                await send_response(
                    send,
                    400,
                    {**cors, "content-type": "text/plain"},
                    b"New epoch must start with sequence 0",
                )
                return
            if isinstance(pr, ProducerSequenceGap):
                await send_response(
                    send,
                    409,
                    {
                        **cors,
                        "content-type": "text/plain",
                        PRODUCER_EXPECTED_SEQ_HEADER: str(pr.expected_seq),
                        PRODUCER_RECEIVED_SEQ_HEADER: str(pr.received_seq),
                    },
                    b"Producer sequence gap",
                )
                return
            if isinstance(pr, ProducerStreamClosed):
                s = store.get(path)
                await send_response(
                    send,
                    409,
                    {
                        **cors,
                        "content-type": "text/plain",
                        STREAM_CLOSED_HEADER_RESP: "true",
                        STREAM_OFFSET_HEADER_RESP: s.current_offset if s else "",
                    },
                    b"Stream is closed",
                )
                return

            # Success
            await send_response(
                send,
                204,
                {
                    **cors,
                    STREAM_OFFSET_HEADER_RESP: close_result.final_offset,
                    STREAM_CLOSED_HEADER_RESP: "true",
                    PRODUCER_EPOCH_HEADER: str(producer_epoch),
                    PRODUCER_SEQ_HEADER: str(producer_seq),
                },
            )
            return
        else:
            # Simple close without producer
            close_result = store.close_stream(path)
            if close_result is None:
                await send_response(
                    send,
                    404,
                    {**cors, "content-type": "text/plain"},
                    b"Stream not found",
                )
                return
            await send_response(
                send,
                204,
                {
                    **cors,
                    STREAM_OFFSET_HEADER_RESP: close_result.final_offset,
                    STREAM_CLOSED_HEADER_RESP: "true",
                },
            )
            return

    # Empty body without close is an error
    if len(body) == 0:
        await send_response(
            send,
            400,
            {**cors, "content-type": "text/plain"},
            b"Empty body",
        )
        return

    # Content-Type required for bodies
    if not content_type:
        await send_response(
            send,
            400,
            {**cors, "content-type": "text/plain"},
            b"Content-Type header is required",
        )
        return

    append_opts = AppendOptions(
        seq=seq,
        content_type=content_type,
        producer_id=producer_id,
        producer_epoch=producer_epoch,
        producer_seq=producer_seq,
        close=close_stream,
    )

    # Use append_with_producer for serialized operations
    if producer_id is not None:
        result = await store.append_with_producer(path, body, append_opts)
    else:
        result = store.append(path, body, append_opts)

    # Handle closed stream
    if result.stream_closed and result.message is None:
        pr = result.producer_result
        if isinstance(pr, ProducerDuplicate):
            s = store.get(path)
            await send_response(
                send,
                204,
                {
                    **cors,
                    STREAM_OFFSET_HEADER_RESP: s.current_offset if s else "",
                    STREAM_CLOSED_HEADER_RESP: "true",
                    PRODUCER_EPOCH_HEADER: str(producer_epoch),
                    PRODUCER_SEQ_HEADER: str(pr.last_seq),
                },
            )
            return

        s = store.get(path)
        await send_response(
            send,
            409,
            {
                **cors,
                "content-type": "text/plain",
                STREAM_CLOSED_HEADER_RESP: "true",
                STREAM_OFFSET_HEADER_RESP: s.current_offset if s else "",
            },
            b"Stream is closed",
        )
        return

    pr = result.producer_result
    if pr is None or (hasattr(pr, "status") and pr.status == "accepted"):
        # Success
        resp_headers: dict[str, str] = {
            **cors,
            STREAM_OFFSET_HEADER_RESP: result.message.offset if result.message else "",
        }
        if producer_epoch is not None:
            resp_headers[PRODUCER_EPOCH_HEADER] = str(producer_epoch)
        if producer_seq is not None:
            resp_headers[PRODUCER_SEQ_HEADER] = str(producer_seq)
        if result.stream_closed:
            resp_headers[STREAM_CLOSED_HEADER_RESP] = "true"
        status_code = 200 if producer_id is not None else 204
        await send_response(send, status_code, resp_headers)
        return

    # Producer validation failures
    if isinstance(pr, ProducerDuplicate):
        dup_headers: dict[str, str] = {
            **cors,
            PRODUCER_EPOCH_HEADER: str(producer_epoch),
            PRODUCER_SEQ_HEADER: str(pr.last_seq),
        }
        if result.stream_closed:
            dup_headers[STREAM_CLOSED_HEADER_RESP] = "true"
        await send_response(send, 204, dup_headers)
        return

    if isinstance(pr, ProducerStaleEpoch):
        await send_response(
            send,
            403,
            {
                **cors,
                "content-type": "text/plain",
                PRODUCER_EPOCH_HEADER: str(pr.current_epoch),
            },
            b"Stale producer epoch",
        )
        return

    if isinstance(pr, ProducerInvalidEpochSeq):
        await send_response(
            send,
            400,
            {**cors, "content-type": "text/plain"},
            b"New epoch must start with sequence 0",
        )
        return

    if isinstance(pr, ProducerSequenceGap):
        await send_response(
            send,
            409,
            {
                **cors,
                "content-type": "text/plain",
                PRODUCER_EXPECTED_SEQ_HEADER: str(pr.expected_seq),
                PRODUCER_RECEIVED_SEQ_HEADER: str(pr.received_seq),
            },
            b"Producer sequence gap",
        )
        return


# =============================================================================
# DELETE — Delete stream
# =============================================================================


async def _handle_delete(
    path: str,
    send: Send,
    store: StreamStore,
    cors: dict[str, str],
) -> None:
    if not store.has(path):
        await send_response(
            send,
            404,
            {**cors, "content-type": "text/plain"},
            b"Stream not found",
        )
        return

    store.delete(path)
    await send_response(send, 204, cors)


# =============================================================================
# Test control endpoints
# =============================================================================


async def _handle_test_inject(
    method: str,
    _scope: Scope,
    receive: Receive,
    send: Send,
    cors: dict[str, str],
    injected_faults: dict[str, InjectedFault],
) -> None:
    if method == "POST":
        body = await read_body(receive)
        try:
            config = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            await send_response(
                send,
                400,
                {**cors, "content-type": "text/plain"},
                b"Invalid JSON body",
            )
            return

        path = config.get("path")
        if not path:
            await send_response(
                send,
                400,
                {**cors, "content-type": "text/plain"},
                b"Missing required field: path",
            )
            return

        injected_faults[path] = InjectedFault(
            count=config.get("count", 1),
            status=config.get("status"),
            retry_after=config.get("retryAfter"),
            delay_ms=config.get("delayMs"),
            drop_connection=config.get("dropConnection", False),
            truncate_body_bytes=config.get("truncateBodyBytes"),
            probability=config.get("probability"),
            method=config.get("method"),
            corrupt_body=config.get("corruptBody", False),
            jitter_ms=config.get("jitterMs"),
            inject_sse_event=config.get("injectSseEvent"),
        )

        await send_response(
            send,
            200,
            {**cors, "content-type": "application/json"},
            b'{"ok":true}',
        )
    elif method == "DELETE":
        injected_faults.clear()
        await send_response(
            send,
            200,
            {**cors, "content-type": "application/json"},
            b'{"ok":true}',
        )
    else:
        await send_response(
            send,
            405,
            {**cors, "content-type": "text/plain"},
            b"Method not allowed",
        )


def _consume_fault(
    faults: dict[str, InjectedFault], path: str, method: str
) -> InjectedFault | None:
    """Check and consume an injected fault for the given path/method."""
    fault = faults.get(path)
    if fault is None:
        return None

    if fault.method and fault.method.upper() != method.upper():
        return None

    fault.count -= 1
    if fault.count <= 0:
        del faults[path]

    return fault
