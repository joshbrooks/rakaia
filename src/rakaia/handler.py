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
from .protocols import StreamServerStore
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
    STREAM_SEQ_HEADER,
    STREAM_SSE_DATA_ENCODING_HEADER,
    STREAM_TTL_HEADER,
    VALID_OFFSET_PATTERN,
    AppendOptions,
    ContentTypeMismatch,
    EmptyJsonArray,
    InvalidJson,
    InvalidOffset,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerInvalidEpochSeq,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    ProducerStreamClosed,
    SequenceConflict,
    StreamConfigConflict,
    StreamError,
    StreamNotFound,
)

# How a store failure becomes a response. A store raises one of the named
# failures in `.types`; this is the whole mapping. Anything not in here — a
# store raising a bare ValueError, say — propagates and becomes a 500, which is
# the same as before, except the decision is now made by type rather than by
# matching English in the exception message (a reworded f-string used to turn a
# 4xx into a 500 silently).
STORE_FAILURE_STATUS: dict[type[StreamError], tuple[int, bytes]] = {
    StreamNotFound: (404, b"Stream not found"),
    StreamConfigConflict: (
        409,
        b"Stream already exists with different configuration",
    ),
    SequenceConflict: (409, b"Sequence conflict"),
    ContentTypeMismatch: (409, b"Content-type mismatch"),
    InvalidJson: (400, b"Invalid JSON"),
    EmptyJsonArray: (400, b"Empty arrays are not allowed"),
    InvalidOffset: (400, b"Invalid offset"),
}


def _status_for(failure: BaseException) -> tuple[int, bytes] | None:
    """The response for a store failure, or `None` to let it propagate.

    Resolved along the MRO rather than by exact type, so a store that
    specializes a failure — `class ShardNotFound(StreamNotFound)` in some other
    backend — inherits its status instead of falling through to a 500. The
    closed-set test only sees subclasses that happen to be imported, so exact
    lookup would leave that hole open for anything defined downstream.
    """
    for cls in type(failure).__mro__:
        mapped = STORE_FAILURE_STATUS.get(cls)  # type: ignore[arg-type]
        if mapped is not None:
            return mapped
    return None


def producer_response(
    result: Any,
    *,
    producer_epoch: int | None,
    offset: str | None = None,
    stream_closed: bool = False,
) -> tuple[int, bytes, dict[str, str]] | None:
    """The HTTP response a refused fenced write becomes, or `None` if it was not
    refused.

    The union-typed twin of `_status_for`, which does the same job for store
    *exceptions*. Both halves of "what status does this become" now live beside
    each other instead of one being a lookup table and the other a ladder of
    `isinstance` checks written out three times in `_handle_append`.

    Returns `(status, body, extra_headers)`; the caller merges CORS and sends it.
    An empty body means 204 — that arm is sent with `send_response`, the rest
    with `_send_error`, which adds ``content-type: text/plain``.

    The three call sites differ only in what they can supply, so those are
    parameters rather than duplication:

    * ``offset`` — the stream's next offset, when the caller has one to report.
      Omitted rather than sent empty when it does not (the append-result path).
    * ``stream_closed`` — whether to flag a duplicate as landing on a now-closed
      stream.

    A duplicate is **204, not an error**: the producer's write did land, it just
    landed on an earlier attempt, and echoing `Producer-Epoch`/`Producer-Seq`
    lets it resume from the right place rather than retrying forever.
    """
    if result is None or isinstance(result, ProducerAccepted):
        return None

    if isinstance(result, ProducerDuplicate):
        headers = {
            PRODUCER_EPOCH_HEADER: str(producer_epoch),
            PRODUCER_SEQ_HEADER: str(result.last_seq),
        }
        if offset is not None:
            headers[STREAM_OFFSET_HEADER_RESP] = offset
        if stream_closed:
            headers[STREAM_CLOSED_HEADER_RESP] = "true"
        return 204, b"", headers

    if isinstance(result, ProducerStaleEpoch):
        # The epoch in force, not the one the client sent — that is what lets a
        # fenced-out producer discover it has been superseded.
        return (
            403,
            b"Stale producer epoch",
            {PRODUCER_EPOCH_HEADER: str(result.current_epoch)},
        )

    if isinstance(result, ProducerInvalidEpochSeq):
        return 400, b"New epoch must start with sequence 0", {}

    if isinstance(result, ProducerSequenceGap):
        return (
            409,
            b"Producer sequence gap",
            {
                PRODUCER_EXPECTED_SEQ_HEADER: str(result.expected_seq),
                PRODUCER_RECEIVED_SEQ_HEADER: str(result.received_seq),
            },
        )

    if isinstance(result, ProducerStreamClosed):
        headers = {STREAM_CLOSED_HEADER_RESP: "true"}
        if offset is not None:
            headers[STREAM_OFFSET_HEADER_RESP] = offset
        return 409, b"Stream is closed", headers

    return None


async def _send_producer_response(
    send: Send, decided: tuple[int, bytes, dict[str, str]], cors: dict[str, str]
) -> None:
    """Send what `producer_response` decided.

    A bodiless 204 goes through `send_response`; anything else through
    `_send_error`, which stamps ``content-type: text/plain``.
    """
    status, body, extra = decided
    if not body:
        await send_response(send, status, {**cors, **extra})
    else:
        await _send_error(send, status, body, cors, extra)


# Header names used in responses (title case for HTTP convention)
STREAM_OFFSET_HEADER_RESP = "Stream-Next-Offset"
STREAM_CURSOR_HEADER_RESP = "Stream-Cursor"
STREAM_UP_TO_DATE_HEADER_RESP = "Stream-Up-To-Date"
STREAM_CLOSED_HEADER_RESP = "Stream-Closed"

# `VALID_OFFSET_PATTERN` is imported from `.types` (the single source of truth,
# #41). It is a syntactic guard only — an offset's meaning belongs to the store
# that issued it, and the two stores use different formats.

# Strict integer pattern for producer headers
STRICT_INTEGER_PATTERN = re.compile(r"^\d+$")


async def _send_error(
    send: Send,
    status: int,
    message: bytes,
    cors: dict[str, str],
    extra_headers: dict[str, str] | None = None,
) -> None:
    """Send a plain-text error response carrying the CORS headers.

    Collapses the repeated ``send_response(send, status, {**cors,
    "content-type": "text/plain"}, msg)`` shape used throughout the handler.
    """
    headers = {**cors, "content-type": "text/plain"}
    if extra_headers:
        headers.update(extra_headers)
    await send_response(send, status, headers, message)


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
    method: str | None = None
    corrupt_body: bool = False
    jitter_ms: int | None = None
    inject_sse_event: dict[str, str] | None = None


@dataclass
class ServerOptions:
    """Configuration for the ASGI handler."""

    long_poll_timeout: float = float(os.environ.get("LONG_POLL_TIMEOUT", "3.0"))
    """Default long-poll hold window in seconds.

    Kept short so a caught-up long-poll (e.g. ``?offset=now&live=long-poll``)
    returns a ``204`` promptly rather than blocking a client for tens of
    seconds. Override with the ``LONG_POLL_TIMEOUT`` environment variable.
    """

    cursor_options: CursorOptions = field(default_factory=CursorOptions)
    """Cursor calculation options."""


def create_app(
    store: StreamServerStore | None = None,
    options: ServerOptions | None = None,
) -> Any:
    """
    Create a plain ASGI application implementing the Durable Streams protocol.

    Usage:
        app = create_app()
        # Run with: uvicorn rakaia:app --port 4437
        # Mount in FastAPI: fastapi_app.mount("/streams", app)  (strips the prefix)
        # Mount in Django: dispatch on scope["path"] in asgi.py and strip the
        # prefix yourself — URLRouter/path() does not strip it, so the stream
        # id would keep the mount prefix. See django_rakaia.integration.get_asgi_app.
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
                "content-type, authorization, If-None-Match, Stream-Seq, "
                "Stream-TTL, Stream-Expires-At, Stream-Closed, Producer-Id, "
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
                await _send_error(send, 405, b"Method not allowed", cors_headers)
        except StreamError as e:
            mapped = _status_for(e)
            if mapped is None:
                raise
            status, body = mapped
            await _send_error(send, status, body, cors_headers)

    return app


# =============================================================================
# PUT — Create stream
# =============================================================================


async def _handle_create(
    path: str,
    scope: Scope,
    receive: Receive,
    send: Send,
    store: StreamServerStore,
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
        await _send_error(
            send, 400, b"Cannot specify both Stream-TTL and Stream-Expires-At", cors
        )
        return

    ttl_seconds: int | None = None
    if ttl_header:
        if not VALID_TTL_PATTERN.match(ttl_header):
            await _send_error(send, 400, b"Invalid Stream-TTL value", cors)
            return
        ttl_seconds = int(ttl_header)

    # Validate Expires-At
    if expires_at_header:
        try:
            from datetime import datetime

            datetime.fromisoformat(expires_at_header.replace("Z", "+00:00"))
        except ValueError:
            await _send_error(send, 400, b"Invalid Stream-Expires-At timestamp", cors)
            return

    # Read body
    body = await read_body(receive)

    is_new = not await store.run_sync(store.has, path)

    # Create stream (may raise ValueError for config mismatch)
    await store.run_sync(
        store.create,
        path,
        content_type=content_type,
        ttl_seconds=ttl_seconds,
        expires_at=expires_at_header,
        initial_data=body if len(body) > 0 else None,
        closed=create_closed,
    )

    stream = await store.run_sync(store.get, path)
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
    store: StreamServerStore,
    cors: dict[str, str],
) -> None:
    stream = await store.run_sync(store.get, path)
    if stream is None:
        await _send_error(send, 404, b"", cors)
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

    # Surface configured expiry metadata (HEAD must not extend the TTL window).
    if stream.ttl_seconds is not None:
        headers["Stream-TTL"] = str(stream.ttl_seconds)
    if stream.expires_at:
        headers["Stream-Expires-At"] = stream.expires_at

    # ETag: {path_b64}:-1:{offset}[:c]
    path_b64 = base64.b64encode(path.encode()).decode()
    closed_suffix = ":c" if stream.closed else ""
    headers["etag"] = f'"{path_b64}:-1:{stream.current_offset}{closed_suffix}"'

    await send_response(send, 200, headers)


# =============================================================================
# GET — Read data
# =============================================================================


async def _validate_read_params(
    send: Send,
    qs: str,
    offset: str | None,
    live: str | None,
    cors: dict[str, str],
) -> bool:
    """Validate the GET read query parameters.

    On the first invalid parameter, send a ``400`` and return ``False`` — the
    caller must then return. Returns ``True`` when the read may proceed.
    """
    if offset is not None:
        if offset == "":
            await _send_error(send, 400, b"Empty offset parameter", cors)
            return False

        if len(get_all_query_params(qs, "offset")) > 1:
            await _send_error(
                send, 400, b"Multiple offset parameters not allowed", cors
            )
            return False

        if not VALID_OFFSET_PATTERN.match(offset):
            await _send_error(send, 400, b"Invalid offset format", cors)
            return False

    # Long-poll and SSE both require an explicit offset.
    if (live == "long-poll" or live == "sse") and not offset:
        label = "SSE" if live == "sse" else "Long-poll"
        await _send_error(
            send, 400, f"{label} requires offset parameter".encode(), cors
        )
        return False

    return True


async def _handle_read(
    path: str,
    scope: Scope,
    receive: Receive,
    send: Send,
    store: StreamServerStore,
    opts: ServerOptions,
    cors: dict[str, str],
) -> None:
    stream = await store.run_sync(store.get, path)
    if stream is None:
        await _send_error(send, 404, b"Stream not found", cors)
        return

    qs = get_query_string(scope)
    offset = get_query_param(qs, "offset")
    live = get_query_param(qs, "live")
    cursor = get_query_param(qs, "cursor")

    if not await _validate_read_params(send, qs, offset, live, cors):
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
        # A catch-up read extends the sliding TTL window.
        await store.run_sync(store.touch, path)
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
    messages, up_to_date = await store.run_sync(store.read, path, effective_offset)

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
            current_stream = await store.run_sync(store.get, path)
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
    current_stream = await store.run_sync(store.get, path)
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
    response_data = await store.run_sync(store.format_response, path, messages)
    await send_response(send, 200, headers, response_data)


# =============================================================================
# SSE Mode
# =============================================================================


def _encode_sse_data(payload: str) -> str:
    """Encode data for SSE format, handling multi-line payloads and preventing CRLF injection."""
    # Protocol Section 5.7: any \r\n or stray \r in payload data is normalized to
    # \n before framing, so each becomes its own `data:` line rather than
    # terminating the frame early (CRLF injection).
    sanitized = payload.replace("\r\n", "\n").replace("\r", "\n")
    lines = sanitized.split("\n")
    result = ""
    for line in lines:
        result += f"data:{line}\n"
    result += "\n"
    return result


def _build_sse_control(
    control_offset: str,
    tail_offset: str,
    stream_is_closed: bool,
    up_to_date: bool,
    cursor: str | None,
    cursor_options: CursorOptions,
) -> bytes:
    """Build an SSE ``control`` event frame for a given offset.

    Emitted immediately after each ``data`` event during catch-up so consumers
    can rely on strict data -> control pairing. Signals closure at the tail and
    otherwise carries the collapse cursor and (at the tail) the up-to-date flag.
    """
    at_tail = control_offset == tail_offset
    control_data: dict[str, str | bool] = {SSE_OFFSET_FIELD: control_offset}
    if stream_is_closed and at_tail:
        control_data[SSE_CLOSED_FIELD] = True
    else:
        control_data[SSE_CURSOR_FIELD] = generate_response_cursor(
            cursor, cursor_options
        )
        if at_tail and up_to_date:
            control_data[SSE_UP_TO_DATE_FIELD] = True
    return f"event: control\n{_encode_sse_data(json.dumps(control_data))}".encode()


def _closed_control(offset: str) -> bytes:
    """The terminal ``control`` frame: this offset is the tail and it is closed.

    A closed-at-tail frame carries ``closed`` instead of a cursor, so the cursor
    arguments are never read.
    """
    return _build_sse_control(offset, offset, True, False, None, CursorOptions())


async def _handle_sse(
    path: str,
    stream: Any,
    initial_offset: str,
    cursor: str | None,
    use_base64: bool,
    _scope: Scope,
    _receive: Receive,
    send: Send,
    store: StreamServerStore,
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

    # The first read runs before the response starts: a StreamError here (bad
    # offset, vanished stream) must surface as its mapped 4xx, which is
    # impossible once http.response.start has been sent.
    pending_read: tuple[Any, bool] | None = await store.run_sync(
        store.read, path, initial_offset
    )

    await start_streaming_response(send, 200, sse_headers)

    current_offset = initial_offset
    is_json_stream = is_json_content_type(stream.content_type)

    while True:
        # Read messages from offset
        if pending_read is not None:
            messages, up_to_date = pending_read
            pending_read = None
        else:
            try:
                messages, up_to_date = await store.run_sync(
                    store.read, path, current_offset
                )
            except (KeyError, StreamError):
                break

        current_stream = await store.run_sync(store.get, path)
        if current_stream is None:
            break

        tail_offset = current_stream.current_offset
        stream_is_closed = current_stream.closed
        cursor_opts = opts.cursor_options

        buffer = b""

        if messages:
            # Pair every data event with its own control event so consumers can
            # rely on a strict data -> control framing during catch-up.
            for message in messages:
                if use_base64:
                    data_payload = base64.b64encode(message.data).decode()
                elif is_json_stream:
                    json_bytes = await store.run_sync(
                        store.format_response, path, [message]
                    )
                    data_payload = json_bytes.decode("utf-8")
                else:
                    data_payload = message.data.decode("utf-8")

                sse_data = f"event: data\n{_encode_sse_data(data_payload)}"
                buffer += sse_data.encode("utf-8")
                buffer += _build_sse_control(
                    message.offset,
                    tail_offset,
                    stream_is_closed,
                    up_to_date,
                    cursor,
                    cursor_opts,
                )
                current_offset = message.offset

            control_offset = messages[-1].offset
        else:
            # Caught up with nothing new: emit a single control event.
            control_offset = tail_offset
            buffer += _build_sse_control(
                control_offset,
                tail_offset,
                stream_is_closed,
                up_to_date,
                cursor,
                cursor_opts,
            )  # noqa: B023

        await send_body_chunk(send, buffer)

        client_at_tail = control_offset == tail_offset

        # If closed and at tail, end connection
        if stream_is_closed and client_at_tail:
            break

        current_offset = control_offset

        # Wait for new data if caught up
        if up_to_date:
            if current_stream.closed:
                # Send final control
                await send_body_chunk(send, _closed_control(current_offset))
                break

            try:
                (
                    _wait_messages,
                    timed_out,
                    stream_closed,
                ) = await store.wait_for_messages(
                    path, current_offset, opts.long_poll_timeout
                )
            except (KeyError, StreamError):
                break

            if stream_closed:
                await send_body_chunk(send, _closed_control(current_offset))
                break

            if timed_out:
                # Keep-alive control event
                stream_after = await store.run_sync(store.get, path)
                if stream_after and stream_after.closed:
                    await send_body_chunk(send, _closed_control(current_offset))
                    break

                await send_body_chunk(
                    send,
                    _build_sse_control(
                        current_offset,
                        current_offset,
                        False,
                        True,
                        cursor,
                        opts.cursor_options,
                    ),
                )
            # Loop continues to read new messages

    # End the SSE connection
    await send_body_chunk(send, b"", more_body=False)


# =============================================================================
# POST — Append data
# =============================================================================


@dataclass
class _ParsedProducer:
    """Validated producer coordinates parsed from the append request headers."""

    producer_id: str | None
    epoch: int | None
    seq: int | None
    has_all: bool
    """Whether all three producer headers were supplied (idempotent-producer mode)."""


async def _parse_producer_headers(
    send: Send,
    scope: Scope,
    cors: dict[str, str],
) -> _ParsedProducer | None:
    """Extract and validate the idempotent-producer headers.

    All three (`Producer-Id`, `Producer-Epoch`, `Producer-Seq`) must be present
    together or absent together, and epoch/seq must be non-negative integers.
    On any violation, send a ``400`` and return ``None`` — the caller must then
    return. Otherwise return the parsed coordinates (epoch/seq are ``None`` when
    no producer headers were supplied).
    """
    producer_id = get_header(scope, PRODUCER_ID_HEADER)
    producer_epoch_str = get_header(scope, PRODUCER_EPOCH_HEADER)
    producer_seq_str = get_header(scope, PRODUCER_SEQ_HEADER)

    present = [producer_id, producer_epoch_str, producer_seq_str]
    has_any = any(h is not None for h in present)
    has_all = all(h is not None for h in present)

    if has_any and not has_all:
        await _send_error(
            send,
            400,
            b"All producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided together",
            cors,
        )
        return None

    if has_all and producer_id == "":
        await _send_error(send, 400, b"Invalid Producer-Id: must not be empty", cors)
        return None

    producer_epoch: int | None = None
    producer_seq: int | None = None
    if has_all:
        assert producer_epoch_str is not None
        assert producer_seq_str is not None

        if not STRICT_INTEGER_PATTERN.match(producer_epoch_str):
            await _send_error(
                send,
                400,
                b"Invalid Producer-Epoch: must be a non-negative integer",
                cors,
            )
            return None
        producer_epoch = int(producer_epoch_str)

        if not STRICT_INTEGER_PATTERN.match(producer_seq_str):
            await _send_error(
                send,
                400,
                b"Invalid Producer-Seq: must be a non-negative integer",
                cors,
            )
            return None
        producer_seq = int(producer_seq_str)

    return _ParsedProducer(producer_id, producer_epoch, producer_seq, has_all)


async def _handle_append(
    path: str,
    scope: Scope,
    receive: Receive,
    send: Send,
    store: StreamServerStore,
    cors: dict[str, str],
) -> None:
    content_type = get_header(scope, "content-type")
    # Stream-Seq is a number, and must be parsed as one. It used to be passed
    # through as the raw header string, so the store's `opts.seq <=
    # stream.last_seq` compared text: seq=10 after seq=9 was rejected as a
    # conflict, because "10" < "9" lexicographically. Any producer reaching
    # double digits started failing.
    seq_str = get_header(scope, STREAM_SEQ_HEADER)
    seq: int | None = None
    if seq_str is not None:
        if not STRICT_INTEGER_PATTERN.match(seq_str):
            await _send_error(send, 400, b"Invalid Stream-Seq", cors)
            return
        seq = int(seq_str)
    closed_header = get_header(scope, STREAM_CLOSED_HEADER)
    close_stream = closed_header == "true"

    parsed = await _parse_producer_headers(send, scope, cors)
    if parsed is None:
        return
    producer_id = parsed.producer_id
    producer_epoch = parsed.epoch
    producer_seq = parsed.seq
    has_all = parsed.has_all

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
                await _send_error(send, 404, b"Stream not found", cors)
                return

            pr = close_result.producer_result
            if isinstance(pr, ProducerStreamClosed):
                # Only this arm needs the stream's current offset, and fetching
                # it costs a round trip — so it is resolved here rather than
                # unconditionally before the decision.
                st = await store.run_sync(store.get, path)
                offset = st.current_offset if st else ""
            else:
                offset = close_result.final_offset
            decided = producer_response(
                pr, producer_epoch=producer_epoch, offset=offset, stream_closed=True
            )
            if decided is not None:
                await _send_producer_response(send, decided, cors)
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
            close_result = await store.run_sync(store.close_stream, path)
            if close_result is None:
                await _send_error(send, 404, b"Stream not found", cors)
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
        await _send_error(send, 400, b"Empty body", cors)
        return

    # Content-Type required for bodies
    if not content_type:
        await _send_error(send, 400, b"Content-Type header is required", cors)
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
        result = await store.run_sync(store.append, path, body, append_opts)

    # Handle closed stream
    if result.stream_closed and result.message is None:
        st = await store.run_sync(store.get, path)
        offset = st.current_offset if st else ""
        decided = producer_response(
            result.producer_result,
            producer_epoch=producer_epoch,
            offset=offset,
            stream_closed=True,
        )
        if decided is None:
            # Nothing to report about the producer — an unfenced append, or one
            # whose fencing was fine. The closed stream is itself the answer.
            decided = producer_response(
                ProducerStreamClosed(), producer_epoch=producer_epoch, offset=offset
            )
        assert decided is not None
        await _send_producer_response(send, decided, cors)
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
    decided = producer_response(
        pr, producer_epoch=producer_epoch, stream_closed=result.stream_closed
    )
    if decided is not None:
        await _send_producer_response(send, decided, cors)
        return


# =============================================================================
# DELETE — Delete stream
# =============================================================================


async def _handle_delete(
    path: str,
    send: Send,
    store: StreamServerStore,
    cors: dict[str, str],
) -> None:
    if not await store.run_sync(store.has, path):
        await _send_error(send, 404, b"Stream not found", cors)
        return

    await store.run_sync(store.delete, path)
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
            await _send_error(send, 400, b"Invalid JSON body", cors)
            return

        path = config.get("path")
        if not path:
            await _send_error(send, 400, b"Missing required field: path", cors)
            return

        injected_faults[path] = InjectedFault(
            count=config.get("count", 1),
            status=config.get("status"),
            retry_after=config.get("retryAfter"),
            delay_ms=config.get("delayMs"),
            drop_connection=config.get("dropConnection", False),
            truncate_body_bytes=config.get("truncateBodyBytes"),
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
        await _send_error(send, 405, b"Method not allowed", cors)


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
