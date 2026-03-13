"""
Lightweight ASGI request/response helpers.

No external dependencies — provides the minimal utilities needed for
the Durable Streams HTTP handler to work as a plain ASGI application.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs

# ASGI type aliases
Scope = dict[str, Any]
Receive = Any  # Callable that returns ASGI events
Send = Any  # Callable that accepts ASGI events


async def read_body(receive: Receive) -> bytes:
    """Read the full request body from an ASGI receive callable."""
    body = b""
    while True:
        message = await receive()
        body += message.get("body", b"")
        if not message.get("more_body", False):
            break
    return body


def get_method(scope: Scope) -> str:
    """Get the HTTP method from an ASGI scope."""
    return scope.get("method", "GET").upper()


def get_path(scope: Scope) -> str:
    """Get the URL path from an ASGI scope."""
    return scope.get("path", "/")


def get_query_string(scope: Scope) -> str:
    """Get the raw query string from an ASGI scope."""
    return scope.get("query_string", b"").decode("latin-1")


def parse_query_params(query_string: str) -> dict[str, list[str]]:
    """Parse URL query parameters into a dict of key -> list of values."""
    return parse_qs(query_string, keep_blank_values=True)


def get_query_param(query_string: str, name: str) -> str | None:
    """Get a single query parameter value, or None if not present."""
    params = parse_qs(query_string, keep_blank_values=True)
    values = params.get(name)
    if values:
        return values[0]
    return None


def get_all_query_params(query_string: str, name: str) -> list[str]:
    """Get all values for a query parameter."""
    params = parse_qs(query_string, keep_blank_values=True)
    return params.get(name, [])


def get_header(scope: Scope, name: str) -> str | None:
    """
    Get a request header value by name (case-insensitive).

    ASGI headers are stored as list of (name, value) byte pairs, all lowercase.
    """
    name_lower = name.lower().encode("latin-1")
    for header_name, header_value in scope.get("headers", []):
        if header_name == name_lower:
            return header_value.decode("latin-1")
    return None


def make_headers(headers_dict: dict[str, str]) -> list[tuple[bytes, bytes]]:
    """Convert a dict of headers to ASGI header format."""
    return [
        (k.lower().encode("latin-1"), v.encode("latin-1"))
        for k, v in headers_dict.items()
    ]


async def send_response(
    send: Send,
    status: int,
    headers: dict[str, str] | None = None,
    body: bytes = b"",
) -> None:
    """Send a complete HTTP response."""
    response_headers = make_headers(headers or {})
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": response_headers,
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": body,
        }
    )


async def start_streaming_response(
    send: Send,
    status: int,
    headers: dict[str, str] | None = None,
) -> None:
    """Start a streaming HTTP response (for SSE)."""
    response_headers = make_headers(headers or {})
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": response_headers,
        }
    )


async def send_body_chunk(
    send: Send,
    data: bytes,
    more_body: bool = True,
) -> None:
    """Send a chunk of response body."""
    await send(
        {
            "type": "http.response.body",
            "body": data,
            "more_body": more_body,
        }
    )


async def send_sse_event(
    send: Send,
    event_type: str,
    data: str,
) -> None:
    """
    Send a Server-Sent Event.

    Format per SSE spec:
        event: <type>
        data: <line1>
        data: <line2>
        <blank line>
    """
    lines = data.split("\n")
    event = f"event: {event_type}\n"
    for line in lines:
        event += f"data:{line}\n"
    event += "\n"
    await send_body_chunk(send, event.encode("utf-8"))
