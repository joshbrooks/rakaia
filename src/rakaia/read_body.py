"""What a reader is sent for a page of messages (#396).

`catch_up_body` is the body of a plain or long-poll read; `sse_frames` is one
page of a live (SSE) read; `sse_encoding` says whether a stream's live frames
carry base64. All three are pure: the server reads, calls one, and sends what
it returns. Which headers and control fields a response carries is decided in
`read_decision`; this module only encodes.

`reshape` is the stream's `ServerOptions.reshape_events` function, or None. It
applies only to JSON streams. It takes one decoded event and returns the value
to send, or None to send nothing; a skipped event still counts, so in a live
read the last message of a page always gets its control frame, which is what
carries the reader past it.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from typing import Any

from .json_mode import format_json_response, is_json_content_type
from .read_decision import sse_control_fields
from .types import StreamMessage

__all__ = ["catch_up_body", "sse_encoding", "sse_frames"]

Reshape = Callable[[Any], Any]


def _payloads(
    content_type: str | None, messages: list[StreamMessage], reshape: Reshape | None
) -> list[bytes | None]:
    """Each message as sent: as stored, or reshaped, with None for a skip."""
    if reshape is None or not is_json_content_type(content_type):
        return [m.data for m in messages]
    sent: list[bytes | None] = []
    for message in messages:
        value = reshape(json.loads(message.data))
        sent.append(
            None if value is None else json.dumps(value, separators=(",", ":")).encode()
        )
    return sent


def catch_up_body(
    content_type: str | None, messages: list[StreamMessage], reshape: Reshape | None
) -> tuple[bytes, dict[str, str]]:
    """The body for `messages`, and any headers it adds.

    A JSON stream is one JSON array; anything else, the payloads joined. A
    reshaped read is sent ``no-store``: its ETag names a range of the log, not
    the shape it was sent in, so a cache revalidating it after the function
    changed would keep the old shape.
    """
    kept = [p for p in _payloads(content_type, messages, reshape) if p is not None]
    if not is_json_content_type(content_type):
        return b"".join(kept), {}
    headers = {} if reshape is None else {"cache-control": "no-store"}
    return format_json_response(kept), headers


def sse_encoding(content_type: str | None) -> str | None:
    """``"base64"`` when the stream's bytes cannot go in an SSE frame as text."""
    ct = (content_type or "").lower().split(";")[0].strip()
    if ct.startswith("text/") or ct == "application/json":
        return None
    return "base64"


def _data_lines(payload: str) -> str:
    """`payload` as SSE ``data:`` lines, ending the frame.

    Protocol §5.7: any ``\\r\\n`` or stray ``\\r`` is turned into ``\\n`` first,
    so each becomes its own ``data:`` line rather than ending the frame early.
    """
    lines = payload.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    return "".join(f"data:{line}\n" for line in lines) + "\n"


def _control(fields: dict[str, Any]) -> bytes:
    return f"event: control\n{_data_lines(json.dumps(fields))}".encode()


def sse_frames(
    messages: list[StreamMessage],
    *,
    content_type: str | None,
    tail_offset: str,
    closed: bool,
    up_to_date: bool,
    response_cursor: str,
    reshape: Reshape | None,
) -> bytes:
    """One page of a live read: a data frame and its control frame for each
    message, or a single control frame at the tail when there are none.

    `tail_offset`, `closed` and `up_to_date` describe the stream as the page was
    read; they decide each control frame's fields (`sse_control_fields`).
    """

    def control(offset: str) -> bytes:
        return _control(
            sse_control_fields(
                control_offset=offset,
                tail_offset=tail_offset,
                closed=closed,
                up_to_date=up_to_date,
                response_cursor=response_cursor,
            )
        )

    if not messages:
        return control(tail_offset)

    encoding = sse_encoding(content_type)
    is_json = is_json_content_type(content_type)
    out = b""
    payloads = _payloads(content_type, messages, reshape)
    for n, (message, payload) in enumerate(zip(messages, payloads, strict=True)):
        if payload is None:
            if n == len(messages) - 1:
                out += control(message.offset)
            continue
        if encoding == "base64":
            text = base64.b64encode(payload).decode()
        elif is_json:
            text = format_json_response([payload]).decode("utf-8")
        else:
            text = payload.decode("utf-8")
        out += f"event: data\n{_data_lines(text)}".encode()
        out += control(message.offset)
    return out
