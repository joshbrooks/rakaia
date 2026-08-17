"""The one translation between a stored row and the event it represents.

`StreamEvent` is a storage shape. What was actually appended is not quite what
the columns hold: a non-JSON body is stored encoded and marked, an append that
carried no envelope label is recorded under a stable sentinel because the column
is required, and an event with no logical timestamp falls back to when it was
written. Reversing those three facts is what turns a row back into an *event*.

Before #153 that reversal was written six times — three times inside
`django_store` (which agreed) and three times outside it, in the channel-layer
frame, the dashboard views and the admin (which did not). A subscriber saw the
raw `"append"` sentinel where a reader saw `""`, and a base64-stored payload
reached subscribers still base64. This module is the single arrow into the
column layout: everything that renders an event goes through `message_of`, and
nothing else reads those columns directly.

It sits below both `django_store` and `channels_signals` on purpose. `_publish`
imports `broadcast_entries` lazily to avoid a cycle, so the translation cannot
live in either of them without one.
"""

from __future__ import annotations

import base64
import json
from typing import TYPE_CHECKING, Any

from rakaia.json_mode import is_json_content_type
from rakaia.types import StreamMessage

from .offsets import format_offset

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .models import StreamEntry

# StreamEvent.event_type is required metadata for the dashboard; raw stream
# appends carry no type, so they are recorded under a single stable label.
APPEND_EVENT_TYPE = "append"

# `StreamEvent.payload_encoding` values. `None` means the event's `data` is the
# payload as a JSON value — the event-sourcing shape, and what every row written
# before this column holds.
_ENCODING_TEXT = "utf-8"
_ENCODING_BASE64 = "base64"


def encode_payload(payload: bytes, content_type: str | None) -> tuple[Any, str | None]:
    """Render one payload for storage as `(data, payload_encoding)`.

    A protocol stream may declare any content type, but `StreamEvent.data` is a
    JSON column, so a body that is not JSON has to be held as a JSON string and
    marked as such. Three cases, in the order they are decided:

    - **A declared non-JSON content type** (`text/plain`, `text/csv`, …) is
      stored verbatim as text, or base64 if it is not valid UTF-8. Never parsed,
      so the bytes come back exactly as they went in — a text stream that
      happens to contain JSON is still text, and is not silently reformatted.
    - **No declared content type** keeps the event-sourcing behaviour: the body
      is parsed and stored as a JSON value. This is the shape `replay()`, the
      admin and the channel-layer signals all read, so it cannot change. A body
      that will not parse falls back to the raw form rather than failing — that
      path used to raise `json.JSONDecodeError` straight through the server as
      a 500.
    - **JSON mode** is handled by the caller, which validates and flattens
      first; each element arrives here already parsed.
    """
    if content_type is not None and not is_json_content_type(content_type):
        return _encode_raw(payload)
    try:
        return json.loads(payload), None
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _encode_raw(payload)


def _encode_raw(payload: bytes) -> tuple[str, str]:
    try:
        return payload.decode("utf-8"), _ENCODING_TEXT
    except UnicodeDecodeError:
        return base64.b64encode(payload).decode("ascii"), _ENCODING_BASE64


def decode_payload(data: Any, payload_encoding: str | None) -> bytes:
    """The payload bytes for a stored event — the inverse of `encode_payload`."""
    if payload_encoding == _ENCODING_TEXT:
        return str(data).encode("utf-8")
    if payload_encoding == _ENCODING_BASE64:
        return base64.b64decode(str(data))
    return json.dumps(data).encode("utf-8")


def event_label(event_type: str) -> str:
    """The envelope label for a stored ``event_type``.

    The sentinel means "a raw append, which carried no label" — so it inverts to
    the empty string, matching the in-memory store. Callers rendering an event
    must use this rather than the column, or a subscriber is told the sentinel
    is the label.
    """
    return "" if event_type == APPEND_EVENT_TYPE else event_type


def payload_fields(data: Any, payload_encoding: str | None) -> dict[str, Any]:
    """The stored payload as the `data`/`payload_encoding` pair a JSON wire carries.

    For surfaces that emit JSON rather than bytes — the channel-layer frame, the
    SSE view, the dashboard APIs. They cannot carry a `StreamMessage`, whose
    `data` is bytes, so they carry the stored pair and let the consumer run
    `decode_payload` for itself.

    Passing the pair through is what makes that inverse exact. Decoding first and
    re-deriving an encoding from the resulting bytes is lossy: a `text/plain`
    body that happens to parse as JSON (`{"a": 1}\\n`, `1.50`, `  7  `) would be
    republished as a JSON value with no encoding, and reconstructing it yields
    different bytes than `read()` returns — the same divergence #153 exists to
    remove.

    `payload_encoding` is omitted when `None`, so an ordinary JSON payload — the
    common case — keeps exactly the shape these surfaces always had and no
    existing consumer sees a new key.
    """
    fields: dict[str, Any] = {"data": data}
    if payload_encoding is not None:
        fields["payload_encoding"] = payload_encoding
    return fields


def message_of(entry: StreamEntry) -> StreamMessage:
    """The event a stored entry represents.

    The single definition. Reverses the three storage facts — payload encoding,
    the append sentinel, and the logical-timestamp fallback — so that every
    reader of the log describes the same event the same way, whether it arrived
    through `read()`, a channel-layer frame, the dashboard or the admin.

    Reads `entry.event`; pass an entry that already has it loaded (or was
    fetched with `select_related("event")`) to avoid a query per row.
    """
    event = entry.event
    written_at = entry.created_at.timestamp()
    return StreamMessage(
        data=decode_payload(event.data, event.payload_encoding),
        offset=format_offset(entry.offset),
        timestamp=written_at,
        # Logical envelope ts if the producer set one, else the append time —
        # mirroring the in-memory store's default.
        event_ts=event.event_ts if event.event_ts is not None else written_at,
        label=event_label(event.event_type),
        metadata=event.metadata or None,
    )
