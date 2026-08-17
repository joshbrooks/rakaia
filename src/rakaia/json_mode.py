"""
JSON mode helpers for the Durable Streams server.

Handles JSON validation, array flattening, and response formatting
for streams with Content-Type: application/json.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from .types import EmptyJsonArray, InvalidJson


def normalize_content_type(content_type: str | None) -> str:
    """
    Normalize content-type by extracting the media type (before any semicolon).

    Handles cases like "application/json; charset=utf-8" -> "application/json".
    """
    if not content_type:
        return "application/octet-stream"
    return content_type.split(";")[0].strip().lower()


def is_json_content_type(content_type: str | None) -> bool:
    """Check if a content type is application/json."""
    return normalize_content_type(content_type) == "application/json"


def process_json_append(data: bytes, is_initial_create: bool = False) -> list[bytes]:
    """The messages a JSON-mode append stores, one payload per message.

    The protocol is explicit that message boundaries are preserved and that a
    posted array is flattened one level, "storing two messages" for a two-element
    body (spec 7.1). So this returns a *list* — the boundaries are the list, and
    the caller stores one message per element.

    It used to return the elements concatenated with trailing commas, as a
    single blob, which the response formatter knew how to unwrap. That destroyed
    the boundaries the spec requires and, because the comma rode along in
    `StreamMessage.data`, made a JSON-mode stream undecodable to anything that
    read the payload directly — `replay()` failed on the first event (#155).
    Framing now happens where it belongs, at the response.

    Returns an empty list when there is nothing to store: an empty array on
    create, which the spec allows as "an empty stream".

    Raises:
        InvalidJson: the body is not valid JSON.
        EmptyJsonArray: an empty array on append (a no-op the spec rejects).
    """
    text = data.decode("utf-8")

    try:
        parsed: Any = json.loads(text)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise InvalidJson(f"Invalid JSON: {e}") from e

    if isinstance(parsed, list):
        if len(parsed) == 0:
            if is_initial_create:
                return []
            raise EmptyJsonArray("Empty arrays are not allowed in append operations")
        return [_canonical(item) for item in parsed]
    return [_canonical(parsed)]


def _canonical(value: Any) -> bytes:
    """One stored payload, in the compact form the response concatenates."""
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def format_json_response(payloads: Sequence[bytes]) -> bytes:
    """The GET body for a JSON-mode range: a JSON array of the stored messages.

    Takes the payloads rather than a pre-concatenated blob. Each is already a
    complete JSON value, so framing is a join and a pair of brackets — the
    separators live here and never in what was stored.
    """
    if not payloads:
        return b"[]"
    return b"[" + b",".join(payloads) + b"]"
