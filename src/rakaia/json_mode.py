"""
JSON mode helpers for the Durable Streams server.

Handles JSON validation, array flattening, and response formatting
for streams with Content-Type: application/json.
"""

from __future__ import annotations

import json
from typing import Any


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


def process_json_append(data: bytes, is_initial_create: bool = False) -> bytes:
    """
    Process JSON data for append in JSON mode.

    - Validates JSON
    - Extracts array elements if data is an array (one-level flatten)
    - Appends trailing comma for easy concatenation

    Args:
        data: Raw bytes of the JSON body
        is_initial_create: If True, empty arrays are allowed (creates empty stream)

    Returns:
        Processed bytes ready for storage (with trailing comma)

    Raises:
        ValueError: If JSON is invalid or array is empty (on non-create append)
    """
    text = data.decode("utf-8")

    try:
        parsed: Any = json.loads(text)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"Invalid JSON: {e}") from e

    if isinstance(parsed, list):
        if len(parsed) == 0:
            if is_initial_create:
                # Empty array on create = empty stream, no data to store
                return b""
            raise ValueError("Empty arrays are not allowed in append operations")

        # Flatten one level: each element becomes a separate message
        # Store as individual JSON values with trailing commas
        parts: list[bytes] = []
        for item in parsed:
            parts.append(json.dumps(item, separators=(",", ":")).encode("utf-8") + b",")
        return b"".join(parts)
    else:
        # Single value - store with trailing comma
        return json.dumps(parsed, separators=(",", ":")).encode("utf-8") + b","


def format_json_response(data: bytes) -> bytes:
    """
    Format JSON mode response by wrapping stored data in array brackets.

    Strips trailing comma before wrapping. Stored data has trailing commas
    between elements for easy concatenation.

    Args:
        data: Concatenated stored message data

    Returns:
        JSON array bytes: [element1,element2,...]
    """
    if len(data) == 0:
        return b"[]"

    # Strip trailing comma before wrapping in array brackets
    trimmed = data
    if trimmed.endswith(b","):
        trimmed = trimmed[:-1]

    return b"[" + trimmed + b"]"
