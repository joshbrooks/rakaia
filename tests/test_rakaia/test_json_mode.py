"""Tests for rakaia.json_mode helpers."""

from __future__ import annotations

import json

import pytest

from rakaia.json_mode import (
    format_json_response,
    is_json_content_type,
    normalize_content_type,
    process_json_append,
)


class TestNormalizeContentType:
    def test_none_returns_octet_stream(self):
        assert normalize_content_type(None) == "application/octet-stream"

    def test_empty_returns_octet_stream(self):
        assert normalize_content_type("") == "application/octet-stream"

    def test_strips_charset_param(self):
        assert (
            normalize_content_type("application/json; charset=utf-8")
            == "application/json"
        )

    def test_lowercases(self):
        assert normalize_content_type("Application/JSON") == "application/json"

    def test_strips_whitespace(self):
        assert normalize_content_type("  application/json  ") == "application/json"


class TestIsJsonContentType:
    def test_plain_json(self):
        assert is_json_content_type("application/json") is True

    def test_json_with_charset(self):
        assert is_json_content_type("application/json; charset=utf-8") is True

    def test_octet_stream(self):
        assert is_json_content_type("application/octet-stream") is False

    def test_text_plain(self):
        assert is_json_content_type("text/plain") is False

    def test_none(self):
        assert is_json_content_type(None) is False


class TestProcessJsonAppend:
    def test_single_object_is_one_unframed_message(self):
        # The payload is stored exactly as it will be read back. It carried a
        # trailing comma until #155, which made it undecodable to anything
        # reading the message directly.
        assert process_json_append(b'{"foo": "bar"}') == [b'{"foo":"bar"}']

    def test_array_flattens_into_one_message_per_element(self):
        # The spec: a two-element body "stores two messages" (7.1).
        assert process_json_append(b'[{"a": 1}, {"b": 2}]') == [
            b'{"a":1}',
            b'{"b":2}',
        ]

    def test_empty_array_on_create_stores_nothing(self):
        assert process_json_append(b"[]", is_initial_create=True) == []

    def test_empty_array_on_append_raises(self):
        with pytest.raises(ValueError, match="Empty arrays"):
            process_json_append(b"[]", is_initial_create=False)

    def test_invalid_json_raises(self):
        with pytest.raises(ValueError, match="Invalid JSON"):
            process_json_append(b"not-json")

    def test_invalid_utf8_raises(self):
        # The decode happens before the try/except, so UnicodeDecodeError
        # propagates as-is (it is itself a subclass of ValueError).
        with pytest.raises(UnicodeDecodeError):
            process_json_append(b"\xff\xfe")

    def test_scalar_value_works(self):
        assert process_json_append(b"42") == [b"42"]

    def test_string_value_works(self):
        assert process_json_append(b'"hello"') == [b'"hello"']


class TestFormatJsonResponse:
    def test_empty_returns_empty_array(self):
        assert format_json_response([]) == b"[]"

    def test_single_element_wrapped(self):
        assert format_json_response([b'{"a":1}']) == b'[{"a":1}]'

    def test_multiple_elements_joined_and_wrapped(self):
        # The separators are added here and exist nowhere else.
        assert format_json_response([b'{"a":1}', b'{"b":2}']) == b'[{"a":1},{"b":2}]'

    def test_a_stored_payload_round_trips_through_the_response(self):
        """What is stored is a complete JSON value, and framing is reversible."""
        payloads = process_json_append(b'[{"a": 1}, {"b": 2}]')
        assert [json.loads(p) for p in payloads] == [{"a": 1}, {"b": 2}]
        assert json.loads(format_json_response(payloads)) == [{"a": 1}, {"b": 2}]
