"""Tests for rakaia.json_mode helpers."""

from __future__ import annotations

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
    def test_single_object_appends_trailing_comma(self):
        result = process_json_append(b'{"foo": "bar"}')
        assert result.endswith(b",")
        assert b'"foo"' in result
        assert b'"bar"' in result

    def test_array_flattens_elements(self):
        result = process_json_append(b'[{"a": 1}, {"b": 2}]')
        # Each element gets its own trailing comma
        assert result.count(b",") >= 2
        assert b'"a"' in result
        assert b'"b"' in result

    def test_empty_array_on_create_returns_empty(self):
        result = process_json_append(b"[]", is_initial_create=True)
        assert result == b""

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
        result = process_json_append(b"42")
        assert result == b"42,"

    def test_string_value_works(self):
        result = process_json_append(b'"hello"')
        assert result == b'"hello",'


class TestFormatJsonResponse:
    def test_empty_returns_empty_array(self):
        assert format_json_response(b"") == b"[]"

    def test_single_element_strips_trailing_comma(self):
        assert format_json_response(b'{"a":1},') == b'[{"a":1}]'

    def test_multiple_elements_wrapped(self):
        assert format_json_response(b'{"a":1},{"b":2},') == b'[{"a":1},{"b":2}]'

    def test_no_trailing_comma_still_wraps(self):
        assert format_json_response(b'{"a":1}') == b'[{"a":1}]'
