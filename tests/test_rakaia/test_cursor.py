"""Tests for rakaia.cursor."""

from __future__ import annotations

from unittest.mock import patch

from rakaia.cursor import (
    DEFAULT_CURSOR_EPOCH,
    CursorOptions,
    calculate_cursor,
    generate_response_cursor,
)


class TestCalculateCursor:
    def test_returns_string(self):
        result = calculate_cursor()
        assert isinstance(result, str)
        assert int(result) >= 0

    def test_default_options(self):
        with patch("rakaia.cursor.time.time", return_value=DEFAULT_CURSOR_EPOCH + 40.0):
            # 40 seconds / 20s interval = 2
            assert calculate_cursor() == "2"

    def test_custom_interval(self):
        opts = CursorOptions(interval_seconds=10)
        with patch("rakaia.cursor.time.time", return_value=DEFAULT_CURSOR_EPOCH + 40.0):
            # 40 seconds / 10s interval = 4
            assert calculate_cursor(opts) == "4"

    def test_custom_epoch(self):
        opts = CursorOptions(epoch=1000.0, interval_seconds=20)
        with patch("rakaia.cursor.time.time", return_value=1100.0):
            # (1100 - 1000) / 20 = 5
            assert calculate_cursor(opts) == "5"

    def test_at_epoch_returns_zero(self):
        with patch("rakaia.cursor.time.time", return_value=DEFAULT_CURSOR_EPOCH):
            assert calculate_cursor() == "0"


class TestGenerateResponseCursor:
    def test_no_client_cursor_returns_current(self):
        with patch("rakaia.cursor.time.time", return_value=DEFAULT_CURSOR_EPOCH + 40.0):
            assert generate_response_cursor(None) == "2"

    def test_empty_client_cursor_returns_current(self):
        with patch("rakaia.cursor.time.time", return_value=DEFAULT_CURSOR_EPOCH + 40.0):
            assert generate_response_cursor("") == "2"

    def test_client_behind_returns_current(self):
        with patch(
            "rakaia.cursor.time.time", return_value=DEFAULT_CURSOR_EPOCH + 100.0
        ):
            # current = 5, client = 2 → return current
            assert generate_response_cursor("2") == "5"

    def test_invalid_client_cursor_falls_back_to_current(self):
        with patch("rakaia.cursor.time.time", return_value=DEFAULT_CURSOR_EPOCH + 40.0):
            assert generate_response_cursor("not-an-int") == "2"

    def test_client_at_or_ahead_adds_jitter(self):
        # When client is at/ahead of current, response should be > client
        with patch("rakaia.cursor.time.time", return_value=DEFAULT_CURSOR_EPOCH + 40.0):
            # current = 2, client = 5 → return 5 + jitter
            result = generate_response_cursor("5")
            assert int(result) > 5

    def test_jitter_is_at_least_one_interval(self):
        with patch("rakaia.cursor.time.time", return_value=DEFAULT_CURSOR_EPOCH + 40.0):
            # Jitter should always advance by at least 1 interval
            result = generate_response_cursor("10")
            assert int(result) >= 11
