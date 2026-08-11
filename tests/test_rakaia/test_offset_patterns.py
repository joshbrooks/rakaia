"""Tests for the shared offset/TTL/content-type validation patterns.

``VALID_OFFSET_PATTERN`` is the single source of truth in ``rakaia.types`` and is
imported by every protocol server (#41). ``VALID_TTL_PATTERN`` /
``VALID_CONTENT_TYPE_PATTERN`` live in ``rakaia.handler``.
"""

from __future__ import annotations

import pytest

from rakaia.handler import VALID_CONTENT_TYPE_PATTERN, VALID_TTL_PATTERN
from rakaia.types import VALID_OFFSET_PATTERN


class TestValidOffsetPattern:
    @pytest.mark.parametrize("value", ["-1", "now", "0_0", "123_456", "999999_0"])
    def test_accepts_canonical_and_sentinels(self, value: str) -> None:
        assert VALID_OFFSET_PATTERN.match(value)

    @pytest.mark.parametrize("value", ["1", "00000000000000000001", "42"])
    def test_accepts_the_durable_stores_plain_integer_format(self, value: str) -> None:
        """A bare integer is `DjangoStreamStore`'s offset format.

        This used to be asserted as malformed, which was harmless only while
        the server could not be handed that store. Once it could, every resume
        read 400'd on an offset the server had just issued itself. The protocol
        makes offsets opaque, not one format (§6).
        """
        assert VALID_OFFSET_PATTERN.match(value)

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "1_",  # missing byte
            "_1",  # missing seq
            "1_2_3",  # too many components
            "-2",  # only -1 is a valid sentinel
            "NOW",  # case-sensitive
            "1_-2",  # negative byte
            "abc",
            "1.2",
            " 0_0",  # leading space
            "0_0 ",  # trailing space
        ],
    )
    def test_rejects_malformed(self, value: str) -> None:
        assert VALID_OFFSET_PATTERN.match(value) is None


class TestValidTtlPattern:
    @pytest.mark.parametrize("value", ["0", "1", "60", "86400"])
    def test_accepts_non_negative_ints(self, value: str) -> None:
        assert VALID_TTL_PATTERN.match(value)

    @pytest.mark.parametrize("value", ["", "-1", "01", "1.5", "abc", " 1"])
    def test_rejects_malformed(self, value: str) -> None:
        assert VALID_TTL_PATTERN.match(value) is None


class TestValidContentTypePattern:
    @pytest.mark.parametrize(
        "value", ["text/plain", "application/json", "application/octet-stream"]
    )
    def test_accepts_typical(self, value: str) -> None:
        assert VALID_CONTENT_TYPE_PATTERN.match(value)

    @pytest.mark.parametrize("value", ["", "plain", "/json", "text"])
    def test_rejects_malformed(self, value: str) -> None:
        assert VALID_CONTENT_TYPE_PATTERN.match(value) is None
