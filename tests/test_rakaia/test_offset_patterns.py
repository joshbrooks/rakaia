"""Tests for the shared offset/TTL/content-type validation.

Offset validity lives in ``rakaia.offsets.is_syntactically_valid`` (#226) — the
last of a stream position's rules to move there, after #206 moved the other
four. ``VALID_TTL_PATTERN`` / ``VALID_CONTENT_TYPE_PATTERN`` still live in
``rakaia.handler``.

**The offset cases assert the request's outcome, not which layer produced it.**
The old version asserted a regex directly, and that pinned the wrong thing twice
over: it made the *server's* guard the authority on what an offset may look
like, and it recorded tokens as "malformed" when the only true statement was "no
rakaia store issues this shape". A store refuses a token it did not issue with
`ForeignOffset`, which is an `InvalidOffset`, which the handler maps to 400 — so
junk that now reaches the store still 400s, one layer later, and the status is
the contract.
"""

from __future__ import annotations

import pytest

from rakaia import StreamStore
from rakaia.handler import VALID_CONTENT_TYPE_PATTERN, VALID_TTL_PATTERN
from rakaia.offsets import MAX_OFFSET_LENGTH, is_syntactically_valid
from tests.asgi_client import asgi_client


async def _status_for_offset(offset: str) -> int:
    """The status a real GET carrying this raw offset returns, end to end."""
    store = StreamStore()
    store.create("/s")
    store.append("/s", b'{"n": 1}')
    async with asgi_client(store) as client:
        return (await client.get(f"/s?offset={offset}")).status_code


class TestAnOffsetNoStoreCouldHaveIssuedIsRefused:
    """The guard's whole remaining job: reject what a URL cannot carry."""

    @pytest.mark.parametrize("char", [",", "&", "=", "?", "/"])
    def test_the_five_characters_the_spec_forbids(self, char: str) -> None:
        """`docs/protocol.md` §6 names these exactly, because each would split
        one offset into several query parameters."""
        assert not is_syntactically_valid(f"12{char}34")

    @pytest.mark.parametrize("value", ["", " ", "0_0 ", " 0_0", "a\tb", "a\nb"])
    def test_empty_and_whitespace(self, value: str) -> None:
        """§6 does not name whitespace, but it asks for URL-safe characters, and
        a token with a space in it is a mangled request rather than a store's
        considered choice of format."""
        assert not is_syntactically_valid(value)

    def test_the_ceiling_is_the_one_the_spec_names(self) -> None:
        """The literal, not the constant.

        Asserting the guard against `MAX_OFFSET_LENGTH` only proves the two
        agree; the constant could be moved to 40 or 1024 and every other case
        here would still pass. 40 would refuse exactly the conforming
        third-party offset this change exists to admit, so the number is the
        thing worth pinning.
        """
        assert MAX_OFFSET_LENGTH == 256, "docs/protocol.md §6: 'under 256'"

    def test_the_length_ceiling(self) -> None:
        assert is_syntactically_valid("9" * (MAX_OFFSET_LENGTH - 1))
        assert not is_syntactically_valid("9" * MAX_OFFSET_LENGTH)


class TestAnOffsetSomeStoreMightHaveIssuedIsPassedOn:
    """What #226 asked for: the guard stops enumerating rakaia's own formats.

    Every value here was refused at the URL layer before, on the grounds that it
    is neither of our two shapes — which made `StreamServerStore`, a documented
    seam with its own conformance suite, a promise the server itself broke.
    """

    @pytest.mark.parametrize(
        "value",
        [
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",  # a ULID; §6 names them as acceptable
            "abc",
            "1.2",
            "-2",
            "NOW",
            "1_",
            "_1",
            "1_2_3",
            "1_-2",
        ],
    )
    def test_an_unfamiliar_shape_reaches_the_store(self, value: str) -> None:
        assert is_syntactically_valid(value)

    @pytest.mark.parametrize(
        "value", ["abc", "1.2", "-2", "01ARZ3NDEKTSV4RRFFQ69G5FAV"]
    )
    async def test_and_the_store_still_makes_it_a_400(self, value: str) -> None:
        """Passing the guard is not passing the request. This is what makes
        widening the guard safe rather than merely tidier."""
        assert await _status_for_offset(value) == 400


class TestWhatTheStoresActuallyIssueStillWorks:
    @pytest.mark.parametrize("value", ["-1", "now", "0_0", "123_456", "999999_0"])
    def test_the_in_memory_format_and_the_sentinels(self, value: str) -> None:
        assert is_syntactically_valid(value)

    @pytest.mark.parametrize("value", ["1", "00000000000000000001", "42"])
    def test_the_plain_integer_format(self, value: str) -> None:
        """Shared by `DjangoStreamStore` and `JsonlStreamStore`.

        Asserted as malformed once, which was harmless only while the server
        could not be handed either store. The moment it could, every resume read
        400'd on an offset the server had just issued itself — this issue's own
        precedent, and the reason the rule stopped enumerating formats.
        """
        assert is_syntactically_valid(value)

    async def test_a_real_resume_round_trips(self) -> None:
        """The control for the whole file: an offset the store *did* issue is
        read back, so none of the above passes by refusing everything."""
        store = StreamStore()
        store.create("/s", content_type="application/json")
        store.append("/s", b'{"n": 1}')
        head = store.get_current_offset("/s")
        store.append("/s", b'{"n": 2}')

        async with asgi_client(store) as client:
            response = await client.get(f"/s?offset={head}")

        assert response.status_code == 200
        # The second message only: the offset resolved to a real position rather
        # than to the start of the stream.
        assert response.json() == [{"n": 2}]


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
