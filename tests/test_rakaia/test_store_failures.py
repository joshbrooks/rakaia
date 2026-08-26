"""The named store failures, and the status each one becomes.

Before these types existed, `handler.py` chose a status by matching English in
`str(e)`. Rewording an f-string in `store.py` turned a 4xx into an unhandled
500, and nothing failed: `test_store.py` asserted `pytest.raises(ValueError)`,
`test_handler.py` asserted status codes, and no test connected the two. These
do.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from rakaia import StreamStore
from rakaia.handler import STORE_FAILURE_STATUS, _status_for
from rakaia.types import (
    ContentTypeMismatch,
    EmptyJsonArray,
    InvalidJson,
    InvalidOffset,
    SequenceConflict,
    StreamConfigConflict,
    StreamError,
    StreamNotFound,
)
from tests.asgi_client import asgi_client


@pytest_asyncio.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with asgi_client(StreamStore()) as ac:
        yield ac


def _subclasses(cls: type) -> set[type]:
    found = set()
    for sub in cls.__subclasses__():
        found.add(sub)
        found |= _subclasses(sub)
    return found


def _rakaia_failures() -> set[type]:
    """Every failure rakaia itself defines.

    Restricted to this package because `__subclasses__` also sees failures
    defined elsewhere — a downstream backend's, or one a test declares a few
    lines below. Those are not rakaia's to map, and `_status_for` resolves them
    along the MRO anyway; what must not gain a hole is the set shipped here.
    """
    return {c for c in _subclasses(StreamError) if c.__module__.startswith("rakaia.")}


class TestFailureSetIsClosed:
    """The mapping is the contract — it may not silently gain a hole."""

    def test_every_failure_has_a_status(self) -> None:
        """A new StreamError subclass must be given a status here.

        This is the test that makes the seam uncheatable: add a failure to
        `types.py`, forget the status, and the server would 500 on it. Now this
        fails instead.
        """
        unmapped = _rakaia_failures() - set(STORE_FAILURE_STATUS)
        assert unmapped == set(), (
            f"store failures with no status in STORE_FAILURE_STATUS: "
            f"{sorted(c.__name__ for c in unmapped)}"
        )

    def test_statuses_are_client_errors(self) -> None:
        for failure, (status, _body) in STORE_FAILURE_STATUS.items():
            assert 400 <= status < 500, f"{failure.__name__} maps to {status}"

    def test_a_specialized_failure_inherits_its_parents_status(self) -> None:
        """A store may narrow a failure; it must not thereby become a 500.

        `test_every_failure_has_a_status` only sees subclasses that happen to
        be imported, so it cannot police a failure defined in someone else's
        backend package. Resolving along the MRO covers those too.
        """

        class ShardNotFound(StreamNotFound):
            """As some other backend might define it."""

        assert (
            _status_for(ShardNotFound("gone")) == STORE_FAILURE_STATUS[StreamNotFound]
        )

    def test_an_unmapped_failure_still_propagates(self) -> None:
        """The 500 path is deliberate, not an accident of the lookup."""

        class Unmapped(StreamError):
            pass

        assert _status_for(Unmapped()) is None


class TestCompatibility:
    """Each failure subclasses the builtin it replaced."""

    @pytest.mark.parametrize(
        "failure",
        [StreamConfigConflict, SequenceConflict, ContentTypeMismatch],
    )
    def test_store_failures_are_value_errors(self, failure: type) -> None:
        assert issubclass(failure, ValueError)

    @pytest.mark.parametrize("failure", [InvalidJson, EmptyJsonArray])
    def test_json_failures_are_value_errors(self, failure: type) -> None:
        assert issubclass(failure, ValueError)

    def test_not_found_is_a_key_error(self) -> None:
        assert issubclass(StreamNotFound, KeyError)


class TestStoreRaisesTheNamedFailure:
    """The store's half of the contract — no HTTP involved."""

    def test_create_with_different_config(self) -> None:
        store = StreamStore()
        store.create("s", content_type="text/plain")
        with pytest.raises(StreamConfigConflict):
            store.create("s", content_type="application/json")

    def test_append_to_missing_stream(self) -> None:
        with pytest.raises(StreamNotFound):
            StreamStore().append("nope", b"data")

    def test_content_type_mismatch(self) -> None:
        from rakaia import AppendOptions

        store = StreamStore()
        store.create("s", content_type="text/plain")
        with pytest.raises(ContentTypeMismatch):
            store.append("s", b"x", AppendOptions(content_type="application/json"))

    def test_sequence_conflict(self) -> None:
        from rakaia import AppendOptions

        store = StreamStore()
        store.create("s")
        store.append("s", b"a", AppendOptions(seq="5"))
        with pytest.raises(SequenceConflict):
            store.append("s", b"b", AppendOptions(seq="5"))

    def test_invalid_json(self) -> None:
        store = StreamStore()
        store.create("s", content_type="application/json")
        with pytest.raises(InvalidJson):
            store.append("s", b"{not json")

    def test_empty_json_array(self) -> None:
        store = StreamStore()
        store.create("s", content_type="application/json")
        with pytest.raises(EmptyJsonArray):
            store.append("s", b"[]")


class TestFailureBecomesStatus:
    """The server's half — each failure, raised for real, over HTTP."""

    @pytest.mark.asyncio
    async def test_missing_stream_is_404(self, client: httpx.AsyncClient) -> None:
        assert (await client.get("/nope")).status_code == 404

    @pytest.mark.asyncio
    async def test_stream_not_found_from_read_is_404(self) -> None:
        """`StreamNotFound` raised by the store itself, past the has() guard.

        `test_missing_stream_is_404` never reaches the store — the handler's
        own absent-stream check answers first. This one creates the stream over
        HTTP (the handler keys streams by the full request path, "/s") so the
        failure genuinely travels store → mapping → status.
        """
        store = StreamStore()

        def _gone(*_args: object, **_kwargs: object) -> None:
            raise StreamNotFound("raised by the store, not the handler")

        async with asgi_client(store) as ac:
            await ac.put("/s")
            store.read = _gone  # type: ignore[method-assign]
            assert (await ac.get("/s")).status_code == 404

    @pytest.mark.asyncio
    async def test_config_conflict_is_409(self, client: httpx.AsyncClient) -> None:
        await client.put("/s", headers={"content-type": "text/plain"})
        r = await client.put("/s", headers={"content-type": "application/json"})
        assert r.status_code == 409

    @pytest.mark.asyncio
    async def test_content_type_mismatch_is_409(
        self, client: httpx.AsyncClient
    ) -> None:
        await client.put("/s", headers={"content-type": "text/plain"})
        r = await client.post(
            "/s", content=b"x", headers={"content-type": "application/json"}
        )
        assert r.status_code == 409

    @pytest.mark.asyncio
    async def test_sequence_conflict_is_409(self, client: httpx.AsyncClient) -> None:
        headers = {"content-type": "text/plain", "stream-seq": "5"}
        await client.put("/s", headers={"content-type": "text/plain"})
        await client.post("/s", content=b"a", headers=headers)
        r = await client.post("/s", content=b"b", headers=headers)
        assert r.status_code == 409

    @pytest.mark.asyncio
    async def test_seq_compares_lexicographically_not_numerically(
        self, client: httpx.AsyncClient
    ) -> None:
        """Stream-Seq "10" after "9" is a conflict, as the protocol requires.

        The values are opaque strings compared byte-wise, and "10" < "9". A
        writer that wants its values to order pads them to a fixed width — the
        same idiom rakaia's own offsets use — so "09" then "10" is accepted.
        """
        await client.put("/s", headers={"content-type": "text/plain"})
        await client.post(
            "/s",
            content=b"a",
            headers={"content-type": "text/plain", "stream-seq": "9"},
        )
        r = await client.post(
            "/s",
            content=b"b",
            headers={"content-type": "text/plain", "stream-seq": "10"},
        )
        assert r.status_code == 409

        await client.put("/padded", headers={"content-type": "text/plain"})
        first = await client.post(
            "/padded",
            content=b"a",
            headers={"content-type": "text/plain", "stream-seq": "09"},
        )
        second = await client.post(
            "/padded",
            content=b"b",
            headers={"content-type": "text/plain", "stream-seq": "10"},
        )
        assert first.status_code in (200, 204)
        assert second.status_code in (200, 204), '"10" must follow padded "09"'

    @pytest.mark.asyncio
    async def test_a_non_numeric_seq_is_accepted(
        self, client: httpx.AsyncClient
    ) -> None:
        """The header is an opaque string, so a ULID is a conforming value and
        must not be refused as malformed."""
        await client.put("/s", headers={"content-type": "text/plain"})
        r = await client.post(
            "/s",
            content=b"a",
            headers={
                "content-type": "text/plain",
                "stream-seq": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            },
        )
        assert r.status_code in (200, 204)

    @pytest.mark.asyncio
    async def test_invalid_json_is_400(self, client: httpx.AsyncClient) -> None:
        await client.put("/s", headers={"content-type": "application/json"})
        r = await client.post(
            "/s", content=b"{not json", headers={"content-type": "application/json"}
        )
        assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_empty_array_is_400(self, client: httpx.AsyncClient) -> None:
        await client.put("/s", headers={"content-type": "application/json"})
        r = await client.post(
            "/s", content=b"[]", headers={"content-type": "application/json"}
        )
        assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_message_text_is_not_load_bearing(self) -> None:
        """Reword a failure's message; the status must not move.

        This is the regression the old substring mapping could not survive.
        """
        store = StreamStore()

        def _reworded(*_args: object, **_kwargs: object) -> None:
            raise StreamNotFound("wholly different wording")

        async with asgi_client(store) as ac:
            # Create over HTTP — the handler keys streams by the full request
            # path ("/s"), so store.create("s") would leave the handler's
            # absent-stream 404 to answer before the patched read ever ran.
            await ac.put("/s")
            store.read = _reworded  # type: ignore[method-assign]
            assert (await ac.get("/s")).status_code == 404

    @pytest.mark.asyncio
    async def test_sse_store_failure_is_a_status_not_a_crash(self) -> None:
        """A StreamError from the first SSE read must answer with its status.

        The streaming 200 used to start before the first read, so a failure
        there could only unwind into a second ``http.response.start`` — under a
        real server, a crashed connection instead of a 400.
        """
        store = StreamStore()

        def _rejects(*_args: object, **_kwargs: object) -> None:
            raise InvalidOffset("offset belongs to no message")

        async with asgi_client(store) as ac:
            await ac.put("/s")
            store.read = _rejects  # type: ignore[method-assign]
            r = await ac.get("/s", params={"offset": "0_0", "live": "sse"})
            assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_sse_with_a_foreign_offset_is_400(
        self, client: httpx.AsyncClient
    ) -> None:
        """The unpatched end-to-end case: `?offset=42&live=sse` on a store
        whose offsets are compound.

        The syntactic guard admits plain integers — it admits any shape a store
        might issue (#226) — so it no longer rejects this before the store does — the store's `InvalidOffset` must come back as a 400,
        not crash an already-started SSE response.
        """
        await client.put("/s")
        r = await client.get("/s", params={"offset": "42", "live": "sse"})
        assert r.status_code == 400
