"""The Durable Streams protocol, served over the durable store.

`rakaia.create_app` is driven over HTTP with `DjangoStreamStore` behind it —
the arrangement that replaced `django_rakaia.protocol_views`, a second, partial
implementation of the same protocol.

The point of these cases is coverage the deleted implementation did not have.
It routed by verb-in-path (`/streams/x/append`) rather than by method, returned
newline-delimited JSON rather than a JSON array, dropped the envelope, and had
no close, TTL, long-poll, ETag or producer fencing at all. Two of its bugs are
pinned below so a regression would be loud.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from asgiref.sync import sync_to_async

from django_rakaia.django_store import DjangoStreamStore
from rakaia import create_app

pytestmark = pytest.mark.django_db(transaction=True)


@pytest_asyncio.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(store=DjangoStreamStore())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


JSON = {"content-type": "application/json"}


class TestTheProtocolOverTheDurableStore:
    async def test_create_read_append_round_trip(
        self, client: httpx.AsyncClient
    ) -> None:
        assert (await client.put("/s", headers=JSON)).status_code in (200, 201, 204)

        posted = await client.post("/s", content=b'{"id": 1}', headers=JSON)
        assert posted.status_code in (200, 204)

        read = await client.get("/s")
        assert read.status_code == 200
        assert read.json() == [{"id": 1}]

    async def test_read_returns_a_json_array_not_ndjson(
        self, client: httpx.AsyncClient
    ) -> None:
        """The deleted implementation returned newline-delimited JSON."""
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"id": 1}', headers=JSON)
        await client.post("/s", content=b'{"id": 2}', headers=JSON)

        body = (await client.get("/s")).json()
        assert body == [{"id": 1}, {"id": 2}]

    async def test_routing_is_by_method_not_by_verb_in_the_path(
        self, client: httpx.AsyncClient
    ) -> None:
        """`/s/append` is a stream named "s/append", not an operation."""
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"id": 1}', headers=JSON)
        assert (await client.get("/s")).json() == [{"id": 1}]

    async def test_missing_stream_is_404(self, client: httpx.AsyncClient) -> None:
        assert (await client.get("/nope")).status_code == 404

    async def test_head_reports_metadata(self, client: httpx.AsyncClient) -> None:
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"id": 1}', headers=JSON)
        head = await client.head("/s")
        assert head.status_code == 200
        assert head.headers.get("stream-next-offset")

    async def test_delete_removes_the_stream(self, client: httpx.AsyncClient) -> None:
        await client.put("/s", headers=JSON)
        assert (await client.delete("/s")).status_code in (200, 204)
        assert (await client.get("/s")).status_code == 404


class TestBugsTheDeletedImplementationHad:
    """Both of these were live defects in `protocol_views`."""

    async def test_an_offset_resumes_from_the_right_place(
        self, client: httpx.AsyncClient
    ) -> None:
        """The old `parse_offset` did `int(offset.split("_")[1])`.

        A handler-issued offset passes the shared validation pattern, so that
        parse silently produced the wrong window rather than an error — a read
        resuming from the head could return the whole stream again.
        """
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"id": 1}', headers=JSON)
        first = await client.get("/s")
        head = first.headers["stream-next-offset"]

        await client.post("/s", content=b'{"id": 2}', headers=JSON)

        resumed = await client.get(f"/s?offset={head}")
        assert resumed.json() == [{"id": 2}], "resume must not replay seen messages"

    async def test_offset_now_does_not_return_the_whole_stream(
        self, client: httpx.AsyncClient
    ) -> None:
        """The old `parse_offset("now")` returned 0 with a comment saying the
        caller would fix it up. No caller did, so `?offset=now` replayed
        everything — the exact opposite of what it means."""
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"id": 1}', headers=JSON)

        caught_up = await client.get("/s?offset=now")
        assert caught_up.status_code == 200
        assert caught_up.json() == []

    async def test_an_offset_from_the_other_store_is_a_400(
        self, client: httpx.AsyncClient
    ) -> None:
        """Not the wrong window, and not a 500.

        `VALID_OFFSET_PATTERN` accepts both stores' formats — it has to, since
        the protocol makes offsets opaque rather than uniform (§6) — so the
        compound `{seq}_{byte}` form reaches a durable-backed server looking
        perfectly valid. Python's `int()` would even parse it, treating the
        underscore as a digit separator, and read from an unrelated position:
        the same class of silent data loss the deleted implementation had.
        """
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"id": 1}', headers=JSON)

        refused = await client.get("/s?offset=0_5")
        assert refused.status_code == 400


class TestCapabilitiesTheDeletedImplementationLacked:
    async def test_close_then_append_is_refused(
        self, client: httpx.AsyncClient
    ) -> None:
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"id": 1}', headers=JSON)

        closed = await client.post("/s", content=b"", headers={"stream-closed": "true"})
        assert closed.status_code in (200, 204)

        after = await client.post("/s", content=b'{"id": 2}', headers=JSON)
        assert after.status_code == 409
        assert after.headers.get("stream-closed") == "true"

    async def test_producer_fencing_rejects_a_stale_epoch(
        self, client: httpx.AsyncClient
    ) -> None:
        await client.put("/s", headers=JSON)
        fresh = {
            **JSON,
            "producer-id": "p",
            "producer-epoch": "5",
            "producer-seq": "0",
        }
        assert (
            await client.post("/s", content=b'{"id": 1}', headers=fresh)
        ).status_code in (200, 204)

        stale = {**fresh, "producer-epoch": "4"}
        refused = await client.post("/s", content=b'{"id": 2}', headers=stale)
        assert refused.status_code == 403

    async def test_producer_retry_is_a_duplicate_not_a_second_write(
        self, client: httpx.AsyncClient
    ) -> None:
        await client.put("/s", headers=JSON)
        headers = {
            **JSON,
            "producer-id": "p",
            "producer-epoch": "0",
            "producer-seq": "0",
        }
        await client.post("/s", content=b'{"id": 1}', headers=headers)
        retry = await client.post("/s", content=b'{"id": 1}', headers=headers)
        assert retry.status_code == 204

        assert (await client.get("/s")).json() == [{"id": 1}]

    async def test_content_type_mismatch_is_409(
        self, client: httpx.AsyncClient
    ) -> None:
        await client.put("/s", headers={"content-type": "text/plain"})
        mismatched = await client.post("/s", content=b"x", headers=JSON)
        assert mismatched.status_code == 409

    async def test_stream_seq_conflict_is_409(self, client: httpx.AsyncClient) -> None:
        await client.put("/s", headers=JSON)
        headers = {**JSON, "stream-seq": "5"}
        await client.post("/s", content=b'{"id": 1}', headers=headers)
        again = await client.post("/s", content=b'{"id": 2}', headers=headers)
        assert again.status_code == 409

    async def test_the_envelope_survives_a_round_trip(
        self, client: httpx.AsyncClient
    ) -> None:
        """The deleted implementation wrote `event_type` only — no metadata, no
        event_ts — so `merge_replay(order_key=ENVELOPE_TS)` could not order
        anything written through it."""
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"id": 1}', headers=JSON)

        # The server addresses streams by request path, leading slash included.
        store = DjangoStreamStore()
        messages, _ = await sync_to_async(store.read)("/s")
        assert messages[0].event_ts is not None
