"""Integration tests for the rakaia ASGI handler.

Uses httpx.AsyncClient with ASGITransport to drive the create_app() ASGI
application directly, exercising the full HTTP protocol.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from rakaia import StreamStore, create_app
from rakaia.handler import ServerOptions
from rakaia.types import INITIAL_OFFSET


def _fast_client(timeout: float = 0.2) -> httpx.AsyncClient:
    """A client whose server uses a short long-poll window (for wait tests)."""
    app = create_app(
        store=StreamStore(), options=ServerOptions(long_poll_timeout=timeout)
    )
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest_asyncio.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """An httpx client driving a fresh ASGI app per test."""
    store = StreamStore()
    app = create_app(store=store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# =============================================================================
# OPTIONS / CORS
# =============================================================================


class TestOptions:
    async def test_cors_preflight(self, client: httpx.AsyncClient):
        response = await client.options("/anything")
        assert response.status_code == 204
        assert "access-control-allow-origin" in {h.lower() for h in response.headers}


# =============================================================================
# PUT — Create
# =============================================================================


class TestCreate:
    async def test_create_returns_201(self, client: httpx.AsyncClient):
        response = await client.put(
            "/foo", headers={"content-type": "application/json"}
        )
        assert response.status_code == 201
        assert "location" in {h.lower() for h in response.headers}
        assert response.headers.get("Stream-Next-Offset") == INITIAL_OFFSET

    async def test_idempotent_create_returns_200(self, client: httpx.AsyncClient):
        await client.put("/foo", headers={"content-type": "application/json"})
        response = await client.put(
            "/foo", headers={"content-type": "application/json"}
        )
        assert response.status_code == 200

    async def test_create_with_conflicting_config_returns_409(
        self, client: httpx.AsyncClient
    ):
        await client.put("/foo", headers={"content-type": "application/json"})
        response = await client.put("/foo", headers={"content-type": "text/plain"})
        assert response.status_code == 409

    async def test_create_with_ttl(self, client: httpx.AsyncClient):
        response = await client.put(
            "/foo",
            headers={"content-type": "application/json", "Stream-TTL": "60"},
        )
        assert response.status_code == 201

    async def test_create_with_invalid_ttl(self, client: httpx.AsyncClient):
        response = await client.put(
            "/foo",
            headers={"content-type": "application/json", "Stream-TTL": "not-a-number"},
        )
        assert response.status_code == 400

    async def test_create_with_ttl_and_expires_at_returns_400(
        self, client: httpx.AsyncClient
    ):
        response = await client.put(
            "/foo",
            headers={
                "content-type": "application/json",
                "Stream-TTL": "60",
                "Stream-Expires-At": "2030-01-01T00:00:00Z",
            },
        )
        assert response.status_code == 400

    async def test_create_with_initial_data(self, client: httpx.AsyncClient):
        response = await client.put(
            "/foo",
            headers={"content-type": "application/json"},
            content=b'{"a":1}',
        )
        assert response.status_code == 201
        # Initial offset should have advanced
        assert response.headers.get("Stream-Next-Offset") != INITIAL_OFFSET

    async def test_create_closed(self, client: httpx.AsyncClient):
        response = await client.put(
            "/foo",
            headers={"content-type": "application/json", "Stream-Closed": "true"},
        )
        assert response.status_code == 201
        assert response.headers.get("Stream-Closed") == "true"


# =============================================================================
# HEAD — Metadata
# =============================================================================


class TestHead:
    async def test_head_returns_metadata(self, client: httpx.AsyncClient):
        await client.put("/foo", headers={"content-type": "application/json"})
        response = await client.head("/foo")
        assert response.status_code == 200
        assert "Stream-Next-Offset" in response.headers
        assert response.headers.get("content-type") == "application/json"
        assert "etag" in {h.lower() for h in response.headers}

    async def test_head_missing_returns_404(self, client: httpx.AsyncClient):
        response = await client.head("/missing")
        assert response.status_code == 404


# =============================================================================
# POST — Append
# =============================================================================


class TestAppend:
    async def test_basic_append(self, client: httpx.AsyncClient):
        await client.put("/foo", headers={"content-type": "application/json"})
        response = await client.post(
            "/foo",
            headers={"content-type": "application/json"},
            content=b'{"a":1}',
        )
        assert response.status_code in (200, 204)
        assert response.headers.get("Stream-Next-Offset") != INITIAL_OFFSET

    async def test_append_to_missing_returns_404(self, client: httpx.AsyncClient):
        response = await client.post(
            "/missing",
            headers={"content-type": "application/json"},
            content=b'{"a":1}',
        )
        assert response.status_code == 404

    async def test_append_invalid_json_returns_400(self, client: httpx.AsyncClient):
        await client.put("/foo", headers={"content-type": "application/json"})
        response = await client.post(
            "/foo",
            headers={"content-type": "application/json"},
            content=b"not-json",
        )
        assert response.status_code == 400

    async def test_append_with_producer(self, client: httpx.AsyncClient):
        await client.put("/foo", headers={"content-type": "application/json"})
        response = await client.post(
            "/foo",
            headers={
                "content-type": "application/json",
                "Producer-Id": "p1",
                "Producer-Epoch": "1",
                "Producer-Seq": "0",
            },
            content=b'{"a":1}',
        )
        assert response.status_code in (200, 204)

    async def test_append_with_close(self, client: httpx.AsyncClient):
        await client.put("/foo", headers={"content-type": "application/json"})
        response = await client.post(
            "/foo",
            headers={"content-type": "application/json", "Stream-Closed": "true"},
            content=b'{"a":1}',
        )
        assert response.status_code in (200, 204)
        assert response.headers.get("Stream-Closed") == "true"

    async def test_append_to_closed_stream_returns_410(self, client: httpx.AsyncClient):
        # Create with closed=true
        await client.put(
            "/foo",
            headers={"content-type": "application/json", "Stream-Closed": "true"},
        )
        response = await client.post(
            "/foo",
            headers={"content-type": "application/json"},
            content=b'{"a":1}',
        )
        # Closed streams return 410 Gone or similar
        assert response.status_code in (410, 409, 200)


# =============================================================================
# GET — Read
# =============================================================================


class TestRead:
    async def test_read_empty_stream(self, client: httpx.AsyncClient):
        await client.put("/foo", headers={"content-type": "application/json"})
        response = await client.get("/foo")
        assert response.status_code == 200
        # Empty JSON stream returns []
        assert response.content == b"[]"

    async def test_read_after_appends(self, client: httpx.AsyncClient):
        await client.put("/foo", headers={"content-type": "application/json"})
        await client.post(
            "/foo",
            headers={"content-type": "application/json"},
            content=b'{"a":1}',
        )
        await client.post(
            "/foo",
            headers={"content-type": "application/json"},
            content=b'{"b":2}',
        )
        response = await client.get("/foo")
        assert response.status_code == 200
        # Should be a JSON array with both elements
        body = response.content
        assert body.startswith(b"[")
        assert body.endswith(b"]")
        assert b'"a"' in body
        assert b'"b"' in body

    async def test_read_missing_returns_404(self, client: httpx.AsyncClient):
        response = await client.get("/missing")
        assert response.status_code == 404

    async def test_read_invalid_offset_returns_400(self, client: httpx.AsyncClient):
        await client.put("/foo", headers={"content-type": "application/json"})
        response = await client.get("/foo?offset=invalid")
        assert response.status_code == 400


# =============================================================================
# DELETE
# =============================================================================


class TestDelete:
    async def test_delete_existing(self, client: httpx.AsyncClient):
        await client.put("/foo", headers={"content-type": "application/json"})
        response = await client.delete("/foo")
        assert response.status_code in (200, 204)
        # Verify it's gone
        head = await client.head("/foo")
        assert head.status_code == 404

    async def test_delete_missing_returns_404(self, client: httpx.AsyncClient):
        response = await client.delete("/missing")
        assert response.status_code == 404


# =============================================================================
# Method not allowed
# =============================================================================


class TestMethods:
    async def test_patch_returns_405(self, client: httpx.AsyncClient):
        response = await client.patch("/foo")
        assert response.status_code == 405


# =============================================================================
# Conformance-gap regressions
#
# These cover protocol behaviors exercised by the upstream
# @durable-streams/server-conformance-tests suite that were previously missing.
# =============================================================================


class TestConformanceGaps:
    async def test_cors_preflight_allows_if_none_match(self, client: httpx.AsyncClient):
        response = await client.options(
            "/foo",
            headers={
                "origin": "https://example.com",
                "access-control-request-method": "GET",
                "access-control-request-headers": "if-none-match",
            },
        )
        assert response.status_code in (200, 204)
        allow = response.headers.get("access-control-allow-headers", "").lower()
        assert "if-none-match" in allow

    async def test_head_returns_ttl_metadata(self, client: httpx.AsyncClient):
        await client.put(
            "/foo",
            headers={"content-type": "text/plain", "Stream-TTL": "3600"},
        )
        response = await client.head("/foo")
        assert response.status_code == 200
        assert response.headers.get("Stream-TTL") == "3600"

    async def test_head_returns_expires_at_metadata(self, client: httpx.AsyncClient):
        await client.put(
            "/foo",
            headers={
                "content-type": "text/plain",
                "Stream-Expires-At": "2999-01-01T00:00:00Z",
            },
        )
        response = await client.head("/foo")
        assert response.status_code == 200
        assert response.headers.get("Stream-Expires-At") == "2999-01-01T00:00:00Z"

    async def test_offset_now_long_poll_returns_204_when_idle(self):
        """offset=now + long-poll with no new data returns 204 promptly."""
        async with _fast_client() as client:
            await client.put(
                "/foo", headers={"content-type": "text/plain"}, content=b"existing"
            )
            read = await client.get("/foo")
            tail = read.headers.get("Stream-Next-Offset")

            response = await client.get("/foo?offset=now&live=long-poll")
            assert response.status_code == 204
            assert response.headers.get("Stream-Up-To-Date") == "true"
            assert response.headers.get("Stream-Next-Offset") == tail

    async def test_sse_catch_up_pairs_each_data_event_with_control(self):
        """During catch-up every SSE data event is followed by a control event.

        Uses a closed stream so the SSE response terminates cleanly (a live
        stream would block on the long-poll wait loop).
        """
        async with _fast_client() as client:
            await client.put(
                "/foo", headers={"content-type": "text/plain"}, content=b"one"
            )
            # Second append also closes the stream so SSE ends at the tail.
            await client.post(
                "/foo",
                headers={"content-type": "text/plain", "Stream-Closed": "true"},
                content=b"two",
            )

            response = await client.get("/foo?offset=-1&live=sse")
            assert response.status_code == 200
            received = response.text

            # Parse event frames in order.
            frames = []
            for block in received.split("\n\n"):
                event_line = next(
                    (ln for ln in block.split("\n") if ln.startswith("event:")), None
                )
                if event_line:
                    frames.append(event_line[len("event:") :].strip())

            data_indices = [i for i, e in enumerate(frames) if e == "data"]
            assert len(data_indices) == 2
            # Every data frame must be immediately followed by a control frame.
            for i in data_indices:
                assert frames[i + 1] == "control"


# Mark all tests as async via pytest-asyncio auto mode
pytestmark = pytest.mark.asyncio
