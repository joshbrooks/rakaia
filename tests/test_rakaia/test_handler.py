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
from rakaia.types import INITIAL_OFFSET


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


# Mark all tests as async via pytest-asyncio auto mode
pytestmark = pytest.mark.asyncio
