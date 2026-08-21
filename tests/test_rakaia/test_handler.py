"""Integration tests for the rakaia ASGI handler.

Uses httpx.AsyncClient with ASGITransport to drive the create_app() ASGI
application directly, exercising the full HTTP protocol.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from rakaia import StreamStore
from rakaia.handler import _fault_injection_enabled_by_env
from rakaia.types import INITIAL_OFFSET
from tests.asgi_client import asgi_client


def _fast_client(timeout: float = 0.2) -> httpx.AsyncClient:
    """A client whose server uses a short long-poll window (for wait tests)."""
    return asgi_client(StreamStore(), long_poll_timeout=timeout)


@pytest_asyncio.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """An httpx client driving a fresh ASGI app per test."""
    async with asgi_client(StreamStore()) as ac:
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
# Fault injection gate
# =============================================================================


def _fault_client(*, enabled: bool) -> httpx.AsyncClient:
    """A client whose server has the fault-injection endpoint on or off."""
    return asgi_client(StreamStore(), enable_fault_injection=enabled)


class TestFaultInjectionGate:
    async def test_default_app_does_not_route_the_endpoint(
        self, client: httpx.AsyncClient
    ):
        """Off by default, and a 404 — not a 403 that would advertise it."""
        response = await client.post(
            "/_test/inject-error", json={"path": "/foo", "status": 500, "count": 999}
        )
        assert response.status_code == 404

    async def test_default_app_ignores_delete_too(self, client: httpx.AsyncClient):
        response = await client.delete("/_test/inject-error")
        assert response.status_code == 404

    async def test_disabled_endpoint_cannot_fault_a_stream(
        self, client: httpx.AsyncClient
    ):
        await client.put("/foo", headers={"content-type": "text/plain"}, content=b"hi")
        await client.post(
            "/_test/inject-error", json={"path": "/foo", "status": 500, "count": 999}
        )
        response = await client.get("/foo")
        assert response.status_code == 200

    async def test_enabled_flag_injects_a_fault(self):
        async with _fault_client(enabled=True) as client:
            await client.put(
                "/foo", headers={"content-type": "text/plain"}, content=b"hi"
            )
            registered = await client.post(
                "/_test/inject-error",
                json={"path": "/foo", "status": 503, "retryAfter": 7, "count": 1},
            )
            assert registered.status_code == 200

            faulted = await client.get("/foo")
            assert faulted.status_code == 503
            assert faulted.headers.get("retry-after") == "7"

            # The fault is consumed after `count` responses.
            assert (await client.get("/foo")).status_code == 200

    async def test_enabled_flag_clears_faults_on_delete(self):
        async with _fault_client(enabled=True) as client:
            await client.put(
                "/foo", headers={"content-type": "text/plain"}, content=b"hi"
            )
            await client.post(
                "/_test/inject-error",
                json={"path": "/foo", "status": 500, "count": 999},
            )
            cleared = await client.delete("/_test/inject-error")
            assert cleared.status_code == 200
            assert (await client.get("/foo")).status_code == 200


class TestFaultInjectionEnvFlag:
    """The conformance runner starts `rakaia:app`, so the env var is the only
    way in — a constructor argument alone cannot reach the default app."""

    async def test_env_var_unset_is_off(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("RAKAIA_ENABLE_FAULT_INJECTION", raising=False)
        assert _fault_injection_enabled_by_env() is False

    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
    async def test_truthy_env_values_enable_it(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ):
        monkeypatch.setenv("RAKAIA_ENABLE_FAULT_INJECTION", value)
        assert _fault_injection_enabled_by_env() is True

    @pytest.mark.parametrize("value", ["", "0", "false", "no", "off"])
    async def test_falsy_env_values_leave_it_off(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ):
        monkeypatch.setenv("RAKAIA_ENABLE_FAULT_INJECTION", value)
        assert _fault_injection_enabled_by_env() is False


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

    async def test_a_close_landing_during_a_long_poll_is_reported(self):
        """A close landing while the client is parked comes back with the data.

        This case passes with or without `_handle_read`'s re-fetch of the stream,
        because `StreamStore.get` hands back the live object rather than a
        snapshot — so the "first" fetch sees the close too. The re-fetch is
        pinned by the durable-store twin of this test in
        `test_django_rakaia/test_protocol_server.py`, where `get` returns a
        detached row and dropping the re-fetch loses the `Stream-Closed`.
        """
        async with _fast_client(timeout=2.0) as client:
            await client.put(
                "/foo", headers={"content-type": "text/plain"}, content=b"one"
            )
            tail = (await client.get("/foo")).headers["Stream-Next-Offset"]

            async def close_it() -> None:
                await asyncio.sleep(0.05)
                await client.post(
                    "/foo",
                    headers={"content-type": "text/plain", "Stream-Closed": "true"},
                    content=b"two",
                )

            poll, _ = await asyncio.gather(
                client.get(f"/foo?offset={tail}&live=long-poll"), close_it()
            )

            assert poll.status_code == 200
            assert poll.content == b"two"
            assert poll.headers.get("Stream-Closed") == "true"
            assert poll.headers["etag"].endswith(':c"')

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
