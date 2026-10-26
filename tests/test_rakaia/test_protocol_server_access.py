"""Body size limit, authorisation hook and allowed origins on the protocol server."""

from __future__ import annotations

import pytest

from rakaia import StreamStore
from rakaia.protocol_server import DEFAULT_MAX_BODY_BYTES, ServerOptions
from tests.asgi_client import asgi_client

JSON = {"content-type": "application/json"}


class TestBodyLimit:
    async def test_an_append_over_the_limit_is_413(self):
        async with asgi_client(StreamStore(), max_body_bytes=100) as c:
            await c.put("/s", headers=JSON)
            response = await c.post(
                "/s", content=b'"' + b"x" * 200 + b'"', headers=JSON
            )
        assert response.status_code == 413

    async def test_a_create_over_the_limit_is_413(self):
        async with asgi_client(StreamStore(), max_body_bytes=100) as c:
            response = await c.put("/s", content=b'"' + b"x" * 200 + b'"', headers=JSON)
        assert response.status_code == 413

    async def test_a_body_under_the_limit_still_works(self):
        async with asgi_client(StreamStore(), max_body_bytes=100) as c:
            await c.put("/s", headers=JSON)
            response = await c.post("/s", content=b'"ok"', headers=JSON)
        assert response.status_code == 204

    async def test_a_body_of_exactly_the_limit_is_accepted(self):
        body = b'"' + b"x" * 8 + b'"'
        async with asgi_client(StreamStore(), max_body_bytes=len(body)) as c:
            await c.put("/s", headers=JSON)
            response = await c.post("/s", content=body, headers=JSON)
        assert response.status_code == 204

    async def test_reading_stops_at_the_limit_instead_of_taking_the_whole_body(self):
        """The server refuses on the chunk that crosses the line, not at the end."""
        chunks_read = 0
        app = _app(max_body_bytes=10)

        async def receive():
            nonlocal chunks_read
            chunks_read += 1
            assert chunks_read < 50, "server kept reading past the limit"
            return {"type": "http.request", "body": b"x" * 8, "more_body": True}

        sent: list[dict] = []

        async def send(message):
            sent.append(message)

        scope = {
            "type": "http",
            "method": "PUT",
            "path": "/s",
            "headers": [(b"content-type", b"application/octet-stream")],
        }
        await app(scope, receive, send)
        assert sent[0]["status"] == 413
        assert chunks_read == 2

    def test_the_default_is_8_mib(self):
        assert ServerOptions().max_body_bytes == DEFAULT_MAX_BODY_BYTES == 8 * 1024**2

    def test_a_negative_limit_is_refused_at_construction(self):
        with pytest.raises(ValueError, match="max_body_bytes"):
            ServerOptions(max_body_bytes=-1)


def _app(**overrides):
    from rakaia import create_app

    options = ServerOptions()
    for name, value in overrides.items():
        setattr(options, name, value)
    return create_app(store=StreamStore(), options=options)


class TestAuthorizeHook:
    async def test_the_hook_sees_the_scope_and_the_action_for_each_method(self):
        seen: list[tuple[str, str, str]] = []

        def authorize(scope, action):
            seen.append((scope["method"], scope["path"], action))

        async with asgi_client(StreamStore(), authorize=authorize) as c:
            await c.put("/s", headers=JSON)
            await c.post("/s", content=b'"a"', headers=JSON)
            await c.get("/s?offset=-1")
            await c.head("/s")
            await c.delete("/s")

        assert seen == [
            ("PUT", "/s", "create"),
            ("POST", "/s", "append"),
            ("GET", "/s", "read"),
            ("HEAD", "/s", "read"),
            ("DELETE", "/s", "delete"),
        ]

    async def test_false_is_403_and_the_store_is_untouched(self):
        store = StreamStore()
        async with asgi_client(store, authorize=lambda *_: False) as c:
            response = await c.put("/s", headers=JSON)
        assert response.status_code == 403
        assert not store.has("/s")

    async def test_a_status_or_a_status_and_body_is_sent_as_given(self):
        async with asgi_client(StreamStore(), authorize=lambda *_: 401) as c:
            assert (await c.get("/s?offset=-1")).status_code == 401
        async with asgi_client(
            StreamStore(), authorize=lambda *_: (401, b"log in")
        ) as c:
            response = await c.get("/s?offset=-1")
        assert (response.status_code, response.content) == (401, b"log in")

    @pytest.mark.parametrize("verdict", [1, 0, 200, 204, 302, 600, -5, (200, b"x")])
    async def test_a_status_outside_400_to_599_is_a_malformed_verdict(self, verdict):
        """`return 200` from a hook must not let the request through as a success."""
        store = StreamStore()
        async with asgi_client(store, authorize=lambda *_: verdict) as c:
            response = await c.put("/s", headers=JSON)
        assert response.status_code == 403
        assert not store.has("/s")

    async def test_a_coroutine_hook_is_awaited(self):
        async def authorize(scope, action):  # noqa: ARG001
            return None if action == "read" else False

        async with asgi_client(StreamStore(), authorize=authorize) as c:
            assert (await c.put("/s", headers=JSON)).status_code == 403

    async def test_a_hook_that_returns_nonsense_fails_closed(self):
        async with asgi_client(StreamStore(), authorize=lambda *_: "yes") as c:
            assert (await c.put("/s", headers=JSON)).status_code == 403

    async def test_a_token_check_reads_the_headers(self):
        def authorize(scope, action):  # noqa: ARG001
            headers = dict(scope["headers"])
            return None if headers.get(b"authorization") == b"Bearer s3" else 401

        async with asgi_client(StreamStore(), authorize=authorize) as c:
            assert (await c.put("/s", headers=JSON)).status_code == 401
            ok = await c.put("/s", headers={**JSON, "authorization": "Bearer s3"})
        assert ok.status_code == 201

    async def test_cors_preflight_is_not_sent_to_the_hook(self):
        async with asgi_client(StreamStore(), authorize=lambda *_: False) as c:
            response = await c.options("/s")
        assert response.status_code == 204

    async def test_a_refusal_carries_the_cors_headers(self):
        async with asgi_client(StreamStore(), authorize=lambda *_: False) as c:
            response = await c.get("/s?offset=-1")
        assert response.headers["access-control-allow-origin"] == "*"

    async def test_with_no_hook_everything_is_allowed(self):
        async with asgi_client(StreamStore()) as c:
            assert (await c.put("/s", headers=JSON)).status_code == 201


class TestAllowedOrigins:
    async def test_the_default_allows_every_origin(self):
        async with asgi_client(StreamStore()) as c:
            response = await c.get("/s?offset=-1", headers={"origin": "https://x.test"})
        assert response.headers["access-control-allow-origin"] == "*"

    async def test_a_listed_origin_is_echoed_with_vary(self):
        async with asgi_client(
            StreamStore(), allowed_origins=("https://app.test",)
        ) as c:
            response = await c.options("/s", headers={"origin": "https://app.test"})
        assert response.headers["access-control-allow-origin"] == "https://app.test"
        assert response.headers["vary"] == "Origin"

    async def test_an_unlisted_origin_gets_no_allow_header(self):
        async with asgi_client(
            StreamStore(), allowed_origins=("https://app.test",)
        ) as c:
            response = await c.options("/s", headers={"origin": "https://evil.test"})
        assert "access-control-allow-origin" not in response.headers

    async def test_a_request_without_an_origin_gets_no_allow_header(self):
        async with asgi_client(
            StreamStore(), allowed_origins=("https://app.test",)
        ) as c:
            response = await c.options("/s")
        assert "access-control-allow-origin" not in response.headers

    def test_one_string_instead_of_a_list_is_refused(self):
        with pytest.raises(ValueError, match="allowed_origins"):
            ServerOptions(allowed_origins="https://app.test")  # type: ignore[arg-type]
