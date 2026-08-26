"""`get_asgi_app` — the mount point a Django project actually calls.

This entry point had **no coverage at all**, and the reason was its shape: it
resolved the store internally from `RAKAIA_STORE`, so the only way to drive it
was to reconfigure a process-wide cache and hope nothing else in the run had
already populated it. Taking an optional `store` makes it drivable, which is
the whole of what these tests need.

What is checked here is *wiring*, not protocol rules — the protocol itself is
covered against both stores in `test_protocol_server.py` and the contract
suites. The questions are narrower: does the app reach the store it was given,
does an explicit store take precedence over the setting, and are `options`
threaded through rather than dropped.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from django_rakaia.integration import get_asgi_app
from django_rakaia.store import reset_store_cache
from rakaia import ServerOptions, StreamStore

JSON = {"content-type": "application/json"}


@pytest.fixture(autouse=True)
def _clear_store_cache():
    reset_store_cache()
    yield
    reset_store_cache()


def _client(app: object) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),  # type: ignore[arg-type]
        base_url="http://test",
    )


@pytest_asyncio.fixture
async def given_store() -> AsyncIterator[StreamStore]:
    yield StreamStore()


class TestItServesTheStoreItIsGiven:
    async def test_an_append_lands_in_the_supplied_store(
        self, given_store: StreamStore
    ) -> None:
        """The round trip is the point: a store passed in is the one written to."""
        async with _client(get_asgi_app(store=given_store)) as client:
            assert (await client.put("/s", headers=JSON)).status_code in (200, 201, 204)
            posted = await client.post("/s", content=b'{"id": 1}', headers=JSON)
            assert posted.status_code in (200, 204)

            assert (await client.get("/s")).json() == [{"id": 1}]

        # ...and observably in *that* object, not some other store the app
        # resolved for itself. Streams key on the full request path.
        messages, _ = given_store.read("/s")
        assert [json.loads(m.data) for m in messages] == [{"id": 1}]

    async def test_two_apps_with_two_stores_do_not_share(self) -> None:
        """The failure this guards is the old shape: both apps resolving the
        same process-wide singleton, so one test's appends leak into the next."""
        a, b = StreamStore(), StreamStore()

        async with _client(get_asgi_app(store=a)) as client:
            await client.put("/s", headers=JSON)
            await client.post("/s", content=b'{"from": "a"}', headers=JSON)

        async with _client(get_asgi_app(store=b)) as client:
            await client.put("/s", headers=JSON)
            assert (await client.get("/s")).json() == []


class TestAnExplicitStoreBeatsTheSetting:
    def test_the_setting_is_not_consulted_at_all(self, settings) -> None:
        """`RAKAIA_STORE` here is a value `get_store()` refuses outright, so if
        the setting were still read this would raise instead of returning."""
        settings.RAKAIA_STORE = "durrable"

        assert get_asgi_app(store=StreamStore()) is not None

    def test_without_a_store_the_setting_still_decides(self, settings) -> None:
        from django.core.exceptions import ImproperlyConfigured

        settings.RAKAIA_STORE = "durrable"

        with pytest.raises(ImproperlyConfigured):
            get_asgi_app()


class TestOptionsAreThreadedThrough:
    async def test_a_supplied_option_reaches_the_app(
        self, given_store: StreamStore
    ) -> None:
        """A long poll at the tail waits out the server's window, so a short one
        is observable: on the 3.0s default this would exceed the timeout below.
        """
        options = ServerOptions()
        options.long_poll_timeout = 0.05

        app = get_asgi_app(options=options, store=given_store)
        async with _client(app) as client:
            await client.put("/s", headers=JSON)
            read = await client.get("/s?offset=now&live=long-poll", timeout=1.0)

        assert read.status_code in (200, 204)
