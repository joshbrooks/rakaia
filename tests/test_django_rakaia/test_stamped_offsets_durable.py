"""Stamped offsets over the durable store: the arrangement Partisipa serves.

Its offsets read ``<generation>.<app stamp>_<20-digit position>``, the format the
translations stream's catch-up read already issues, so a device can move from
that read to the live one with the offset it holds.
"""

from __future__ import annotations

import httpx
import pytest

from django_rakaia.django_store import DjangoStreamStore
from rakaia import StampedOffsetStore
from tests.asgi_client import asgi_client

pytestmark = pytest.mark.django_db(transaction=True)

JSON = {"content-type": "application/json"}


async def _first_offset(client: httpx.AsyncClient) -> str:
    await client.put("/s", headers=JSON)
    await client.post("/s", content=b'{"n":1}', headers=JSON)
    return (await client.get("/s", params={"offset": "-1"})).headers[
        "stream-next-offset"
    ]


async def test_an_offset_read_before_a_rebuild_answers_410() -> None:
    inner = DjangoStreamStore()
    async with asgi_client(StampedOffsetStore(inner, stamp=lambda _: "t1")) as client:
        offset = await _first_offset(client)
        generation = await inner.run_sync(inner.get_generation, "/s")
        head = await inner.run_sync(inner.get_current_offset, "/s")
        assert offset == f"{generation}.t1_{head}"

        await client.post("/s", content=b'{"n":2}', headers=JSON)
        resumed = await client.get("/s", params={"offset": offset})
        await client.delete("/s")
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"n":1}', headers=JSON)
        stale = await client.get("/s", params={"offset": offset})

    assert resumed.status_code == 200
    assert resumed.json() == [{"n": 2}]
    assert stale.status_code == 410


def _deny_everything(_scope: object, _action: str) -> int:
    return 403


async def test_wrapped_through_get_asgi_app_the_access_check_still_applies() -> None:
    """The wiring the guide shows: `get_asgi_app` applies `RAKAIA_AUTHORIZE`."""
    from django.test import override_settings

    from django_rakaia import get_asgi_app, get_store

    with override_settings(RAKAIA_AUTHORIZE=_deny_everything):
        app = get_asgi_app(store=StampedOffsetStore(get_store(), stamp=lambda _: "t1"))
        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(
            transport=transport, base_url="http://t"
        ) as client:
            created = await client.put("/s", headers=JSON)

    assert created.status_code == 403
