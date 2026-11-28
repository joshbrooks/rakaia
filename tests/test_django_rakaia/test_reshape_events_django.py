"""`reshape_events` through the Django entry point keeps the project's settings.

Passing `options` to `get_asgi_app` replaces every `RAKAIA_*` setting, the
access check included, so the entry point takes `reshape_events` on its own.
"""

from __future__ import annotations

import json

import httpx
import pytest
from django.test import override_settings

from django_rakaia import get_asgi_app
from django_rakaia.django_store import DjangoStreamStore

pytestmark = pytest.mark.django_db(transaction=True)

JSON = {"content-type": "application/json"}


def _only_reads(_scope: object, action: str) -> int | None:
    return None if action == "read" else 403


def _upper(event: dict[str, str]) -> dict[str, str]:
    return {"name": event["name"].upper()}


async def test_reshape_events_apply_alongside_the_access_check() -> None:
    store = DjangoStreamStore()
    await store.run_sync(store.create, "/names", content_type="application/json")
    await store.run_sync(store.append, "/names", json.dumps({"name": "ana"}).encode())

    with override_settings(RAKAIA_AUTHORIZE=_only_reads):
        app = get_asgi_app(store=store, reshape_events={"/names": _upper})
        transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(
            transport=transport, base_url="http://t"
        ) as client:
            read = await client.get("/names", params={"offset": "-1"})
            write = await client.post("/names", content=b'{"name":"x"}', headers=JSON)

    assert read.json() == [{"name": "ANA"}]
    assert write.status_code == 403
