"""The recipe in docs/django-integration.md ("Protect the protocol app") works.

`authorize` below is that recipe copied verbatim, so a change to the page must be
made here too. The mount bypasses Django's auth middleware, so the session half
asks Channels' `AuthMiddlewareStack` to fill `scope["user"]` from the cookie.
"""

from __future__ import annotations

import httpx
import pytest
from channels.auth import AuthMiddlewareStack
from django.contrib.auth import BACKEND_SESSION_KEY, HASH_SESSION_KEY, SESSION_KEY
from django.contrib.auth.models import User
from django.contrib.sessions.backends.db import SessionStore
from django.test import override_settings

from django_rakaia.integration import get_asgi_app
from rakaia import StreamStore

pytestmark = pytest.mark.django_db(transaction=True)

TOKEN = "s3cret"
TEXT = {"content-type": "text/plain"}


# --- verbatim from docs/django-integration.md: `myproject/access.py` ---------
from django.conf import settings  # noqa: E402


def authorize(scope, action):
    # A token: scope["headers"] is a list of (bytes, bytes) pairs.
    token = dict(scope["headers"]).get(b"authorization")
    if token == f"Bearer {settings.PROTOCOL_TOKEN}".encode():
        return None
    # Or, for a browser session, wrap the app in Channels' AuthMiddlewareStack
    # in asgi.py (below) and read the user it puts in the scope.
    user = scope.get("user")
    if user is not None and user.is_authenticated:
        return None if action == "read" or user.is_staff else 403
    return 401


# -----------------------------------------------------------------------------


async def _cookie(**user_fields: bool) -> str:
    user = await User.objects.acreate_user(
        f"u{len(user_fields)}{user_fields.get('is_staff', 0)}",
        password="x",
        **user_fields,
    )
    session = SessionStore()
    session[SESSION_KEY] = str(user.pk)
    session[BACKEND_SESSION_KEY] = "django.contrib.auth.backends.ModelBackend"
    session[HASH_SESSION_KEY] = user.get_session_auth_hash()
    await session.acreate()
    return f"sessionid={session.session_key}"


@pytest.fixture
def client():
    with override_settings(RAKAIA_AUTHORIZE=authorize, PROTOCOL_TOKEN=TOKEN):
        app = AuthMiddlewareStack(get_asgi_app(store=StreamStore()))  # type: ignore[arg-type]
        yield httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),  # type: ignore[arg-type]
            base_url="http://test",
        )


async def test_an_anonymous_caller_is_401(client):
    async with client:
        assert (await client.put("/s", headers=TEXT)).status_code == 401


async def test_the_token_is_accepted(client):
    async with client:
        response = await client.put(
            "/s", headers={**TEXT, "authorization": f"Bearer {TOKEN}"}
        )
    assert response.status_code == 201


async def test_a_wrong_token_is_401(client):
    async with client:
        response = await client.put("/s", headers={**TEXT, "authorization": "Bearer x"})
    assert response.status_code == 401


async def test_a_staff_session_can_write_and_read(client):
    headers = {**TEXT, "cookie": await _cookie(is_staff=True)}
    async with client:
        created = await client.put("/s", headers=headers)
        read = await client.get("/s?offset=-1", headers=headers)
    assert (created.status_code, read.status_code) == (201, 200)


async def test_a_signed_in_non_staff_session_can_read_but_not_write(client):
    token = {"authorization": f"Bearer {TOKEN}"}
    plain = {"cookie": await _cookie()}
    async with client:
        await client.put("/s", headers={**TEXT, **token})
        read = await client.get("/s?offset=-1", headers=plain)
        write = await client.post("/s", content=b"x", headers={**TEXT, **plain})
    assert (read.status_code, write.status_code) == (200, 403)
