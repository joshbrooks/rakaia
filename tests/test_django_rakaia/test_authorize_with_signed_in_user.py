"""The `request.user` recipe in docs/django-integration.md works as written.

The protocol app bypasses Django's auth middleware, so the documented way to
ask who is calling is Channels' `AuthMiddlewareStack`, which fills
`scope["user"]` from the session cookie before the app runs.
"""

from __future__ import annotations

import httpx
import pytest
from channels.auth import AuthMiddlewareStack
from django.contrib.auth import BACKEND_SESSION_KEY, HASH_SESSION_KEY, SESSION_KEY
from django.contrib.auth.models import User
from django.contrib.sessions.backends.db import SessionStore

from django_rakaia.integration import get_asgi_app
from rakaia import StreamStore

pytestmark = pytest.mark.django_db(transaction=True)


def signed_in_only(scope, action):  # noqa: ARG001 - the hook's fixed signature
    user = scope.get("user")
    return None if user is not None and user.is_authenticated else 401


async def _session_cookie(user: User) -> str:
    session = SessionStore()
    session[SESSION_KEY] = str(user.pk)
    session[BACKEND_SESSION_KEY] = "django.contrib.auth.backends.ModelBackend"
    session[HASH_SESSION_KEY] = user.get_session_auth_hash()
    await session.acreate()
    assert session.session_key is not None
    return f"sessionid={session.session_key}"


async def test_the_hook_sees_the_signed_in_user():
    from django.test import override_settings

    with override_settings(RAKAIA_AUTHORIZE=signed_in_only):
        app = AuthMiddlewareStack(get_asgi_app(store=StreamStore()))  # type: ignore[arg-type]
        user = await User.objects.acreate_user("ida", password="x")
        cookie = await _session_cookie(user)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),  # type: ignore[arg-type]
            base_url="http://test",
        ) as client:
            anonymous = await client.put("/s", headers={"content-type": "text/plain"})
            signed_in = await client.put(
                "/s", headers={"content-type": "text/plain", "cookie": cookie}
            )

    assert (anonymous.status_code, signed_in.status_code) == (401, 201)
