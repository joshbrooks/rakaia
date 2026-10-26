"""Sign every visitor in as one demo user.

As of 0.10.0 the dashboard's live event feed (`/streams/api/streams/<id>/sse/`)
needs a signed-in user, like every other dashboard page. The chat sample has no
login, and a sample app should not greet people with a login page, so this
attaches a fixed demo user to every request. It must sit after
`AuthenticationMiddleware`, which it replaces the user of. A real project would
sign people in properly and delete this file.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from asgiref.sync import iscoroutinefunction, markcoroutinefunction
from django.contrib.auth import get_user_model

DEMO_USERNAME = "chat-demo"


class DemoUserMiddleware:
    """Works in both modes, and must stay async-capable.

    A sync-only middleware would make Django run the async live-feed view
    through a sync wrapper, which cannot stream: it would try to consume the
    never-ending feed before sending a byte.
    """

    sync_capable = True
    async_capable = True

    def __init__(self, get_response: Callable[[Any], Any]) -> None:
        self.get_response = get_response
        if iscoroutinefunction(get_response):
            markcoroutinefunction(self)

    def __call__(self, request: Any) -> Any:
        if iscoroutinefunction(self.get_response):
            return self._async_call(request)
        user, _ = get_user_model().objects.get_or_create(username=DEMO_USERNAME)
        self._sign_in(request, user)
        return self.get_response(request)

    async def _async_call(self, request: Any) -> Any:
        user, _ = await get_user_model().objects.aget_or_create(username=DEMO_USERNAME)
        self._sign_in(request, user)
        return await self.get_response(request)

    @staticmethod
    def _sign_in(request: Any, user: Any) -> None:
        # `login_required` on an async view awaits `request.auser()`, not
        # `request.user`, so both are set.
        async def auser() -> Any:
            return user

        request.user = user
        request.auser = auser
