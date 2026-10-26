"""The chat sample's live feed works for a visitor who never signed in.

The dashboard's SSE view needs a login as of 0.10.0 and the sample has none, so
the sample signs everyone in as a demo user. This runs the middleware list from
the sample's own settings, because the failure was invisible from the library's
tests: an anonymous request to the real stack is redirected to the login page.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from django.test import AsyncClient, override_settings

pytestmark = pytest.mark.django_db(transaction=True)

CHAT = Path(__file__).resolve().parents[2] / "examples" / "chat"
URL = "/streams/api/streams/room:1:messages/sse/"


@pytest.fixture
def chat_middleware(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    monkeypatch.syspath_prepend(str(CHAT))
    settings = importlib.import_module("chat_project.settings")
    yield list(settings.MIDDLEWARE)
    for name in [m for m in sys.modules if m.split(".")[0] in ("chat", "chat_project")]:
        del sys.modules[name]


async def test_an_anonymous_visitor_gets_the_stream_with_the_samples_middleware(
    chat_middleware: list[str],
):
    with override_settings(MIDDLEWARE=chat_middleware):
        response = await AsyncClient().get(URL)

    assert response.status_code == 200
    assert response["Content-Type"] == "text/event-stream"


async def test_without_the_demo_user_the_same_request_is_sent_to_login(
    chat_middleware: list[str],
):
    """The control: proves the test above is about the middleware."""
    without = [m for m in chat_middleware if not m.endswith("DemoUserMiddleware")]
    with override_settings(MIDDLEWARE=without):
        response = await AsyncClient().get(URL)

    assert response.status_code == 302
