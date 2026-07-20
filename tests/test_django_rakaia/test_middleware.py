"""Tests for django_rakaia.middleware.ProvenanceMiddleware."""

from __future__ import annotations

from types import SimpleNamespace

from django_rakaia.middleware import ProvenanceMiddleware
from rakaia.context import get_provenance


def _run(method: str, user):
    captured: dict = {}

    def get_response(request):  # noqa: ARG001
        captured.update(get_provenance())  # what an append would see mid-request
        return "ok"

    request = SimpleNamespace(method=method, path="/api/x", user=user)
    result = ProvenanceMiddleware(get_response)(request)
    return result, captured


class TestProvenanceMiddleware:
    def test_mutating_request_stamps_user_and_url(self):
        user = SimpleNamespace(pk=42, is_authenticated=True)
        result, captured = _run("POST", user)
        assert result == "ok"
        assert captured == {"user": 42, "url": "/api/x"}
        assert get_provenance() == {}  # cleared after the request

    def test_get_request_is_not_stamped(self):
        user = SimpleNamespace(pk=42, is_authenticated=True)
        _, captured = _run("GET", user)
        assert captured == {}  # reads add no provenance

    def test_anonymous_user_stamps_null_actor(self):
        user = SimpleNamespace(pk=None, is_authenticated=False)
        _, captured = _run("POST", user)
        assert captured == {"user": None, "url": "/api/x"}
