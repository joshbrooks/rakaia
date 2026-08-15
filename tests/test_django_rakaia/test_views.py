"""Tests for the Django dashboard views (``django_rakaia.views``).

Covers the login gate, the HTML dashboard pages (rendered via app templates),
and the JSON API endpoints: happy paths, empty state, 404/absent streams, and
input validation on pagination/offset parameters.
"""

import pytest
from django.contrib.auth import get_user_model
from django.test import Client
from django.urls import reverse

from django_rakaia.models import Stream, StreamEntry, StreamEvent

pytestmark = pytest.mark.django_db


@pytest.fixture
def client() -> Client:
    return Client()


@pytest.fixture
def user():
    return get_user_model().objects.create_user(username="dash", password="pw")


@pytest.fixture
def auth_client(client: Client, user) -> Client:
    client.force_login(user)
    # The test app emits a stream event on every ``auth.User`` save (create +
    # the last_login update from force_login), so start each test from a clean
    # slate rather than counting those incidental rows.
    StreamEntry.objects.all().delete()
    StreamEvent.objects.all().delete()
    Stream.objects.all().delete()
    return client


def _make_entry(stream_id: str, offset: int, event_type: str = "create", **data):
    """Create a Stream + StreamEvent + StreamEntry triple at ``offset``."""
    stream, _ = Stream.objects.get_or_create(stream_id=stream_id)
    event = StreamEvent.objects.create(event_type=event_type, data=data or {"k": "v"})
    StreamEntry.objects.create(stream=stream, event=event, offset=offset)
    return stream, event


# ---------------------------------------------------------------------------
# Auth gate
# ---------------------------------------------------------------------------


class TestAuthGate:
    def test_streams_index_redirects_when_anonymous(self, client: Client) -> None:
        resp = client.get(reverse("django_rakaia:streams_index"))
        assert resp.status_code == 302
        assert "/accounts/login" in resp["Location"]

    def test_streams_api_redirects_when_anonymous(self, client: Client) -> None:
        resp = client.get(reverse("django_rakaia:streams_api"))
        assert resp.status_code == 302


# ---------------------------------------------------------------------------
# HTML dashboard pages
# ---------------------------------------------------------------------------


class TestStreamsIndex:
    def test_empty(self, auth_client: Client) -> None:
        resp = auth_client.get(reverse("django_rakaia:streams_index"))
        assert resp.status_code == 200
        assert resp.context["total_streams"] == 0
        assert resp.context["total_events"] == 0
        assert resp.context["streams"] == []

    def test_with_data(self, auth_client: Client) -> None:
        _make_entry("s:1", 1)
        _make_entry("s:1", 2)
        _make_entry("s:2", 1)
        resp = auth_client.get(reverse("django_rakaia:streams_index"))
        assert resp.status_code == 200
        # Two streams have entries; three events total.
        assert resp.context["total_streams"] == 2
        assert resp.context["total_events"] == 3
        counts = {s["stream_id"]: s["event_count"] for s in resp.context["streams"]}
        assert counts == {"s:1": 2, "s:2": 1}


class TestStreamDetail:
    def test_absent_stream_renders_exists_false(self, auth_client: Client) -> None:
        resp = auth_client.get(reverse("django_rakaia:stream_detail", args=["nope"]))
        assert resp.status_code == 200
        assert resp.context["exists"] is False

    def test_existing_stream_with_events(self, auth_client: Client) -> None:
        _make_entry("s:1", 1)
        _make_entry("s:1", 5)
        resp = auth_client.get(reverse("django_rakaia:stream_detail", args=["s:1"]))
        assert resp.status_code == 200
        assert resp.context["exists"] is True
        assert resp.context["stats"]["event_count"] == 2
        assert resp.context["stats"]["min_offset"] == 1
        assert resp.context["stats"]["max_offset"] == 5

    def test_existing_stream_no_events(self, auth_client: Client) -> None:
        Stream.objects.create(stream_id="empty:1")
        resp = auth_client.get(reverse("django_rakaia:stream_detail", args=["empty:1"]))
        assert resp.status_code == 200
        assert resp.context["exists"] is True
        assert resp.context["stats"]["event_count"] == 0


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------


class TestStreamsApi:
    def test_empty(self, auth_client: Client) -> None:
        resp = auth_client.get(reverse("django_rakaia:streams_api"))
        assert resp.status_code == 200
        body = resp.json()
        assert body == {"streams": [], "total": 0}

    def test_serialization(self, auth_client: Client) -> None:
        _make_entry("s:1", 1)
        _make_entry("s:1", 2)
        resp = auth_client.get(reverse("django_rakaia:streams_api"))
        body = resp.json()
        assert body["total"] == 1
        (stream,) = body["streams"]
        assert stream["stream_id"] == "s:1"
        assert stream["event_count"] == 2
        assert stream["min_offset"] == 1
        assert stream["max_offset"] == 2
        assert stream["last_event"] is not None


class TestStreamEventsApi:
    def test_absent_stream_returns_empty(self, auth_client: Client) -> None:
        resp = auth_client.get(
            reverse("django_rakaia:stream_events_api", args=["nope"])
        )
        assert resp.status_code == 200
        assert resp.json() == {"stream_id": "nope", "events": [], "count": 0}

    def test_returns_events_ordered_by_offset(self, auth_client: Client) -> None:
        _make_entry("s:1", 2, event_type="update", value=2)
        _make_entry("s:1", 1, event_type="create", value=1)
        resp = auth_client.get(reverse("django_rakaia:stream_events_api", args=["s:1"]))
        body = resp.json()
        assert body["count"] == 2
        assert [e["offset"] for e in body["events"]] == [1, 2]
        assert body["events"][0]["event_type"] == "create"

    def test_after_offset_filter(self, auth_client: Client) -> None:
        for off in (1, 2, 3):
            _make_entry("s:1", off)
        resp = auth_client.get(
            reverse("django_rakaia:stream_events_api", args=["s:1"]),
            {"after_offset": 1},
        )
        body = resp.json()
        assert [e["offset"] for e in body["events"]] == [2, 3]

    def test_limit_clamped_and_has_more(self, auth_client: Client) -> None:
        for off in range(1, 6):
            _make_entry("s:1", off)
        resp = auth_client.get(
            reverse("django_rakaia:stream_events_api", args=["s:1"]),
            {"limit": 2},
        )
        body = resp.json()
        assert body["count"] == 2
        assert body["has_more"] is True

    def test_invalid_limit_returns_400(self, auth_client: Client) -> None:
        _make_entry("s:1", 1)
        resp = auth_client.get(
            reverse("django_rakaia:stream_events_api", args=["s:1"]),
            {"limit": "abc"},
        )
        assert resp.status_code == 400

    def test_invalid_after_offset_returns_400(self, auth_client: Client) -> None:
        _make_entry("s:1", 1)
        resp = auth_client.get(
            reverse("django_rakaia:stream_events_api", args=["s:1"]),
            {"after_offset": "abc"},
        )
        assert resp.status_code == 400

    def test_foreign_after_offset_is_refused(self, auth_client: Client) -> None:
        """An offset in the other store's format must fail, not resolve.

        Python reads ``_`` as a digit separator, so ``int("0_5")`` is 5: the
        in-memory store's compound ``{seq}_{byte}`` offset would silently
        resolve to an unrelated position and return the wrong window with a
        200. Only the store that issued an offset can say whether it owns it,
        and this view serves durable (plain-integer) offsets.
        """
        for off in (1, 2, 3, 4, 5, 6):
            _make_entry("s:1", off)
        resp = auth_client.get(
            reverse("django_rakaia:stream_events_api", args=["s:1"]),
            {"after_offset": "0_5"},
        )
        assert resp.status_code == 400, (
            f"foreign offset resolved instead of being refused: {resp.content!r}"
        )
