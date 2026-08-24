"""What the shipped URL file can address, and what it needs installed (#230, #231).

Two defects in one thirty-line file, and they interact, so they are tested
together.

**#231 — slashes.** Stream names are the server's business; the protocol says so
and offers `/v1/stream/{path}` as an example scheme. `StreamStore.create` takes a
name as given, so `submissions/status` is created, appended to and read back
without complaint — and then cannot be reached through the URL file, because
Django's `str` converter refuses a slash. `SCRATCH_PATH` is `_scratch/fold`, so
rakaia produces such a name itself.

**#230 — the live-updates import.** A consumer that only polls still had to
install `channels`, because this file imported the SSE view at module scope. The
setting that is meant to say "this tier does not do live updates" could not help:
Django imports the URLconf before any of it is consulted.

**Why the ordering tests matter more than they look.** The fix for #231 swaps
`str:` for `path:`, which is greedy where `str:` is not — so the *order* of the
routes becomes load-bearing, and getting it wrong silently redirects the whole
API to the dashboard view. Every test here resolves a real path rather than
reversing a name, because reversing agrees with a wrong ordering.
"""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager

import pytest
from django.urls import clear_url_caches, resolve

from django_rakaia import urls as urls_module
from django_rakaia.envelope import SCRATCH_PATH

PREFIX = "/streams"


def _names(module) -> list[str]:
    return [pattern.name for pattern in module.urlpatterns]


@contextmanager
def _channels_missing():
    """Make `import channels` fail, as it does on a polling-only tier.

    `None` in `sys.modules` is the documented way to make an import raise
    `ImportError` — the same failure a tier that never installed the extra gets,
    without needing an environment that lacks it.
    """
    blocked = (
        "channels",
        "channels.layers",
        "django_rakaia.channels_views",
        "django_rakaia.channels_signals",
    )
    saved = {name: sys.modules.get(name, ...) for name in blocked}
    for name in blocked:
        sys.modules[name] = None  # type: ignore[assignment]
    try:
        yield
    finally:
        for name, module in saved.items():
            if module is ...:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


@pytest.fixture(autouse=True)
def _restore_urls():
    """Reloading the URL module is what these tests do; put it back afterwards
    so a later test does not inherit a half-configured urlconf."""
    yield
    importlib.reload(urls_module)
    clear_url_caches()


class TestNamesWithSlashes:
    """#231. A slash in a stream name must be addressable, not a 404."""

    def test_the_read_api_reaches_a_slashed_name(self):
        match = resolve(f"{PREFIX}/api/streams/submissions/status/")
        assert match.url_name == "stream_events_api"
        assert match.kwargs["stream_id"] == "submissions/status"

    def test_the_dashboard_reaches_a_slashed_name(self):
        match = resolve(f"{PREFIX}/submissions/status/")
        assert match.url_name == "stream_detail"
        assert match.kwargs["stream_id"] == "submissions/status"

    def test_rakaias_own_scratch_stream_is_addressable(self):
        """`SCRATCH_PATH` is `_scratch/fold`, so the library generates a name of
        the shape its own URL file could not reach."""
        match = resolve(f"{PREFIX}/api/streams/{SCRATCH_PATH}/")
        assert match.kwargs["stream_id"] == SCRATCH_PATH

    def test_a_deeply_nested_name_is_addressable(self):
        match = resolve(f"{PREFIX}/api/streams/a/b/c/d/")
        assert match.kwargs["stream_id"] == "a/b/c/d"

    def test_a_plain_name_is_unaffected(self):
        match = resolve(f"{PREFIX}/api/streams/orders/")
        assert match.url_name == "stream_events_api"
        assert match.kwargs["stream_id"] == "orders"


class TestTheGreedyConverterDoesNotSwallowTheApi:
    """The regression the #231 fix can introduce if the routes stay in order.

    `path:` matches slashes, so a catch-all dashboard route listed first claims
    `api/streams/foo/` as a stream named `api/streams/foo`. The read API would
    then return an HTML page, and every test that *reverses* a URL name would
    still pass.
    """

    def test_the_api_index_is_not_taken_for_a_stream(self):
        assert resolve(f"{PREFIX}/api/streams/").url_name == "streams_api"

    def test_a_read_api_url_is_not_taken_for_a_dashboard_page(self):
        match = resolve(f"{PREFIX}/api/streams/orders/")
        assert match.url_name == "stream_events_api", (
            "the dashboard catch-all swallowed an API URL — the greedy "
            "converter needs the API routes listed first"
        )

    def test_the_dashboard_index_still_resolves(self):
        assert resolve(f"{PREFIX}/").url_name == "streams_index"


class TestTheLiveUpdatesRoute:
    def test_the_sse_route_resolves_when_channels_is_installed(self):
        match = resolve(f"{PREFIX}/api/streams/orders/sse/")
        assert match.url_name == "stream_events_sse"
        assert match.kwargs["stream_id"] == "orders"

    def test_the_sse_route_reaches_a_slashed_name(self):
        match = resolve(f"{PREFIX}/api/streams/submissions/status/sse/")
        assert match.url_name == "stream_events_sse"
        assert match.kwargs["stream_id"] == "submissions/status"

    def test_a_stream_named_like_the_sse_suffix_loses_to_the_endpoint(self):
        """The one ambiguity `path:` cannot remove, pinned deliberately.

        A stream genuinely named `orders/sse` spells the same URL as the SSE
        endpoint of a stream named `orders`. The endpoint wins, because the
        alternative makes SSE unreachable for *every* stream rather than making
        one unusual name unreachable through this route.
        """
        match = resolve(f"{PREFIX}/api/streams/orders/sse/")
        assert match.url_name == "stream_events_sse"
        assert match.kwargs["stream_id"] == "orders"


class TestPollingWithoutTheLiveUpdatesLibrary:
    """#230. The setting that says "no live updates" has to mean it."""

    def test_the_url_file_imports_without_channels(self):
        with _channels_missing():
            reloaded = importlib.reload(urls_module)
        assert "streams_index" in _names(reloaded)

    def test_the_polling_api_is_still_routed_without_channels(self):
        with _channels_missing():
            reloaded = importlib.reload(urls_module)
        assert "stream_events_api" in _names(reloaded)
        assert "streams_api" in _names(reloaded)

    def test_the_sse_route_is_simply_absent_without_channels(self):
        with _channels_missing():
            reloaded = importlib.reload(urls_module)
        assert "stream_events_sse" not in _names(reloaded)

    def test_opting_out_drops_the_route_even_with_channels_installed(self, settings):
        settings.RAKAIA_ENABLE_SSE = False
        reloaded = importlib.reload(urls_module)
        assert "stream_events_sse" not in _names(reloaded)
        assert "stream_events_api" in _names(reloaded)

    def test_asking_for_sse_without_the_library_still_fails_loudly(self, settings):
        """The third state of the gate in `apps.py`: an explicit opt-*in* with
        the extra missing is a real misconfiguration and must not be silently
        downgraded to polling."""
        settings.RAKAIA_ENABLE_SSE = True
        with _channels_missing(), pytest.raises(ImportError):
            importlib.reload(urls_module)

    def test_the_default_still_wires_sse_when_channels_is_present(self, settings):
        if hasattr(settings, "RAKAIA_ENABLE_SSE"):
            del settings.RAKAIA_ENABLE_SSE
        reloaded = importlib.reload(urls_module)
        assert "stream_events_sse" in _names(reloaded)


@pytest.mark.django_db
class TestReachingASlashedStreamForReal:
    """Routing is not the whole claim — the views have to serve the name too.

    `resolve()` proves a URL reaches a view. It does not prove the view can find
    a stream whose id contains a slash, which is what the issue is actually
    about: the first production consumer names every stream family this way and
    could not read any of them back through the dashboard or the API.
    """

    @pytest.fixture
    def auth_client(self):
        from django.contrib.auth import get_user_model
        from django.test import Client

        from django_rakaia.models import Stream, StreamEntry, StreamEvent

        client = Client()
        client.force_login(
            get_user_model().objects.create_user(username="dash", password="pw")
        )
        # The test app emits a stream event on every `auth.User` save, so start
        # from a clean slate rather than counting those incidental rows.
        StreamEntry.objects.all().delete()
        StreamEvent.objects.all().delete()
        Stream.objects.all().delete()
        return client

    @pytest.fixture
    def seeded(self, auth_client):  # noqa: ARG002 - ordering: it clears the DB
        from django_rakaia.django_store import DjangoStreamStore

        store = DjangoStreamStore()
        store.create("submissions/status")
        store.append("submissions/status", b'{"state": "approved"}')
        return store

    def test_the_read_api_returns_the_events(self, auth_client, seeded):  # noqa: ARG002
        import json

        response = auth_client.get(f"{PREFIX}/api/streams/submissions/status/")

        assert response.status_code == 200
        body = json.loads(response.content)
        assert body["stream_id"] == "submissions/status"
        assert body["count"] == 1
        assert body["events"][0]["data"] == {"state": "approved"}

    def test_the_dashboard_page_renders(self, auth_client, seeded):  # noqa: ARG002
        response = auth_client.get(f"{PREFIX}/submissions/status/")
        assert response.status_code == 200

    def test_an_unknown_slashed_name_reads_as_empty_not_missing(
        self,
        auth_client,
        seeded,  # noqa: ARG002 - the stream it seeds is not the one requested
    ):
        """The failure the issue names is a bare 404 that does not say what went
        wrong. An unknown stream should read as empty, exactly as an unknown
        plain name does — the route existing is what makes that possible."""
        import json

        response = auth_client.get(f"{PREFIX}/api/streams/nothing/here/")

        assert response.status_code == 200
        assert json.loads(response.content)["count"] == 0

    def test_the_route_exists_even_for_an_anonymous_caller(self, client):
        """A 302 to the login page proves the URL resolved. Before the fix this
        was a 404, and the two are easy to confuse when debugging."""
        response = client.get(f"{PREFIX}/api/streams/submissions/status/")
        assert response.status_code == 302
        assert "/accounts/login" in response["Location"]
