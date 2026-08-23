"""`RAKAIA_STORE` must not fail open.

`get_store()` reads one setting and returns one of two stores. The failure mode
that matters is a *misspelt* backend: the setting is a free-form string, so
``"durrable"`` is indistinguishable from ``"memory"`` to the selector. Falling
back to the in-memory store in that case is the worst possible default — every
append lands in a process-local dict and is lost on restart, silently, with the
deployment believing it is durable.

ADR 0002 named this ("`RAKAIA_STORE` swaps stores by string with no interface
check"), and the first real consumer wrote its own Django system check to catch
it downstream. These tests move the guard upstream, where it belongs.
"""

from __future__ import annotations

import pytest
from django.core.exceptions import ImproperlyConfigured

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.store import get_store, reset_store_cache
from rakaia import JsonlStreamStore, StreamStore


@pytest.fixture(autouse=True)
def _clear_store_cache():
    """`get_store()` memoises per backend name; drop the cache around each test
    so one test's selection can't satisfy the next one's lookup.

    This goes through `reset_store_cache()` rather than mutating the private
    `_stores` dict, which is what this file used to do — the one place in the
    suite that reached past an interface by design.
    """
    reset_store_cache()
    yield
    reset_store_cache()


class TestBackendSelection:
    def test_default_is_the_in_memory_store(self, settings):
        del settings.RAKAIA_STORE
        assert isinstance(get_store(), StreamStore)

    def test_memory_selects_the_in_memory_store(self, settings):
        settings.RAKAIA_STORE = "memory"
        assert isinstance(get_store(), StreamStore)

    def test_durable_selects_the_django_store(self, settings):
        settings.RAKAIA_STORE = "durable"
        assert isinstance(get_store(), DjangoStreamStore)

    def test_jsonl_selects_the_file_backed_store(self, settings, tmp_path):
        settings.RAKAIA_STORE = "jsonl"
        settings.RAKAIA_JSONL_ROOT = str(tmp_path / "streams")
        assert isinstance(get_store(), JsonlStreamStore)

    def test_the_jsonl_root_is_the_one_configured(self, settings, tmp_path):
        settings.RAKAIA_STORE = "jsonl"
        settings.RAKAIA_JSONL_ROOT = str(tmp_path / "streams")
        store = get_store()
        store.create("s")
        assert (tmp_path / "streams" / "s" / "meta.json").exists()


class TestTheJsonlRootIsRequired:
    """A file-backed store with no root must refuse, not guess.

    The same failure `RAKAIA_STORE` itself has: a guessed root — a temp
    directory, the working directory — accepts every append and puts the log
    somewhere the next deployment does not look. That is the in-memory
    silent-loss failure with extra steps.
    """

    def test_jsonl_without_a_root_raises(self, settings):
        settings.RAKAIA_STORE = "jsonl"
        if hasattr(settings, "RAKAIA_JSONL_ROOT"):
            del settings.RAKAIA_JSONL_ROOT
        with pytest.raises(ImproperlyConfigured, match="RAKAIA_JSONL_ROOT"):
            get_store()

    def test_the_check_reports_it_before_the_first_append(self, settings):
        from django_rakaia.checks import check_store_backend

        settings.RAKAIA_STORE = "jsonl"
        if hasattr(settings, "RAKAIA_JSONL_ROOT"):
            del settings.RAKAIA_JSONL_ROOT
        errors = check_store_backend(None)
        assert [e.id for e in errors] == ["rakaia.E002"]

    def test_a_configured_root_raises_no_error(self, settings, tmp_path):
        from django_rakaia.checks import check_store_backend

        settings.RAKAIA_STORE = "jsonl"
        settings.RAKAIA_JSONL_ROOT = str(tmp_path)
        settings.DEBUG = False
        # Errors only: a configured root may still draw the W002 warning about
        # live subscribers, which is a different subject and has its own test.
        errors = [m for m in check_store_backend(None) if m.id.startswith("rakaia.E")]
        assert errors == []


class TestUnknownBackendIsRefused:
    """The RED case: today every string below returns a `StreamStore`."""

    @pytest.mark.parametrize(
        "backend",
        [
            "durrable",  # the typo that motivates this
            "durable ",  # a stray space from a .env file
            "Durable",  # wrong case
            "postgres",  # a plausible-but-wrong guess
            "db",
            "",
        ],
    )
    def test_unknown_backend_raises_instead_of_falling_back(self, settings, backend):
        settings.RAKAIA_STORE = backend
        with pytest.raises(ImproperlyConfigured):
            get_store()

    def test_the_error_names_the_backend_and_the_valid_choices(self, settings):
        settings.RAKAIA_STORE = "durrable"
        with pytest.raises(ImproperlyConfigured) as exc:
            get_store()
        message = str(exc.value)
        assert "durrable" in message
        assert "durable" in message
        assert "memory" in message

    def test_the_check_reports_it_before_the_first_append(self, settings):
        """`get_store()` only refuses on first use, which in a worker may be
        hours in. The check has to catch it at startup."""
        from django_rakaia.checks import check_store_backend

        settings.RAKAIA_STORE = "durrable"
        ids = [e.id for e in check_store_backend(None)]
        assert ids == ["rakaia.E001"]

    def test_a_refused_backend_is_not_cached(self, settings):
        """A failed selection must not poison the cache — fixing the setting
        without restarting the process has to work.

        Asserted through behaviour rather than by inspecting the cache dict: if
        the refusal had left an entry behind, the second call would hand it back
        instead of raising again.
        """
        settings.RAKAIA_STORE = "durrable"
        with pytest.raises(ImproperlyConfigured):
            get_store()
        with pytest.raises(ImproperlyConfigured):
            get_store()

        settings.RAKAIA_STORE = "memory"
        assert isinstance(get_store(), StreamStore)


class TestTheCacheAndItsReset:
    """`reset_store_cache()` replaces the private-dict poke this file used to do.

    Both halves are asserted, because a no-op reset would pass a test that only
    checked the second: memoisation has to be real for dropping it to mean
    anything.
    """

    def test_the_same_backend_is_memoised(self, settings):
        settings.RAKAIA_STORE = "memory"
        assert get_store() is get_store()

    def test_reset_forces_a_rebuild(self, settings):
        settings.RAKAIA_STORE = "memory"
        first = get_store()
        reset_store_cache()
        assert get_store() is not first

    def test_reset_is_safe_on_an_empty_cache(self):
        reset_store_cache()
        reset_store_cache()


class TestTheJsonlStoreDoesNotBroadcast:
    """A capability the durable store has and this one cannot.

    `DjangoStreamStore` publishes every append over channels as it writes it.
    `JsonlStreamStore` lives in the framework-independent package and has no way
    to reach Django, so a deployment that switches backends and has live
    consumers would find them going quietly silent. A warning at
    `manage.py check` is where this repo puts that kind of surprise.
    """

    def test_jsonl_with_channels_installed_warns(self, settings, tmp_path):
        from django_rakaia.checks import check_store_backend

        settings.RAKAIA_STORE = "jsonl"
        settings.RAKAIA_JSONL_ROOT = str(tmp_path)
        warnings = check_store_backend(None)
        assert [w.id for w in warnings] == ["rakaia.W002"]

    def test_the_durable_store_is_silent_about_broadcasting(self, settings):
        from django_rakaia.checks import check_store_backend

        settings.RAKAIA_STORE = "durable"
        assert [w.id for w in check_store_backend(None)] == []


class TestInMemoryInProductionIsFlagged:
    """A correctly-spelt `"memory"` is legitimate in development and almost
    never meant in production — worth a warning, never an error."""

    def test_memory_with_debug_off_warns(self, settings):
        from django_rakaia.checks import check_store_backend

        settings.RAKAIA_STORE = "memory"
        settings.DEBUG = False
        assert [w.id for w in check_store_backend(None)] == ["rakaia.W001"]

    def test_memory_with_debug_on_is_silent(self, settings):
        from django_rakaia.checks import check_store_backend

        settings.RAKAIA_STORE = "memory"
        settings.DEBUG = True
        assert check_store_backend(None) == []

    def test_durable_is_silent(self, settings):
        from django_rakaia.checks import check_store_backend

        settings.RAKAIA_STORE = "durable"
        settings.DEBUG = False
        assert check_store_backend(None) == []
