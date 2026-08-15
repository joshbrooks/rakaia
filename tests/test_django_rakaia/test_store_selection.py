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
from django_rakaia.store import get_store
from rakaia import StreamStore


@pytest.fixture(autouse=True)
def _clear_store_cache():
    """`get_store()` memoises per backend name; drop the cache around each test
    so one test's selection can't satisfy the next one's lookup."""
    from django_rakaia import store as store_module

    store_module._stores.clear()
    yield
    store_module._stores.clear()


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
        without restarting the process has to work."""
        from django_rakaia import store as store_module

        settings.RAKAIA_STORE = "durrable"
        with pytest.raises(ImproperlyConfigured):
            get_store()
        assert "durrable" not in store_module._stores

        settings.RAKAIA_STORE = "memory"
        assert isinstance(get_store(), StreamStore)


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
