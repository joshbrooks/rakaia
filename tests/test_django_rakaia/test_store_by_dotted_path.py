"""`RAKAIA_STORE` may name a store of your own by its dotted import path (#364)."""

from __future__ import annotations

import json
from typing import get_type_hints

import httpx
import pytest
from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.core.management.base import SystemCheckError

from django_rakaia.checks import check_store_backend
from django_rakaia.integration import get_asgi_app
from django_rakaia.store import get_store, reset_store_cache
from rakaia import AppendResult, CloseResult, StreamServerStore
from rakaia.protocols import WritableStore
from tests.test_django_rakaia.custom_store import CustomStore

HERE = "tests.test_django_rakaia.custom_store"
JSON = {"content-type": "application/json"}


@pytest.fixture(autouse=True)
def _clear_store_cache():
    reset_store_cache()
    yield
    reset_store_cache()


class TestDottedPathSelectsAStore:
    def test_a_class(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.CustomStore"
        assert isinstance(get_store(), CustomStore)

    def test_a_factory_function(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.make_custom_store"
        assert isinstance(get_store(), CustomStore)

    def test_the_same_object_twice_until_reset(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.CustomStore"
        first = get_store()
        assert get_store() is first
        reset_store_cache()
        assert get_store() is not first


class TestBadValuesAreRefused:
    def test_a_short_name_that_is_not_a_backend(self, settings):
        settings.RAKAIA_STORE = "durrable"
        with pytest.raises(ImproperlyConfigured, match="dotted"):
            get_store()

    def test_a_path_that_does_not_import(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.NoSuchStore"
        with pytest.raises(ImproperlyConfigured, match="NoSuchStore"):
            get_store()

    def test_a_module_that_does_not_exist(self, settings):
        settings.RAKAIA_STORE = "no_such_package.stores.Thing"
        with pytest.raises(ImproperlyConfigured, match="no_such_package"):
            get_store()

    def test_something_that_cannot_be_called(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.not_callable"
        with pytest.raises(ImproperlyConfigured, match="callable"):
            get_store()

    def test_a_result_that_is_not_a_store(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.make_something_else"
        with pytest.raises(ImproperlyConfigured, match="StreamServerStore"):
            get_store()

    def test_a_class_that_is_not_a_store(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.NotAStore"
        with pytest.raises(ImproperlyConfigured, match="StreamServerStore"):
            get_store()

    def test_a_refusal_names_the_methods_the_store_lacks(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.NotAStore"
        with pytest.raises(ImproperlyConfigured, match="close_stream") as refused:
            get_store()
        assert "run_sync" in str(refused.value)

    @pytest.mark.parametrize("value", [".memory", "..", "memory."])
    def test_a_path_with_an_empty_part(self, settings, value):
        settings.RAKAIA_STORE = value
        with pytest.raises(ImproperlyConfigured, match="could not be imported"):
            get_store()
        assert [e.id for e in check_store_backend(None)] == ["rakaia.E003"]

    @pytest.mark.parametrize("value", [None, 7, ("memory",)])
    def test_a_value_that_is_not_text(self, settings, value):
        settings.RAKAIA_STORE = value
        with pytest.raises(ImproperlyConfigured, match="not a known store backend"):
            get_store()
        assert [e.id for e in check_store_backend(None)] == ["rakaia.E001"]

    def test_a_refused_path_is_not_cached(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.make_something_else"
        for _ in range(2):
            with pytest.raises(ImproperlyConfigured):
                get_store()


class TestManageCheck:
    def test_an_unimportable_path_fails_the_check(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.NoSuchStore"
        assert [e.id for e in check_store_backend(None)] == ["rakaia.E003"]
        with pytest.raises(SystemCheckError, match=r"rakaia\.E003"):
            call_command("check")

    def test_a_valid_path_passes(self, settings):
        settings.RAKAIA_STORE = f"{HERE}.make_custom_store"
        settings.DEBUG = False
        assert check_store_backend(None) == []
        call_command("check")

    def test_the_check_does_not_build_the_store(self, settings, monkeypatch):
        """Importing is enough to report a typo; building a user's store may
        open connections or touch files."""
        built: list[int] = []

        def counting_init(*_args: object, **_kwargs: object) -> None:
            built.append(1)

        monkeypatch.setattr(CustomStore, "__init__", counting_init)
        settings.RAKAIA_STORE = f"{HERE}.CustomStore"
        check_store_backend(None)
        assert built == []

    def test_a_misspelt_short_name_is_still_the_old_error(self, settings):
        settings.RAKAIA_STORE = "durrable"
        assert [e.id for e in check_store_backend(None)] == ["rakaia.E001"]


class TestItServesOverHttp:
    async def test_an_append_and_a_read_land_in_the_custom_store(
        self, settings
    ) -> None:
        settings.RAKAIA_STORE = f"{HERE}.make_custom_store"
        app = get_asgi_app()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),  # type: ignore[arg-type]
            base_url="http://test",
        ) as client:
            assert (await client.put("/s", headers=JSON)).status_code in (200, 201, 204)
            posted = await client.post("/s", content=b'{"id": 1}', headers=JSON)
            assert posted.status_code in (200, 204)
            assert (await client.get("/s")).json() == [{"id": 1}]

        store = get_store()
        assert isinstance(store, CustomStore)
        messages, _ = store.read("/s")
        assert [json.loads(m.data) for m in messages] == [{"id": 1}]


class TestTheProtocolsAnnounceTheirResults:
    def test_append(self):
        assert get_type_hints(WritableStore.append)["return"] is AppendResult

    def test_append_many(self):
        assert get_type_hints(WritableStore.append_many)["return"] == list[AppendResult]

    def test_close_stream(self):
        hint = get_type_hints(StreamServerStore.close_stream)["return"]
        assert hint == CloseResult | None

    def test_get_store_returns_the_server_store_protocol(self):
        assert get_type_hints(get_store)["return"] is StreamServerStore
