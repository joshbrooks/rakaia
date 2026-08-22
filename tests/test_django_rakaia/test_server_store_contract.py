"""Durable DjangoStreamStore against the shared protocol-server contract.

The same contract the in-memory store passes. Passing it is what lets
`rakaia.create_app` serve the protocol straight off the database, instead of
the Django integration carrying a second implementation of it.

`transaction=True` because the async cases reach the database through
`sync_to_async`, i.e. from another thread, which the default per-test
transaction wrapper would hide.
"""

from __future__ import annotations

import pytest
from asgiref.sync import sync_to_async

from django_rakaia.django_store import DjangoStreamStore
from tests.server_store_contract import ServerStoreContract


@pytest.mark.django_db(transaction=True)
class TestDjangoStreamStoreServerContract(ServerStoreContract):
    # CHARACTERISING (#214): `append_many` here declines the flatten
    # deliberately — a batch item is one event whose payload may be a list. The
    # opposite of the in-memory store, and not yet reconciled.
    append_many_flattens_json_arrays = False

    @pytest.fixture
    def store(self) -> DjangoStreamStore:
        return DjangoStreamStore()

    @staticmethod
    async def _sync(fn, *args, **kwargs):
        """The ORM cannot be touched directly from an async test."""
        return await sync_to_async(fn)(*args, **kwargs)
