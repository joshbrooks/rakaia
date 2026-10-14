"""Durable DjangoStreamStore against the shared protocol-server contract.

The same contract the in-memory store passes. Passing it is what lets
`rakaia.create_app` serve the protocol straight off the database, instead of
the Django integration carrying a second implementation of it.

`transaction=True` because the long-poll cases reach the database through
`run_sync`, i.e. from another thread, which the default per-test transaction
wrapper would hide.
"""

from __future__ import annotations

import pytest

from django_rakaia.django_store import DjangoStreamStore
from tests.server_store_contract import ServerStoreContract


@pytest.mark.django_db(transaction=True)
class TestDjangoStreamStoreServerContract(ServerStoreContract):
    @pytest.fixture
    def store(self) -> DjangoStreamStore:
        return DjangoStreamStore()
