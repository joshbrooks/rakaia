"""Durable DjangoStreamStore against the shared WritableStore conformance contract.

Same contract as the in-memory store (tests/store_contract.py) — this is what
makes "test on memory, ship on durable" safe.
"""

from __future__ import annotations

import pytest

from django_rakaia.django_store import DjangoStreamStore
from tests.store_contract import StoreContract


@pytest.mark.django_db
class TestDjangoStreamStoreContract(StoreContract):
    @pytest.fixture
    def store(self) -> DjangoStreamStore:
        return DjangoStreamStore()
