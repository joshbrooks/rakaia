"""In-memory StreamStore against the shared WritableStore conformance contract."""

from __future__ import annotations

import pytest

from rakaia.store import StreamStore
from tests.store_contract import StoreContract


class TestStreamStoreContract(StoreContract):
    @pytest.fixture
    def store(self) -> StreamStore:
        return StreamStore()
