"""In-memory StreamStore against the shared protocol-server contract."""

from __future__ import annotations

import pytest

from rakaia.store import StreamStore
from tests.server_store_contract import ServerStoreContract


class TestStreamStoreServerContract(ServerStoreContract):
    @pytest.fixture
    def store(self) -> StreamStore:
        return StreamStore()
