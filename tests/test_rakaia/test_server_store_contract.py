"""In-memory StreamStore against the shared protocol-server contract."""

from __future__ import annotations

import pytest

from rakaia.store import StreamStore
from rakaia.testing.server_store_contract import ServerStoreContract
from rakaia.testing.tagged_reads_contract import TaggedReadsContract


class TestStreamStoreServerContract(ServerStoreContract):
    @pytest.fixture
    def store(self) -> StreamStore:
        return StreamStore()


class TestStreamStoreTaggedReads(TaggedReadsContract):
    @pytest.fixture
    def store(self) -> StreamStore:
        return StreamStore()
