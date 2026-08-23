"""The JSONL store against both shared conformance contracts (spike, #229).

This file *is* the spike's result. If both contracts pass against a store with
nothing but a directory under it, `StreamServerStore` is a real seam; if they
don't, the protocol was a description of the two backends that already existed.
"""

from __future__ import annotations

import pytest

from rakaia.jsonl_store import JsonlStreamStore
from tests.server_store_contract import ServerStoreContract
from tests.store_contract import StoreContract


@pytest.fixture
def store(tmp_path) -> JsonlStreamStore:
    # A small segment size so the roll-over is exercised by ordinary contract
    # traffic rather than only by the test that asks for it.
    return JsonlStreamStore(tmp_path / "streams", segment_size=4)


class TestJsonlStoreContract(StoreContract):
    @pytest.fixture
    def store(self, tmp_path) -> JsonlStreamStore:
        return JsonlStreamStore(tmp_path / "streams", segment_size=4)


class TestJsonlStoreServerContract(ServerStoreContract):
    @pytest.fixture
    def store(self, tmp_path) -> JsonlStreamStore:
        return JsonlStreamStore(tmp_path / "streams", segment_size=4)
