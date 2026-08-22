"""In-memory StreamStore against the shared protocol-server contract."""

from __future__ import annotations

import pytest

from rakaia.store import StreamStore
from tests.server_store_contract import ServerStoreContract


class TestStreamStoreServerContract(ServerStoreContract):
    # CHARACTERISING (#214): `append_many` here is a loop of `append`, so it
    # inherits `append`'s one-level array flatten. Inherited rather than chosen,
    # which is why the question is open.
    append_many_flattens_json_arrays = True

    @pytest.fixture
    def store(self) -> StreamStore:
        return StreamStore()
