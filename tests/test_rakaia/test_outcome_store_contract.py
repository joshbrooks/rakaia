"""The in-memory outcome store against the shared contract."""

from __future__ import annotations

import pytest

from rakaia.outcomes import InMemoryOutcomeStore
from tests.outcome_store_contract import OutcomeStoreContract


class TestInMemoryOutcomeStoreContract(OutcomeStoreContract):
    @pytest.fixture
    def outcomes(self) -> InMemoryOutcomeStore:
        return InMemoryOutcomeStore()
