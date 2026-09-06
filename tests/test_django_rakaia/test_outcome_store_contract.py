"""SPIKE — the Django outcome store against the shared suites.

The point is not that this store works. It is whether the suites written for the
other two catch a constraint neither of them has, without anyone adding a case for
it. If they do, the class the redesign set out to close is closed.
"""

from __future__ import annotations

import pytest

from django_rakaia.outcomes import DjangoOutcomeStore
from tests.outcome_store_contract import OutcomeStoreContract


@pytest.mark.django_db
class TestDjangoOutcomeStoreContract(OutcomeStoreContract):
    @pytest.fixture
    def outcomes(self) -> DjangoOutcomeStore:
        return DjangoOutcomeStore()
