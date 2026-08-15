"""DjangoExecutor against the shared Executor conformance contract.

Same contract as the in-memory `InMemoryProjections` (tests/executor_contract.py) —
this is what makes "rehearse the batch in memory, apply it on Django" safe.
"""

from __future__ import annotations

import pytest

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from tests.executor_contract import ExecutorContract, ExecutorSeam


@pytest.mark.django_db
class TestDjangoExecutorContract(ExecutorContract):
    @pytest.fixture
    def seam(self) -> ExecutorSeam:
        return ExecutorSeam(
            executor=DjangoExecutor(),
            reader=DjangoProjectionReader(),
            model="test_django_rakaia.Alert",
        )


@pytest.mark.django_db
class TestDjangoExecutorSkipUnchangedContract(ExecutorContract):
    """`skip_unchanged=True` takes a different write path (SELECT, compare, write
    only the changed columns). It is an optimisation, so it must converge to the
    identical rows — run the whole contract against it too."""

    @pytest.fixture
    def seam(self) -> ExecutorSeam:
        return ExecutorSeam(
            executor=DjangoExecutor(skip_unchanged=True),
            reader=DjangoProjectionReader(),
            model="test_django_rakaia.Alert",
        )
