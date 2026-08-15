"""In-memory DictProjections against the shared Executor conformance contract."""

from __future__ import annotations

import pytest

from rakaia.executors import DictProjections
from tests.executor_contract import ExecutorContract, ExecutorSeam


class TestDictProjectionsExecutorContract(ExecutorContract):
    @pytest.fixture
    def seam(self) -> ExecutorSeam:
        projections = DictProjections()
        # Executor and reader are the same object; the model label is arbitrary
        # because the tables are created on demand.
        return ExecutorSeam(executor=projections, reader=projections, model="app.Alert")
