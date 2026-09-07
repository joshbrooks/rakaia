"""In-memory InMemoryProjections against the shared Executor conformance contract,
and the same contract again through a `RecordingExecutor` wrapping it."""

from __future__ import annotations

import pytest

from rakaia.executors import InMemoryProjections, RecordingExecutor
from tests.executor_contract import ExecutorContract, ExecutorSeam


class TestInMemoryProjectionsExecutorContract(ExecutorContract):
    @pytest.fixture
    def seam(self) -> ExecutorSeam:
        projections = InMemoryProjections()
        # Executor and reader are the same object; the model label is arbitrary
        # because the tables are created on demand.
        return ExecutorSeam(executor=projections, reader=projections, model="app.Alert")


class TestRecordingExecutorIsTransparent(ExecutorContract):
    """The whole contract, run through the recorder.

    Transparency is the recorder's only promise, and the cheapest way to hold it
    is to make every executor invariant the suite already pins -- the three
    ordered passes, one resolver per batch, collision detection before the first
    write, the upsert counts, the retire flips -- have to survive the wrap. A
    recorder that reordered, deduplicated, swallowed or re-batched anything fails
    here rather than in a consumer's rebuild gate.
    """

    @pytest.fixture
    def seam(self) -> ExecutorSeam:
        projections = InMemoryProjections()
        return ExecutorSeam(
            executor=RecordingExecutor(projections),
            reader=projections,
            model="app.Alert",
        )

    def test_the_contract_run_left_a_recording(self, seam):
        """The suite reads rows, so nothing else in it would notice an empty
        recording."""
        seam.executor.apply([self._upsert(seam, "a")])

        assert [e.lookup["field_key"] for e in seam.executor.effects] == ["a"]
