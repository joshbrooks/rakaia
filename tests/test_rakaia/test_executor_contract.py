"""In-memory InMemoryProjections against the shared Executor conformance contract,
and the same contract again through a `RecordingExecutor` wrapping it."""

from __future__ import annotations

import pytest

from rakaia.effects import EffectCollisionError, Ref, Upsert
from rakaia.executors import (
    CollectingExecutor,
    InMemoryProjections,
    MultipleRowsMatched,
    RecordingExecutor,
)
from tests.executor_contract import (
    ExecutorChecksContract,
    ExecutorContract,
    ExecutorSeam,
)


class TestInMemoryProjectionsExecutorContract(ExecutorContract):
    @pytest.fixture
    def seam(self) -> ExecutorSeam:
        projections = InMemoryProjections()
        # Executor and reader are the same object; the model label is arbitrary
        # because the tables are created on demand.
        return ExecutorSeam(
            executor=projections,
            reader=projections,
            model="app.Alert",
            multiple_rows_error=MultipleRowsMatched,
        )


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
            multiple_rows_error=MultipleRowsMatched,
        )

    def test_the_contract_run_left_a_recording(self, seam):
        """The suite reads rows, so nothing else in it would notice an empty
        recording."""
        seam.executor.apply([self._upsert(seam, "a")])

        assert [e.lookup["field_key"] for e in seam.executor.effects] == ["a"]


class TestCollectingExecutorChecksContract(ExecutorChecksContract):
    """A dry run runs the checks a real write runs. It keeps no rows, so it takes
    the half of the contract that reads none back."""

    @pytest.fixture
    def seam(self) -> ExecutorSeam:
        return ExecutorSeam(
            executor=CollectingExecutor(),
            reader=InMemoryProjections(),
            model="app.Alert",
            multiple_rows_error=MultipleRowsMatched,
        )

    def test_a_batch_that_raises_is_not_recorded(self, seam):
        alert = {"stream_key": "s", "alert_type": "m", "field_key": "a"}
        collide = [
            Upsert(model_label=seam.model, lookup=alert, defaults={"severity": v})
            for v in ("info", "error")
        ]
        with pytest.raises(EffectCollisionError):
            seam.executor.apply(collide)

        assert seam.executor.effects == []

    def test_effects_are_kept_as_emitted_with_refs_unresolved(self, seam):
        effects = [
            Upsert(
                model_label=seam.model,
                lookup={"field_key": "a"},
                defaults={},
                produces="anchor",
            ),
            Upsert(
                model_label=seam.model,
                lookup={"field_key": "b"},
                defaults={"dismissed_version": Ref("anchor")},
            ),
        ]
        seam.executor.apply(effects)

        assert seam.executor.effects == effects
