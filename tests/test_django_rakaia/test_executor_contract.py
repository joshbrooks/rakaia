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

    def test_reapplying_an_identical_batch_is_reported_as_skipped(self, seam):
        """The one executor that can skip, doing so — and saying so.

        This is the counter's reason to exist. `skip_unchanged` already computed
        which columns differed and then threw the answer away, so a replay that
        rewrote every row and one that wrote nothing reported the same thing:
        converged state, and silence. A consumer measuring write churn had no
        signal at all. Now an identical second apply reports every upsert
        skipped and none written.
        """
        effects = [self._upsert(seam, "a"), self._upsert(seam, "b")]

        first = seam.executor.apply(effects)
        assert (
            first.upserts_created,
            first.upserts_written,
            first.upserts_skipped,
        ) == (2, 2, 0)

        # A byte-identical batch against the rows it just wrote: nothing to do.
        again = seam.executor.apply(effects)
        assert (
            again.upserts_created,
            again.upserts_written,
            again.upserts_skipped,
        ) == (0, 0, 2)

        # A genuine change is still written, so `skipped` tracks the data rather
        # than merely counting repeat applies.
        changed = seam.executor.apply([self._upsert(seam, "a", message="different")])
        assert (changed.upserts_written, changed.upserts_skipped) == (1, 0)
