"""In-memory InMemoryProjections against the shared ProjectionReader contract."""

from __future__ import annotations

import pytest

from rakaia.executors import InMemoryProjections
from tests.projection_reader_contract import ProjectionReaderContract, ReaderSeam


class TestInMemoryProjectionsReaderContract(ProjectionReaderContract):
    @pytest.fixture
    def seam(self) -> ReaderSeam:
        projections = InMemoryProjections()
        return ReaderSeam(
            apply=projections.apply,
            make_reader=lambda _effects: projections,
            model="app.Alert",
        )
