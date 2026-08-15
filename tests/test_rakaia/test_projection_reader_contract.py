"""In-memory DictProjections against the shared ProjectionReader contract."""

from __future__ import annotations

import pytest

from rakaia.executors import DictProjections
from tests.projection_reader_contract import ProjectionReaderContract, ReaderSeam


class TestDictProjectionsReaderContract(ProjectionReaderContract):
    @pytest.fixture
    def seam(self) -> ReaderSeam:
        projections = DictProjections()
        return ReaderSeam(
            apply=projections.apply,
            make_reader=lambda _effects: projections,
            model="app.Alert",
        )
