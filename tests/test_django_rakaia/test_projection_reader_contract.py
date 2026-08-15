"""The two Django projection readers against the shared reader contract.

`PreloadedProjectionReader` exists purely so `diff_effects_against_rows` can do
one query per lookup-shape instead of one per effect. Nothing in the code makes
it *fail* if it answers differently from the plain `DjangoProjectionReader` — it
would just quietly report a rebuild as verified when it is not. Binding both to
one contract is what turns that into a test failure.
"""

from __future__ import annotations

import pytest

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from django_rakaia.verification import PreloadedProjectionReader
from tests.projection_reader_contract import ProjectionReaderContract, ReaderSeam

MODEL = "test_django_rakaia.Alert"


@pytest.mark.django_db
class TestDjangoProjectionReaderContract(ProjectionReaderContract):
    @pytest.fixture
    def seam(self) -> ReaderSeam:
        return ReaderSeam(
            apply=DjangoExecutor().apply,
            make_reader=lambda _effects: DjangoProjectionReader(),
            model=MODEL,
        )


@pytest.mark.django_db
class TestPreloadedProjectionReaderContract(ProjectionReaderContract):
    @pytest.fixture
    def seam(self) -> ReaderSeam:
        return ReaderSeam(
            apply=DjangoExecutor().apply,
            make_reader=PreloadedProjectionReader,
            model=MODEL,
        )
