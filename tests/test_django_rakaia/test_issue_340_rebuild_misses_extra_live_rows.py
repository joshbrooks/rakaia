"""GitHub #340: `rebuild_and_verify` only compares effects to live rows, one way."""

from __future__ import annotations

import pytest

from django_rakaia.rebuild import rebuild_and_verify

from .models import FinanceLine
from .test_rebuild import PATH, _live, _registry, _seed_log

pytestmark = pytest.mark.django_db(databases=["default", "overlay"])


@pytest.mark.xfail(
    strict=True,
    reason="GitHub #340: a live row no event produces still certifies the rebuild",
)
def test_a_live_row_the_log_never_produced_fails_the_rebuild():
    row = {"id": "a", "suku": "s1", "delta": 1}
    _seed_log(row)
    _live(row, {"id": "ghost", "suku": "s9", "delta": 9})

    report = rebuild_and_verify(
        PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
    )

    assert not report.certified
