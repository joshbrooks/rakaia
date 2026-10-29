"""Property: the rebuild check certifies exactly the untouched projection (#340).

Rebuild a small log, perturb the live table (nothing, a stray row, a deleted row,
a changed field), and the verdict must be green only for "nothing" -- and a red
report must name the row that was touched.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from django_rakaia.rebuild import rebuild_and_verify

from .models import FinanceLine
from .test_rebuild import PATH, _live, _registry, _seed_log

pytestmark = pytest.mark.django_db(databases=["default", "overlay"])

_row = st.fixed_dictionaries(
    {
        "suku": st.sampled_from(["s1", "s2", "s3"]),
        "delta": st.integers(min_value=0, max_value=9),
    }
)


@st.composite
def _case(draw):
    n = draw(st.integers(min_value=1, max_value=4))
    rows = [{"id": f"r{i}", **draw(_row)} for i in range(n)]
    kind = draw(st.sampled_from(["none", "stray", "delete", "alter"]))
    target = draw(st.integers(min_value=0, max_value=n - 1))
    return rows, kind, target


@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(_case())
def test_certified_exactly_when_the_live_table_is_untouched(case):
    rows, kind, target = case
    # Examples share one test transaction, so each starts from a clean slate.
    for alias in ("default", "overlay"):
        FinanceLine.objects.using(alias).all().delete()
    from django_rakaia.django_store import DjangoStreamStore

    DjangoStreamStore().delete(PATH, force=True)

    _seed_log(*rows)
    _live(*rows)
    victim = f"r{target}"
    if kind == "stray":
        _live({"id": "ghost", "suku": "s9", "delta": 9})
        victim = "ghost"
    elif kind == "delete":
        FinanceLine.objects.filter(submission_id=victim).delete()
    elif kind == "alter":
        FinanceLine.objects.filter(submission_id=victim).update(delta=99)

    report = rebuild_and_verify(
        PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
    )

    assert report.certified == (kind == "none"), (kind, str(report))
    if kind != "none":
        assert victim in str(report), str(report)
