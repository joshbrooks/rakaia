"""GitHub #340: `rebuild_and_verify` only compares effects to live rows, one way."""

from __future__ import annotations

import pytest

from django_rakaia.rebuild import rebuild_and_verify
from rakaia.effects import Upsert

from .models import FinanceLine
from .test_rebuild import PATH, _live, _registry, _seed_log

pytestmark = pytest.mark.django_db(databases=["default", "overlay"])


def test_a_live_row_the_log_never_produced_fails_the_rebuild():
    row = {"id": "a", "suku": "s1", "delta": 1}
    _seed_log(row)
    _live(row, {"id": "ghost", "suku": "s9", "delta": 9})

    report = rebuild_and_verify(
        PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
    )

    assert not report.certified
    assert "ghost" in str(report) and "only in the live table" in str(report)


def test_a_row_the_log_deleted_but_live_still_has_fails_the_rebuild():
    import json

    from rakaia.effects import Delete
    from rakaia.registry import HandlerRegistry

    a, b = {"id": "a", "suku": "s", "delta": 1}, {"id": "b", "suku": "s", "delta": 2}
    store = _seed_log(a, b)
    store.append(PATH, json.dumps({"id": "b", "gone": True}).encode())
    _live(a, b)

    reg = HandlerRegistry()
    reg.register(
        name="fin",
        event_match=PATH,
        fn=lambda e: [
            Delete(
                model_label="test_django_rakaia.FinanceLine",
                lookup={"submission_id": e["id"]},
            )
            if e.get("gone")
            else Upsert(
                model_label="test_django_rakaia.FinanceLine",
                lookup={"submission_id": e["id"]},
                defaults={"suku": e["suku"], "delta": e["delta"]},
            )
        ],
        effective_from=0,
    )

    report = rebuild_and_verify(
        PATH, into="overlay", live_models=[FinanceLine], registry=reg
    )

    assert not report.certified
    assert "'b'" in str(report) and "only in the live table" in str(report)


def test_a_row_written_twice_is_compared_by_its_final_state():
    _seed_log(
        {"id": "a", "suku": "s", "delta": 1},
        {"id": "a", "suku": "s", "delta": 5},
    )
    _live({"id": "a", "suku": "s", "delta": 5})

    report = rebuild_and_verify(
        PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
    )

    assert report.certified


def test_an_update_before_the_row_exists_cannot_hide_behind_the_live_value():
    from rakaia.effects import Update
    from rakaia.registry import HandlerRegistry

    _seed_log({"step": "update"}, {"step": "upsert"})
    _live({"id": "a", "suku": "s", "delta": 7})
    lookup = {"submission_id": "a"}
    reg = HandlerRegistry()
    reg.register(
        name="fin",
        event_match=PATH,
        fn=lambda e: [
            Update(
                model_label="test_django_rakaia.FinanceLine",
                lookup=lookup,
                defaults={"delta": 7},
            )
            if e["step"] == "update"
            else Upsert(
                model_label="test_django_rakaia.FinanceLine",
                lookup=lookup,
                defaults={"suku": "s", "delta": 0},
            )
        ],
        effective_from=0,
    )

    report = rebuild_and_verify(
        PATH, into="overlay", live_models=[FinanceLine], registry=reg
    )

    assert not report.certified
    assert "delta" in str(report)
