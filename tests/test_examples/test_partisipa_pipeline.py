"""What the pipeline example claims, tested over in-memory tables.

The demo saves seven forms one at a time and then rebuilds everything from the
log. These tests pin the claims that are about the pipeline rather than about
Django, which `just pipeline-demo` covers against real tables:

* saving forms one by one and rebuilding them all at once give the same tables,
  because both routes take their handlers from one step list;
* a report saved before its project's registration has no link, and the save
  that brings the registration records its assignment, with the reason;
* a rebuild replays the assignments the saves recorded, even one the matching
  rule would not make, and a report keeps the first project it was given;
* a step that decides something on a save must say why a rebuild does not;
* every step that produces something outside the database (an email) says a
  rebuild does not reproduce it, and only those steps do;
* ``--until`` runs a prefix of the list and nothing after it.
"""

from __future__ import annotations

from typing import Any

import pytest
from pipeline.forms import FORMS, ROAD, STREAM, WATER, WATER_REGISTRATION
from pipeline.routes import rebuild, save_form
from pipeline.steps import STEPS, Step, build_registry, steps_until

from rakaia import ExternalEffect, InMemoryProjections, StreamStore

TABLES = (
    "pipeline.Project",
    "pipeline.ProgressReport",
    "pipeline.ProgressLine",
    "pipeline.ProjectLink",
    "pipeline.ReportStatus",
    "pipeline.MonthReadiness",
)


def _table(projections: InMemoryProjections, label: str) -> list[list[tuple[str, Any]]]:
    """A table's rows without the synthetic ``pk``, which differs by insert order."""
    return sorted(
        sorted((k, v) for k, v in row.items() if k != "pk")
        for row in projections.rows(label)
    )


def _save_all(
    steps: Any = STEPS,
) -> tuple[StreamStore, InMemoryProjections, list[ExternalEffect]]:
    store, live, outbox = StreamStore(), InMemoryProjections(), []
    registry = build_registry(steps)
    for _, form in FORMS:
        save_form(
            store,
            STREAM,
            form,
            registry,
            executor=live,
            reader=live,
            outbox=outbox,
            steps=steps,
        )
    return store, live, outbox


def test_saving_one_form_at_a_time_gives_the_tables_a_rebuild_gives() -> None:
    store, live, _ = _save_all()
    rebuilt = InMemoryProjections()
    rebuild(store, STREAM, build_registry(), executor=rebuilt, reader=rebuilt)

    for label in TABLES:
        assert _table(live, label) == _table(rebuilt, label), label
    assert live.get("pipeline.MonthReadiness", suku="Fatuberliu").ready is True


def test_the_save_that_brings_a_late_registration_assigns_the_waiting_report() -> None:
    store, live, outbox = StreamStore(), InMemoryProjections(), []
    registry = build_registry()
    arrived = next(
        i for i, (_, f) in enumerate(FORMS) if f.get("key") == WATER_REGISTRATION
    )

    for index, (_, form) in enumerate(FORMS[: arrived + 1]):
        recorded = save_form(
            store, STREAM, form, registry, executor=live, reader=live, outbox=outbox
        )
        link = live.get("pipeline.ProjectLink", report_key=WATER)
        if index < arrived:
            assert link is None, index
        else:
            assert link is not None and link.registration == WATER_REGISTRATION
            assert [(e["report"], e["registration"]) for e in recorded] == [
                (WATER, WATER_REGISTRATION)
            ]
            assert link.because == (
                "matched suku Fatuberliu and output WATER when its registration arrived"
            )


def test_a_rebuild_keeps_an_assignment_the_rule_would_not_make() -> None:
    """A rebuild replays what was recorded; it does not decide again.

    The road report is reassigned by hand to the water registration, which the
    suku-and-output rule would never choose. A rebuild that re-decided would put
    it back on the road registration.
    """
    store, live, outbox = _save_all()
    manual = {
        "form_type": "ASSIGNED",
        "report": ROAD,
        "registration": WATER_REGISTRATION,
        "because": "picked by hand",
    }
    save_form(
        store,
        STREAM,
        manual,
        build_registry(),
        executor=live,
        reader=live,
        outbox=outbox,
    )
    rebuilt = InMemoryProjections()
    rebuild(store, STREAM, build_registry(), executor=rebuilt, reader=rebuilt)

    link = rebuilt.get("pipeline.ProjectLink", report_key=ROAD)
    assert (link.registration, link.because) == (WATER_REGISTRATION, "picked by hand")
    assert _table(rebuilt, "pipeline.ProjectLink") == _table(
        live, "pipeline.ProjectLink"
    )


def test_a_resubmitted_report_keeps_the_project_it_was_first_given() -> None:
    store, live, outbox = StreamStore(), InMemoryProjections(), []
    registry = build_registry()
    recorded = []
    for _, form in FORMS:
        recorded += save_form(
            store, STREAM, form, registry, executor=live, reader=live, outbox=outbox
        )
    assert sorted(e["report"] for e in recorded) == [ROAD, WATER]


def test_a_step_that_decides_on_a_save_must_say_why_a_rebuild_does_not() -> None:
    with pytest.raises(ValueError, match="decides on a save"):
        Step(name="sneaky", does="decides quietly", decide=lambda _form, _reader: [])


def test_only_a_step_that_reaches_outside_says_what_a_rebuild_does_with_it() -> None:
    """Add the steps one at a time and see which one starts the emails."""
    emailing = set()
    sent_before = 0
    for step in STEPS:
        _, _, outbox = _save_all(steps_until(step.name))
        if len(outbox) > sent_before:
            emailing.add(step.name)
        sent_before = len(outbox)

    declared = {
        step.name
        for step in STEPS
        if step.on_rebuild is not None and step.on_rebuild.kind == "not_reproduced"
    }
    assert emailing == declared == {"notify_coordinator"}
    notify = next(step for step in STEPS if step.name == "notify_coordinator")
    assert notify.on_rebuild is not None
    assert notify.on_rebuild.kind == "not_reproduced"


def test_a_rebuild_hands_the_emails_back_instead_of_sending_them() -> None:
    store, _, outbox = _save_all()
    rebuilt = InMemoryProjections()
    result = rebuild(store, STREAM, build_registry(), executor=rebuilt, reader=rebuilt)

    assert [e.payload["subject"] for e in result.external] == [
        e.payload["subject"] for e in outbox
    ]
    assert len(outbox) == 2


def test_until_runs_the_named_step_and_none_after_it() -> None:
    _, live, outbox = _save_all(steps_until("form_rows"))

    assert len(live.rows("pipeline.ProgressLine")) == 4
    assert all("total" not in row for row in live.rows("pipeline.ProgressReport"))
    assert live.rows("pipeline.ReportStatus") == []
    assert outbox == []
    with pytest.raises(ValueError, match="no step named 'totlas'"):
        steps_until("totlas")
