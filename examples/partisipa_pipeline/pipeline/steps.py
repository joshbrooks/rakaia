"""The pipeline as one ordered list of steps, and the handlers each step runs.

A save and a rebuild both build their handlers from `STEPS`, so neither can
leave a step out without saying so. A step that does not run the same way on a
rebuild carries a `Skip` with the kind of reason and the reason itself. This
shape is borrowed from partisipa-import's proposal for the same problem. It is
this example's own code, not part of rakaia: rakaia sees the handlers, not the
list (GitHub #374 is the request for it to see the list too).

The order matters because of stages. Every stage-0 handler runs over the whole
batch before any stage-1 work starts, so a later step can read what an earlier
one wrote: the totals read the lines, and the month check reads everything.

One step also decides something on a save: which project a report belongs to.
It records the decision as an event, and a rebuild replays the event instead of
deciding again.

Apart from that, the example runs the same handlers on a save and on a rebuild, which is where
rakaia GitHub #374 points. partisipa-import's list shares the order and the
inventory between its two routes, but most of its steps have a separate function
for each route.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from rakaia import (
    Effect,
    ExternalEffect,
    HandlerRegistry,
    Update,
    Upsert,
    reconcile_children,
)

PROJECT = "pipeline.Project"
REPORT = "pipeline.ProgressReport"
LINE = "pipeline.ProgressLine"
LINK = "pipeline.ProjectLink"
STATUS = "pipeline.ReportStatus"
READINESS = "pipeline.MonthReadiness"


# -- step 1: form_rows -------------------------------------------------------


def project_row(event: dict[str, Any]) -> Effect:
    """A project registration creates a project, named by the form's own key."""
    return Upsert(
        model_label=PROJECT,
        lookup={"registration": event["key"]},
        defaults={
            "suku": event["suku"],
            "output": event["output"],
            "name": event["name"],
            "budget": event["budget"],
        },
    )


def report_rows(event: dict[str, Any]) -> list[Effect]:
    """A progress report becomes a report row and one row per line.

    A line missing from a resubmission is deleted: `reconcile_children` ends
    with a delete of every line the new version no longer has.
    """
    report = Upsert(
        model_label=REPORT,
        lookup={"key": event["key"]},
        defaults={
            "suku": event["suku"],
            "output": event["output"],
            "period": event["period"],
        },
    )
    lines = reconcile_children(
        LINE,
        {"report_key": event["key"]},
        "line_index",
        event["lines"],
        lambda line: {"item": line["item"], "cost": line["cost"]},
    )
    return [report, *lines]


# -- step 2: status ----------------------------------------------------------


def status_row(event: dict[str, Any]) -> Effect:
    """A review decision becomes the report's current status."""
    return Upsert(
        model_label=STATUS,
        lookup={"report_key": event["report"]},
        defaults={"status": event["status"]},
    )


# -- step 3: project_links ---------------------------------------------------


def assignments(form: dict[str, Any], reader: Any) -> list[dict[str, Any]]:
    """The assignments a save records for `form`, each with the reason for it.

    This is the save's side of the step, and it is the only place a project is
    chosen. A report is assigned to the registration for its suku and output, if
    one has arrived. A registration that arrives late is assigned the reports
    already waiting for it. A report that already has a project keeps it.

    Each assignment becomes an event in the log, so a rebuild replays what was
    decided rather than deciding again.
    """
    if form["form_type"] == "PROGRESS":
        waiting = [] if reader.get(LINK, report_key=form["key"]) else [form["key"]]
        registration = reader.get(PROJECT, suku=form["suku"], output=form["output"])
        when = "when the report was saved"
    elif form["form_type"] == "PROJECT":
        waiting = [
            report.key
            for report in reader.filter(
                REPORT, suku=form["suku"], output=form["output"]
            )
            if reader.get(LINK, report_key=report.key) is None
        ]
        registration = reader.get(PROJECT, registration=form["key"])
        when = "when its registration arrived"
    else:
        return []
    if registration is None:
        return []
    because = (
        f"matched suku {registration.suku} and output {registration.output} {when}"
    )
    return [
        {
            "form_type": "ASSIGNED",
            "report": key,
            "registration": registration.registration,
            "because": because,
        }
        for key in waiting
    ]


def project_link(event: dict[str, Any]) -> Effect:
    """An assignment event becomes the report's link, with its reason."""
    return Upsert(
        model_label=LINK,
        lookup={"report_key": event["report"]},
        defaults={"registration": event["registration"], "because": event["because"]},
    )


# -- step 4: totals ----------------------------------------------------------


def report_totals(reader: Any) -> list[Effect]:
    """Every report's total is the sum of its lines."""
    return [
        Update(
            model_label=REPORT,
            lookup={"key": report.key},
            defaults={
                "total": sum(
                    line.cost for line in reader.filter(LINE, report_key=report.key)
                )
            },
        )
        for report in reader.query(REPORT)
    ]


# -- step 5: month_ready -----------------------------------------------------


def month_readiness(reader: Any) -> list[Effect]:
    """A suku's month is ready to close when every report in it is approved,
    linked to a project, and within that project's budget."""
    reasons: dict[tuple[str, str], list[str]] = {}
    for report in reader.query(REPORT):
        found = reasons.setdefault((report.suku, report.period), [])
        status = reader.get(STATUS, report_key=report.key)
        link = reader.get(LINK, report_key=report.key)
        project = reader.get(PROJECT, registration=link.registration) if link else None
        if status is None or status.status != "approved":
            found.append(f"{report.key} not approved")
        if project is None:
            found.append(f"{report.key} has no project")
        elif report.total > project.budget:
            found.append(f"{report.key} over budget")
    return [
        Upsert(
            model_label=READINESS,
            lookup={"suku": suku, "period": period},
            defaults={"ready": not found, "reasons": "; ".join(found)},
        )
        for (suku, period), found in sorted(reasons.items())
    ]


# -- step 6: notify_coordinator ----------------------------------------------


def approval_email(event: dict[str, Any], reader: Any) -> ExternalEffect | None:  # noqa: ARG001
    """An approved report sends the coordinator an email.

    It is an `ExternalEffect`, so rakaia never sends it: a replay hands it back
    in `ReplayResult.external` and the caller decides. A save sends it; a
    rebuild does not.
    """
    if event["status"] != "approved":
        return None
    return ExternalEffect(
        kind="email",
        payload={"to": "coordinator", "subject": f"{event['report']} approved"},
    )


# -- the list ----------------------------------------------------------------


@dataclass(frozen=True)
class Skip:
    """Why a step does not run the same way on a rebuild.

    The kinds are partisipa-import's: ``not_needed``, work that means nothing
    on a rebuild, and ``not_reproduced``, a result the rebuild leaves out on
    purpose. Its third, ``done_elsewhere``, is not used here and is left out.
    """

    kind: Literal["not_needed", "not_reproduced"]
    reason: str


@dataclass(frozen=True)
class Handler:
    """One handler a step registers: it runs on every event of `form_type`."""

    form_type: str
    fn: Callable[..., Any]
    stage: int = 0


@dataclass(frozen=True)
class Reducer:
    """One reducer a step registers: it runs once per pass, after `stage`."""

    fn: Callable[..., Any]
    stage: int


@dataclass(frozen=True)
class Step:
    name: str
    does: str
    handlers: tuple[Handler, ...] = ()
    reducers: tuple[Reducer, ...] = field(default=())
    decide: Callable[[dict[str, Any], Any], list[dict[str, Any]]] | None = None
    """Save-only work: events to record after a form is saved, from what the
    tables say at that moment. A step with one must say why a rebuild skips it."""
    on_rebuild: Skip | None = None
    """``None`` means a rebuild runs this step exactly as a save does."""

    def __post_init__(self) -> None:
        if self.decide is not None and self.on_rebuild is None:
            raise ValueError(
                f"step {self.name!r} decides on a save; say why a rebuild does not"
            )


STEPS: tuple[Step, ...] = (
    Step(
        name="form_rows",
        does="Each form becomes rows: a project, or a report and its lines.",
        handlers=(Handler("PROJECT", project_row), Handler("PROGRESS", report_rows)),
    ),
    Step(
        name="status",
        does="Each review decision becomes the report's status.",
        handlers=(Handler("STATUS", status_row),),
    ),
    Step(
        name="project_links",
        does="A save assigns each report to the registration for its suku and output.",
        handlers=(Handler("ASSIGNED", project_link),),
        decide=assignments,
        on_rebuild=Skip(
            "not_needed",
            "a rebuild replays the assignments the saves recorded; it decides nothing",
        ),
    ),
    Step(
        name="totals",
        does="Each report's total is added up from its lines.",
        reducers=(Reducer(report_totals, stage=1),),
    ),
    Step(
        name="month_ready",
        does="Each suku's month is marked ready to close, or says why not.",
        reducers=(Reducer(month_readiness, stage=2),),
    ),
    Step(
        name="notify_coordinator",
        does="An approved report emails the coordinator.",
        handlers=(Handler("STATUS", approval_email, stage=3),),
        on_rebuild=Skip(
            "not_reproduced",
            "the email went out when the report was approved; "
            "a rebuild must not send it again",
        ),
    ),
)

STEP_NAMES: tuple[str, ...] = tuple(step.name for step in STEPS)


def steps_until(last: str | None = None) -> tuple[Step, ...]:
    """The steps from the first up to and including `last` (all when ``None``)."""
    if last is None:
        return STEPS
    if last not in STEP_NAMES:
        raise ValueError(
            f"no step named {last!r}; the steps are {', '.join(STEP_NAMES)}"
        )
    return STEPS[: STEP_NAMES.index(last) + 1]


def build_registry(steps: tuple[Step, ...] = STEPS) -> HandlerRegistry:
    """A fresh registry holding the handlers and reducers of `steps`."""
    registry = HandlerRegistry()
    for step in steps:
        for handler in step.handlers:
            registry.register(
                f"{step.name}.{handler.fn.__name__}",
                handler.form_type,
                handler.fn,
                effective_from=0,
                match_field="form_type",
                stage=handler.stage,
            )
        for reducer in step.reducers:
            registry.register_reducer(
                f"{step.name}.{reducer.fn.__name__}", reducer.stage, reducer.fn
            )
    return registry
