"""`manage.py demo_pipeline [--until STEP]` — one suku's month, step by step.

Saves seven forms one at a time, the way they would arrive in real use, and
shows the tables after each save. Then it rebuilds every table from the log into
an empty database and compares the two. Finally it changes one live row by hand
to show the comparison catching it.

``--until STEP`` runs only the steps up to and including STEP, so each explainer
page can show its own step on its own. The rebuild and the comparison run only
when every step is on.

Each claim is checked, and a failed one raises `CommandError`, so the command
exits non-zero when the example stops being true.
"""

from __future__ import annotations

import json
from typing import Any

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError, CommandParser

from django_rakaia import (
    GREEN,
    RED,
    DjangoExecutor,
    DjangoProjectionReader,
    get_store,
    rebuild_and_verify,
)
from pipeline.forms import (
    FORMS,
    ROAD,
    ROAD_REGISTRATION,
    STREAM,
    WATER,
    WATER_REGISTRATION,
)
from pipeline.models import (
    MonthReadiness,
    ProgressLine,
    ProgressReport,
    Project,
    ProjectLink,
    ReportStatus,
)
from pipeline.routes import not_on_a_rebuild, save_form
from pipeline.steps import STEP_NAMES, build_registry, steps_until
from rakaia import ExternalEffect

TABLES = (
    Project,
    ProgressReport,
    ProgressLine,
    ProjectLink,
    ReportStatus,
    MonthReadiness,
)
SCRATCH = "scratch"


class Command(BaseCommand):
    help = "Save one suku's forms through the pipeline, then rebuild and compare."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--until",
            choices=STEP_NAMES,
            help="Run the steps up to and including this one.",
        )

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        call_command("migrate", verbosity=0, interactive=False)
        call_command("migrate", database=SCRATCH, verbosity=0, interactive=False)

        steps = steps_until(opts["until"])
        self.on = {step.name for step in steps}
        registry = build_registry(steps)
        store = get_store()
        store.delete(STREAM)
        for model in TABLES:
            model.objects.all().delete()

        self.say("The steps, in order:")
        for number, step in enumerate(steps, start=1):
            self.say(f"  {number}. {step.name:<20} {step.does}")
        skipped = [name for name in STEP_NAMES if name not in self.on]
        if skipped:
            self.say(f"  (not running: {', '.join(skipped)})")

        outbox: list[ExternalEffect] = []
        for number, (what, form) in enumerate(FORMS, start=1):
            sent = len(outbox)
            recorded = save_form(
                store,
                STREAM,
                form,
                registry,
                executor=DjangoExecutor(),
                reader=DjangoProjectionReader(),
                outbox=outbox,
                steps=steps,
            )
            self.say(f"\n[{number}] Saved: {what}.")
            for event in recorded:
                self.say(
                    f"    recorded: {event['report']} assigned to "
                    f"{event['registration']}, because it {event['because']}"
                )
            self.show_reports()
            for email in outbox[sent:]:
                self.say(
                    f"    email to {email.payload['to']}: {email.payload['subject']}"
                )
            self.check_after(number, outbox)

        self.say("\nThe tables now:")
        self.show_tables()

        if len(steps) < len(STEP_NAMES):
            self.say(
                "\nRebuild skipped: it compares a rebuild of every step with live, "
                "so it needs every step on."
            )
        else:
            self.rebuild_and_compare(registry)

        self.stdout.write(self.style.SUCCESS("\nEvery check passed ✓"))

    # -- the checks ----------------------------------------------------------

    def check_after(self, number: int, outbox: list[ExternalEffect]) -> None:
        if number == 1 and "project_links" in self.on:
            self.claim(
                link(WATER) == "",
                "the water report waits unlinked: no registration matches it yet",
            )
        if number == 3 and "project_links" in self.on:
            self.claim(
                link(ROAD) == ROAD_REGISTRATION,
                "the road report is assigned at once: its registration is here",
            )
        if number == 3 and "totals" in self.on:
            self.claim(report(ROAD).total == 2200, "the road report adds up to 2200")
        if number == 3 and "month_ready" in self.on:
            self.claim(
                "road-2026-03 over budget" in month().reasons,
                "the month is not ready: the road report is over budget",
            )
        if number == 4 and "project_links" in self.on:
            self.claim(
                link(WATER) == WATER_REGISTRATION,
                "the save that brought the late registration assigned the waiting report",
            )
        if number == 7 and "project_links" in self.on:
            assigned = [
                json.loads(m.data)
                for m in get_store().read(STREAM)[0]
                if json.loads(m.data)["form_type"] == "ASSIGNED"
            ]
            self.claim(
                [e["report"] for e in assigned] == [ROAD, WATER],
                "the log holds both assignments, each with its reason",
            )
        if number == 5:
            self.claim(
                ProgressLine.objects.filter(report_key=ROAD).count() == 2,
                "the line taken out of the resubmitted road report is gone",
            )
        if number == 5 and "totals" in self.on:
            self.claim(report(ROAD).total == 1600, "the road total is now 1600")
        if number == 7 and "month_ready" in self.on:
            self.claim(month().ready, "every report is approved: the month is ready")
        if number == 7 and "notify_coordinator" in self.on:
            self.claim(len(outbox) == 2, "the coordinator got one email per approval")

    def rebuild_and_compare(self, registry: Any) -> None:
        self.say("\nRebuild: replay the whole log into an empty database.")
        for step in not_on_a_rebuild():
            assert step.on_rebuild is not None
            self.say(
                f"  not on a rebuild: {'deciding in ' if step.decide else ''}{step.name} "
                f"({step.on_rebuild.kind.replace('_', ' ')}: {step.on_rebuild.reason})"
            )
        verdict = self.compare(registry)
        self.claim(
            verdict.verdict == GREEN, "the rebuild matches live, table for table"
        )

        self.say("\nNow change one live row by hand, the way a stray script might.")
        ProgressReport.objects.filter(key=WATER).update(total=9999)
        verdict = self.compare(registry)
        self.claim(verdict.verdict == RED, "the comparison notices")
        for problem in verdict.problems:
            self.say(f"  {problem}")
        self.claim(
            any(d.field == "total" for p in verdict.problems for d in p.field_diffs),
            "and names the field that is wrong",
        )
        ProgressReport.objects.filter(key=WATER).update(total=2000)

    def compare(self, registry: Any) -> Any:
        for model in TABLES:
            model.objects.using(SCRATCH).all().delete()
        result = rebuild_and_verify(
            STREAM, into=SCRATCH, live_models=TABLES, registry=registry
        )
        self.say(f"  compared {result.compared} rows: {result.verdict}")
        return result

    # -- output --------------------------------------------------------------

    def say(self, text: str) -> None:
        self.stdout.write(text)

    def claim(self, holds: bool, text: str) -> None:
        if not holds:
            raise CommandError(f"Expected: {text}")
        self.say(f"    ✓ {text}")

    def show_reports(self) -> None:
        for row in ProgressReport.objects.all():
            status = ReportStatus.objects.filter(report_key=row.key).first()
            project = project_name(link(row.key))
            self.say(
                f"    {row.key:<26} total {row.total:>5}  "
                f"project {project:<30} "
                f"{status.status if status else 'submitted'}"
            )
        for month_row in MonthReadiness.objects.all():
            state = "READY" if month_row.ready else f"not ready: {month_row.reasons}"
            self.say(f"    month {month_row.suku} {month_row.period}: {state}")

    def show_tables(self) -> None:
        for project in Project.objects.all():
            self.say(
                f"  project {project.name} ({project.output}, budget {project.budget}), "
                f"created by {project.registration}"
            )
        for line in ProgressLine.objects.all():
            self.say(
                f"  line    {line.report_key} #{line.line_index} {line.item} {line.cost}"
            )
        self.show_reports()


def report(key: str) -> ProgressReport:
    return ProgressReport.objects.get(key=key)


def link(report_key: str) -> str:
    row = ProjectLink.objects.filter(report_key=report_key).first()
    return row.registration if row else ""


def project_name(registration: str) -> str:
    """The name of the project a link points at, or ``(none)``."""
    if not registration:
        return "(none)"
    return Project.objects.get(registration=registration).name


def month() -> MonthReadiness:
    return MonthReadiness.objects.get(suku="Fatuberliu", period="2026-03")
