"""
`manage.py check_stream_coverage` — check every stream named in
``RAKAIA_COVERAGE_CHECKS`` against the table it is written from.
"""

from __future__ import annotations

import json
from typing import Any

from django.apps import apps
from django.conf import settings
from django.core.exceptions import FieldDoesNotExist, FieldError
from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db.models import QuerySet
from django.utils.module_loading import import_string

from django_rakaia.coverage import StreamCoverage, stream_coverage

_ENTRY_KEYS = frozenset(
    {
        "model",
        "filter",
        "queryset",
        "stream_path",
        "subject_key",
        "row_key",
        "changed_field",
    }
)


def _queryset_for(index: int, entry: dict[str, Any]) -> QuerySet[Any]:
    """The queryset an entry names, by model label or by a factory's dotted path."""
    where = f"RAKAIA_COVERAGE_CHECKS[{index}]"
    if ("model" in entry) == ("queryset" in entry):
        raise CommandError(
            f"{where} must name exactly one of 'model' (an app label such as "
            "'forms.Submission') or 'queryset' (the dotted path of a function "
            "returning one)."
        )
    if "queryset" in entry:
        if "filter" in entry:
            raise CommandError(
                f"{where} gives both 'queryset' and 'filter'. Filter inside the "
                "queryset function instead."
            )
        try:
            factory = import_string(entry["queryset"])
        except ImportError as exc:
            raise CommandError(f"{where}: cannot import {entry['queryset']!r}: {exc}")
        queryset = factory()
        if not isinstance(queryset, QuerySet):
            raise CommandError(
                f"{where}: {entry['queryset']!r} returned "
                f"{type(queryset).__name__}, not a QuerySet."
            )
        return queryset
    try:
        model = apps.get_model(entry["model"])
    except (LookupError, ValueError) as exc:
        raise CommandError(f"{where}: no model {entry['model']!r}: {exc}")
    return model._default_manager.filter(**entry.get("filter", {}))


def _entries() -> list[tuple[QuerySet[Any], dict[str, Any]]]:
    """Every configured entry, validated before any of them is run."""
    configured = getattr(settings, "RAKAIA_COVERAGE_CHECKS", None)
    if not configured:
        raise CommandError(
            "RAKAIA_COVERAGE_CHECKS is empty or not set, so there is nothing to "
            "check. Add an entry for each stream written from a table."
        )
    resolved = []
    for index, entry in enumerate(configured):
        where = f"RAKAIA_COVERAGE_CHECKS[{index}]"
        unknown = set(entry) - _ENTRY_KEYS
        if unknown:
            raise CommandError(
                f"{where} has unknown keys {sorted(unknown)}; "
                f"expected some of {sorted(_ENTRY_KEYS)}."
            )
        for required in ("stream_path", "subject_key"):
            if not entry.get(required):
                raise CommandError(f"{where} is missing {required!r}.")
        fields = [entry.get("row_key", "pk")]
        if entry.get("changed_field"):
            fields.append(entry["changed_field"])
        try:
            # Building the filter and naming the columns raises for a misspelt
            # lookup or field without running or compiling a query, so a
            # queryset that can never match (`.none()`) still passes.
            queryset = _queryset_for(index, entry)
            queryset.order_by(fields[0]).values_list(*fields)
        except (FieldError, FieldDoesNotExist) as exc:
            raise CommandError(f"{where}: {exc}") from exc
        options = {
            "stream_path": entry["stream_path"],
            "subject_key": entry["subject_key"],
            "row_key": entry.get("row_key", "pk"),
            "changed_field": entry.get("changed_field"),
        }
        resolved.append((queryset, options))
    return resolved


def _line(report: StreamCoverage) -> str:
    stale = "-" if report.stale is None else str(report.stale)
    line = (
        f"{'ok ' if report.ok else 'GAP'} {report.stream_path}: "
        f"rows={report.rows} covered={report.covered} missing={report.missing} "
        f"stale={stale} extra={report.extra} skipped={report.skipped}"
    )
    for name in ("missing_sample", "stale_sample", "extra_sample"):
        sample = getattr(report, name)
        if sample:
            line += f"\n    {name}: {', '.join(sample)}"
    return line


class Command(BaseCommand):
    """Run every entry in ``RAKAIA_COVERAGE_CHECKS`` and fail if any has a gap.

    One line per stream, or a JSON list with ``--json``. The exit status is
    non-zero when any entry has a missing or a stale row, so a nightly timer can
    alert on it; events whose row has gone (``extra``) are reported and do not
    fail it. Every entry's settings keys, model, filter and field names are
    checked before any is run, so a typo in the last one is not discovered after
    the first has already taken its time.
    """

    help = "Check that each configured stream has an event for every row of its table."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--json",
            action="store_true",
            help="Print the reports as a JSON list instead of one line per stream.",
        )

    def handle(self, *args: Any, **options: Any) -> None:  # noqa: ARG002
        reports = [stream_coverage(queryset, **entry) for queryset, entry in _entries()]

        if options["json"]:
            self.stdout.write(json.dumps([r.as_dict() for r in reports], indent=2))
        else:
            for report in reports:
                style = self.style.SUCCESS if report.ok else self.style.ERROR
                self.stdout.write(style(_line(report)))

        gaps = [r.stream_path for r in reports if not r.ok]
        if gaps:
            raise CommandError(
                f"{len(gaps)} of {len(reports)} streams do not cover their table: "
                + ", ".join(gaps)
            )
