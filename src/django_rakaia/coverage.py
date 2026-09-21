"""Check that a table and the stream written from it still agree.

`stream_coverage` compares the rows of a queryset with the events in one durable
stream and reports rows no event names (``missing``), rows changed after their
newest event (``stale``), and subjects whose row is gone (``extra``). It exists
because a stream can quietly stop receiving events while the table keeps
growing, and nothing else notices until a full rebuild comes up short.

    from django.db.models import Max
    from django_rakaia import stream_coverage

    report = stream_coverage(
        Submission.objects.filter(form="tf611").annotate(
            last_changed=Max("events__pgh_created_at")
        ),
        "submissions/tf611",
        subject_key="submission",
        changed_field="last_changed",
    )
    assert report.ok, report

`manage.py check_stream_coverage` runs the same check for every entry in the
``RAKAIA_COVERAGE_CHECKS`` setting.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from django.core.exceptions import FieldDoesNotExist
from django.db import connections, transaction
from django.db.models import Max, Model, Q, QuerySet, TextField, UUIDField
from django.db.models.fields.json import KeyTextTransform, KeyTransform
from django.db.models.functions import Cast

from .models import StreamEvent

#: The most keys any one sample holds. A sample is a place to start looking,
#: not an export, and the table it is drawn from can have millions of rows.
SAMPLE_LIMIT = 10


@dataclass(frozen=True)
class StreamCoverage:
    """What `stream_coverage` found for one queryset and one stream.

    Samples hold keys as text, at most `SAMPLE_LIMIT` of them: missing and stale
    rows in the queryset's key order, extra subjects sorted as text.
    ``stale`` is ``None`` when no change time was given, so "not checked" never
    reads as "none found". ``extra`` counts distinct subjects, not events, and
    does not affect `ok`: a deleted row leaves its events behind by design.
    """

    stream_path: str
    rows: int
    covered: int
    missing: int
    missing_sample: tuple[str, ...]
    stale: int | None
    stale_sample: tuple[str, ...]
    extra: int
    extra_sample: tuple[str, ...]
    skipped: int

    @property
    def ok(self) -> bool:
        """Every row has an event, and none has changed since its newest one."""
        return self.missing == 0 and not self.stale

    def as_dict(self) -> dict[str, Any]:
        """The report as plain JSON-ready values, with `ok` included."""
        data = asdict(self)
        for name in ("missing_sample", "stale_sample", "extra_sample"):
            data[name] = list(data[name])
        data["ok"] = self.ok
        return data


def _is_uuid_key(model: type[Model], row_key: str) -> bool:
    """Whether ``row_key`` is a UUID column, whose text form differs by backend."""
    if row_key == "pk":
        return isinstance(model._meta.pk, UUIDField)
    try:
        return isinstance(model._meta.get_field(row_key), UUIDField)
    except FieldDoesNotExist:
        return False


def _as_epoch(value: Any) -> float | None:
    """A change time or event time as Unix seconds, the unit of ``event_ts``."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, int | float):
        return float(value)
    raise TypeError(
        f"stream_coverage compares change times as datetimes or Unix seconds, "
        f"not {type(value).__name__} ({value!r})."
    )


def stream_coverage(
    queryset: QuerySet[Any],
    stream_path: str,
    *,
    subject_key: str,
    row_key: str = "pk",
    changed_field: str | None = None,
) -> StreamCoverage:
    """Report how completely the stream at ``stream_path`` covers ``queryset``.

    An event is about a row when its payload's ``subject_key`` equals the row's
    ``row_key``, compared as text, so ``{"submission": 7}`` and
    ``{"submission": "7"}`` both name pk 7. ``changed_field`` names a datetime
    the queryset exposes — a column, or any annotation on it, such as
    ``Max("events__pgh_created_at")`` over a history table — and a row is stale
    when it is later than the row's newest event time (``event_ts``, falling back
    to the event row's ``created_at``). Events with a non-JSON
    ``payload_encoding``, and payloads whose ``subject_key`` is JSON ``null``,
    are not counted as subjects; the former are counted in ``skipped``.

    Three read-only queries on ``queryset.db``, whatever the size of the table:
    the database extracts each subject and groups the stream by it, the rows are
    streamed once in key order, and the two are matched in Python. Payloads are
    never decoded in Python and nothing is written. Memory grows with the number
    of distinct subjects, not rows. One stream per call: a table fanned out into
    one stream per row is out of scope. The three queries read one snapshot: they
    run in a transaction on ``queryset.db``, read-only and REPEATABLE READ on
    Postgres, so a row and its event committed mid-check cannot show as missing.
    Inside a caller's transaction the caller's isolation level applies instead.
    """
    db = queryset.db
    connection = connections[db]
    outermost = not connection.in_atomic_block
    with transaction.atomic(using=db):
        if outermost and connection.vendor == "postgresql":
            # Must be the transaction's first statement, which is why only the
            # outermost block can set it.
            with connection.cursor() as cursor:
                cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
        return _measure(queryset, db, stream_path, subject_key, row_key, changed_field)


def _measure(
    queryset: QuerySet[Any],
    db: str,
    stream_path: str,
    subject_key: str,
    row_key: str,
    changed_field: str | None,
) -> StreamCoverage:
    uuid_key = _is_uuid_key(queryset.model, row_key)

    def key_text(value: Any) -> str:
        # A UUID reaches Python as a `UUID` from the column and as hyphenated
        # text from the payload; compare both as bare hex.
        if uuid_key and value is not None:
            try:
                return uuid.UUID(str(value)).hex
            except ValueError:
                pass
        return str(value)

    in_stream = (
        StreamEvent.objects.using(db)
        .filter(entries__stream__stream_id=stream_path)
        .order_by()
    )
    subjects = (
        in_stream.filter(payload_encoding__isnull=True)
        .annotate(_subject=Cast(KeyTextTransform(subject_key, "data"), TextField()))
        .filter(_subject__isnull=False)
        # A JSON null names no row. SQLite's text extraction renders it as
        # "null" where Postgres gives SQL NULL, so exclude it explicitly, with a
        # transform that takes the key literally as the extraction does; a
        # `data__<key>` lookup string would parse `a__b` or `contains` instead.
        .alias(_raw=KeyTransform(subject_key, "data"))
        .exclude(_raw=None)
        .values("_subject")
        .annotate(
            newest_ts=Max("event_ts"),
            newest_created=Max("created_at", filter=Q(event_ts__isnull=True)),
        )
    )
    newest: dict[str, float] = {}
    shown: dict[str, str] = {}
    for subject in subjects:
        key = key_text(subject["_subject"])
        # Each event's time is `event_ts`, or `created_at` where that is null;
        # the newest of those is the newer of the two per-subject maxima.
        latest = max(
            t
            for t in (
                _as_epoch(subject["newest_ts"]),
                _as_epoch(subject["newest_created"]),
            )
            if t is not None
        )
        newest[key] = max(latest, newest.get(key, latest))
        shown.setdefault(key, str(subject["_subject"]))

    fields = [row_key] if changed_field is None else [row_key, changed_field]
    rows = missing = stale = 0
    missing_sample: list[str] = []
    stale_sample: list[str] = []
    matched: set[str] = set()
    for values in (
        queryset.order_by(row_key).values_list(*fields).iterator(chunk_size=2000)
    ):
        rows += 1
        key = key_text(values[0])
        newest_event = newest.get(key)
        if newest_event is None:
            missing += 1
            if len(missing_sample) < SAMPLE_LIMIT:
                missing_sample.append(str(values[0]))
            continue
        matched.add(key)
        if changed_field is None:
            continue
        changed = _as_epoch(values[1])
        if changed is not None and changed > newest_event:
            stale += 1
            if len(stale_sample) < SAMPLE_LIMIT:
                stale_sample.append(str(values[0]))

    orphaned = sorted(shown[key] for key in newest.keys() - matched)
    skipped = in_stream.filter(payload_encoding__isnull=False).distinct().count()

    return StreamCoverage(
        stream_path=stream_path,
        rows=rows,
        covered=rows - missing,
        missing=missing,
        missing_sample=tuple(missing_sample),
        stale=None if changed_field is None else stale,
        stale_sample=tuple(stale_sample),
        extra=len(orphaned),
        extra_sample=tuple(orphaned[:SAMPLE_LIMIT]),
        skipped=skipped,
    )
