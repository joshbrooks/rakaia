"""The converged write/read model (RFC #22, Decision #13).

`record_submission` is the single sanctioned write path — a "save" that
**appends a `SubmissionEvent` and reprojects `Submission` in one transaction**.
`Submission` is never written directly; it is a projection of the event log,
rebuilt by `reproject_all`. `/history` is the ordered event log, materialised
into an audit table.

Both read-side steps are thin adapters over the shipped core helpers —
`rakaia.project_latest` (latest-snapshot-per-subject projection) and
`django_rakaia.materialize_history` (the `/history` materialiser) — so the
example carries no hand-rolled fold of its own; it exercises the real library
API an adopter would use.

The event log itself is rakaia's durable `StreamEvent`/`StreamEntry`
(`DjangoStreamStore`), so append participates in the ambient transaction —
which is what makes the same-transaction guarantee (Decision #11) structural and
the append/projection pair atomic.
"""

from __future__ import annotations

import json
from typing import Any

from django.db import transaction

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.history import materialize_history as _materialize_history
from rakaia import (
    AppendOptions,
    envelope_actor,
    label_marker,
    project_latest,
    provenance,
)

STREAM = "submissions"
SUBMISSION_MODEL = "submission_stream.Submission"
HISTORY_MODEL = "submission_stream.SubmissionHistory"


def get_store() -> DjangoStreamStore:
    return DjangoStreamStore()


def record_submission(
    store: DjangoStreamStore,
    key: str,
    *,
    fields: dict[str, Any],
    status: int,
    actor: str | None = None,
    url: str | None = None,
    label: str = "update",
) -> None:
    """Append one `SubmissionEvent`, then reproject `Submission` — one atomic.

    Provenance (`actor`/`url`) rides the envelope via `provenance(...)`, which
    `append()` already merges into the event's metadata — so there is no
    `post_save` signal in the loop (that path would miss bulk writes; Decision
    #13). `status` is carried top-level in the envelope metadata (Decision #12),
    not buried in the form `fields`.
    """
    ambient = {k: v for k, v in (("user", actor), ("url", url)) if v is not None}
    payload = {"key": key, "fields": fields, "status": status}
    with transaction.atomic():
        with provenance(**ambient):
            store.append(
                STREAM,
                json.dumps(payload).encode("utf-8"),
                AppendOptions(label=label, metadata={"status": status}),
            )
        reproject_all(store)


def reproject_all(store: DjangoStreamStore) -> None:
    """Rebuild `Submission` from the event log via `rakaia.project_latest`: one
    idempotent upsert per live key's latest snapshot, plus a delete per
    tombstoned key (Decision #2 — a `delete` event). `Submission` is a pure
    function of `SubmissionEvent`; ``version`` is the durable offset, so this
    reprojection agrees with a history keyed by ``int(m.offset)``.
    """
    messages, _ = store.read(STREAM)
    effects = project_latest(
        messages,
        SUBMISSION_MODEL,
        subject_field="key",
        subject_of=lambda e: e["key"],
        defaults_of=lambda msg, event: {
            "fields": event["fields"],
            "status": event["status"],
            "user": envelope_actor(msg, event),
            "version": int(msg.offset),
        },
    )
    DjangoExecutor().apply(effects)


def materialize_history(store: DjangoStreamStore) -> None:
    """`/history` = the ordered event log, materialised by the shipped
    `django_rakaia.materialize_history` (one row per event, keyed by
    ``(key, offset)`` so re-running is a no-op)."""
    _materialize_history(
        store,
        STREAM,
        HISTORY_MODEL,
        subject_field="key",
        subject_of=lambda e: e["key"],
        version_of=lambda m: int(m.offset),
        defaults_of=lambda msg, event: {
            "marker": label_marker(msg.label),
            "actor": envelope_actor(msg, event),
            "url": (msg.metadata or {}).get("url"),
            "status": event["status"],
            "snapshot": event,
            "ts": msg.timestamp,
        },
    )
