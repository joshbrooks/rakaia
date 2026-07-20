"""The converged write/read model (RFC #22, Decision #13).

`record_submission` is the single sanctioned write path — a "save" that
**appends a `SubmissionEvent` and reprojects `Submission` in one transaction**.
`Submission` is never written directly; it is a projection of the event log,
rebuilt by `reproject_all`. `/history` is the ordered event log, fanned into an
audit table by the shipped `rakaia.history_effects`.

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
from rakaia import (
    AppendOptions,
    envelope_actor,
    history_effects,
    label_marker,
    provenance,
)

from .models import Submission, SubmissionHistory

STREAM = "submissions"
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


def _latest_by_key(store: DjangoStreamStore) -> dict[str, tuple[int, dict, Any]]:
    """Fold the log to the newest event per key. Oldest-first read + last-wins =
    latest snapshot; the index is the (never-renumbered) stream version."""
    messages, _ = store.read(STREAM)
    latest: dict[str, tuple[int, dict, Any]] = {}
    for version, msg in enumerate(messages):
        event = json.loads(msg.data)
        latest[event["key"]] = (version, event, msg)
    return latest


def reproject_all(store: DjangoStreamStore) -> None:
    """Rebuild every `Submission` row from the event log — `Submission` is a pure
    function of `SubmissionEvent`. (Spike: full rebuild for clarity; real code
    would project incrementally via replay handlers.)"""
    latest = _latest_by_key(store)
    Submission.objects.all().delete()
    for key, (version, event, msg) in latest.items():
        Submission.objects.create(
            key=key,
            fields=event["fields"],
            status=event["status"],
            user=envelope_actor(msg, event),
            version=version,
        )


def materialize_history(store: DjangoStreamStore) -> None:
    """`/history` = the ordered event log, fanned into an audit table by the
    shipped `history_effects` helper (one row per event, keyed by (key,
    version))."""
    messages, _ = store.read(STREAM)
    effects = history_effects(
        messages,
        HISTORY_MODEL,
        subject_field="key",
        subject_of=lambda e: e["key"],
        defaults_of=lambda msg, event: {
            "marker": label_marker(msg.label),
            "actor": envelope_actor(msg, event),
            "url": (msg.metadata or {}).get("url"),
            "status": event["status"],
            "snapshot": event,
            "ts": msg.timestamp,
        },
    )
    SubmissionHistory.objects.all().delete()
    DjangoExecutor().apply(effects)
