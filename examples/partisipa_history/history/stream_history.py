"""Deriving the audit log from the stream instead of from pghistory.

This is the heart of the spike. ``append_saves`` writes each save as an envelope
event. ``replay_history`` rebuilds *two* projections from that one stream via
rakaia effects:

* ``SubmissionRecord`` — current state (the Submissions-table replacement),
  ``update_or_create`` per live row, ``delete`` when a submission is deleted.
* ``SubmissionHistoryEntry`` — the audit log, one ``update_or_create`` per event
  keyed by ``(submission_id, seq)`` so re-replay is idempotent by construction.

Both are ordinary rakaia ``Effect``s applied through ``DjangoExecutor`` — the
same delete op shipped in #6 — so nothing here is bespoke to history.

``naive_append`` / ``naive_history`` are the strawman: a store that only ever
carried ``fields`` (a plain ``append(new_state)``). Its "history" cannot name an
actor or distinguish a create from an update, which is precisely the gap the
envelope closes.

``recover_peak_snapshot`` is the stream-native ``repair_blank_save_dataloss``:
the pre-truncation snapshot never left the log, so recovery is one query.
"""

from __future__ import annotations

import json
from typing import Any

from django_rakaia.effect_executor import DjangoExecutor
from rakaia.effects import Effect

from .envelope import OP_TO_LABEL, canonical, make_event
from .models import SubmissionHistoryEntry

RECORD_MODEL = "history.SubmissionRecord"
HISTORY_MODEL = "history.SubmissionHistoryEntry"


# -- writing the stream ------------------------------------------------------


def append_saves(store: Any, stream: str, saves: list[dict[str, Any]]) -> None:
    """Write each save to the stream as a full envelope event."""
    for save in saves:
        store.append(stream, json.dumps(make_event(save)).encode("utf-8"))


def naive_append(store: Any, stream: str, saves: list[dict[str, Any]]) -> None:
    """The strawman: append only the fields — no actor, no op, no timestamp."""
    for save in saves:
        payload = {"key": save["key"], "fields": save["fields"]}
        store.append(stream, json.dumps(payload).encode("utf-8"))


# -- replaying into projections ---------------------------------------------


def replay_history(store: Any, stream: str) -> None:
    """Rebuild both projections from the enveloped stream, via rakaia effects.

    Effects are applied per event, in stream order — the same fold rakaia's
    ``replay()`` performs. The append-only audit row (keyed by seq) and the
    last-write-wins current-state row therefore never collide within a batch.
    """
    messages, _ = store.read(stream)
    executor = DjangoExecutor()
    for seq, msg in enumerate(messages):
        event = json.loads(msg.data)
        executor.apply([_history_effect(seq, event), _record_effect(event)])


def _history_effect(seq: int, event: dict[str, Any]) -> Effect:
    """One append-only audit row per event; idempotent on (submission_id, seq)."""
    return Effect(
        op="update_or_create",
        model_label=HISTORY_MODEL,
        lookup={"submission_id": event["key"], "seq": seq},
        defaults={
            "label": OP_TO_LABEL[event["op"]],
            "actor": event["actor"],
            "ts": event["ts"],
            "fields": event["fields"],
        },
    )


def _record_effect(event: dict[str, Any]) -> Effect:
    """Current-state upsert, or a delete when the submission is removed."""
    if event["op"] == "delete":
        return Effect(
            op="delete",
            model_label=RECORD_MODEL,
            lookup={"submission_id": event["key"]},
        )
    return Effect(
        op="update_or_create",
        model_label=RECORD_MODEL,
        lookup={"submission_id": event["key"]},
        defaults={
            "fields": event["fields"],
            "actor": event["actor"],
            "updated_at": event["ts"],
        },
    )


def naive_history(store: Any, stream: str) -> list[dict[str, Any]]:
    """What a fields-only store can reconstruct: no actor, no label."""
    messages, _ = store.read(stream)
    return [json.loads(m.data) for m in messages]


# -- recovery ----------------------------------------------------------------


def recover_peak_snapshot(submission_id: str) -> dict[str, Any]:
    """`repair_blank_save_dataloss`, stream edition — restore the peak snapshot.

    The pre-truncation snapshot is still an audit row, so recovery is a query
    for the historical snapshot with the most fields.

    Caveat: "most fields" is the same proxy the real ``repair_blank_save_dataloss``
    uses for "peak", and it inherits the same limitation — it misfires for a
    submission that legitimately *shrinks* over time, or a partial save that
    leaves several junk keys. It is a recovery heuristic, not a correctness
    guarantee; a production port would prefer an explicit "last known good"
    marker on the envelope over a key-count argmax.
    """
    rows = SubmissionHistoryEntry.objects.filter(submission_id=submission_id)
    return max((r.fields for r in rows), key=len, default={})


def snapshots_equal(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """Byte-for-byte snapshot comparison (canonical JSON)."""
    return canonical(a) == canonical(b)
