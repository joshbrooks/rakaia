"""
History read-model: materialise a per-event audit log from an enveloped stream.

This is the streams-native replacement for `django-pghistory`'s consumers — the
`/history` audit API, the admin event log, and blank-save recovery. Because a
stream carries the event **envelope** (label + metadata) on each message
(PR A), a history projection is just another fan-out: one audit row per event,
keyed by ``(subject, version)``, carrying the label, timestamp, actor, metadata,
and the full payload snapshot.

`history_effects` handles the iteration + keying and leaves the row *shape* to
the caller (a `defaults_of(msg, event)` callback), so the audit model can match
whatever `/history` returns. Two convenience helpers cover the two fiddly bits
the audit consumers need: `label_marker` (label → ``+``/``~``/``-``) and
`envelope_actor` (the ``metadata['user']`` editor, falling back to the payload's
own owner FK). `recover_peak_snapshot` is the `repair_blank_save_dataloss`
analogue — recover a truncated subject from its historical peak.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from typing import Any

from .effects import Effect
from .types import StreamMessage


def label_marker(label: str) -> str:
    """Map an envelope label to the `/history` diff marker `+` / `~` / `-`.

    ``insert``/``create`` → ``+``, ``delete`` → ``-``, everything else (incl.
    ``update`` and the empty raw-append label) → ``~`` — matching pghistory's
    ``_label_to_type``.
    """
    if label in ("insert", "create"):
        return "+"
    if label == "delete":
        return "-"
    return "~"


def envelope_actor(
    msg: StreamMessage, event: dict[str, Any], *, owner_key: str = "user_id"
) -> Any:
    """The acting user: the envelope's ``metadata['user']`` (the editor), falling
    back to the payload's own owner FK (``event[owner_key]``) when there is no
    request-context actor (bulk import, management command, migration). Returns
    None when neither is present.
    """
    meta = msg.metadata or {}
    if meta.get("user") is not None:
        return meta["user"]
    return event.get(owner_key)


def history_effects(
    messages: Sequence[StreamMessage],
    model_label: str,
    *,
    subject_of: Callable[[dict[str, Any]], Any],
    defaults_of: Callable[[StreamMessage, dict[str, Any]], dict[str, Any]],
    subject_field: str = "subject",
    version_field: str = "version",
) -> list[Effect]:
    """One idempotent audit-row upsert per event in `messages`.

    Each row is keyed by ``{subject_field: subject_of(event), version_field:
    <index>}`` — the event's position in the stream — so re-materialising is a
    no-op. ``defaults_of(msg, event)`` shapes the row (typically via
    `label_marker` and `envelope_actor` plus the payload snapshot).

    Args:
        messages: the stream's messages (from ``store.read``), carrying the
            envelope ``label``/``metadata`` on each.
        model_label: 'app_label.ModelName' of the audit-row model.
        subject_of: maps a decoded event to its subject (the aggregate id — e.g.
            the Submission UUID).
        defaults_of: maps ``(message, decoded event)`` to the row's ``defaults=``.
        subject_field / version_field: the audit model's key columns.
    """
    effects: list[Effect] = []
    for version, msg in enumerate(messages):
        event = json.loads(msg.data)
        effects.append(
            Effect(
                op="update_or_create",
                model_label=model_label,
                lookup={subject_field: subject_of(event), version_field: version},
                defaults=defaults_of(msg, event),
            )
        )
    return effects


def recover_peak_snapshot(
    messages: Sequence[StreamMessage],
    subject: Any,
    *,
    subject_of: Callable[[dict[str, Any]], Any],
    snapshot_of: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Recover a subject's historical **peak** snapshot — the one with the most
    fields — the streams edition of ``repair_blank_save_dataloss``.

    Legacy-only: it recovers from a *bug* (a blank/truncating save). With no-op
    suppressed appends, stream-native writes needn't produce blank snapshots at
    all; carry this to recover old pghistory data, not as an ongoing need.

    Returns the peak snapshot for `subject`, or ``{}`` if it has no events.
    """
    snapshots = [
        (snapshot_of(event) if snapshot_of else event)
        for event in (json.loads(m.data) for m in messages)
        if subject_of(event) == subject
    ]
    return max(snapshots, key=len, default={})
