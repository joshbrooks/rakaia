"""
History read-model: materialise a per-event audit log from an enveloped stream.

This is the streams-native replacement for `django-pghistory`'s consumers — the
`/history` audit API and the admin event log. Because a stream carries the
event **envelope** (label + metadata) on each message (PR A), a history
projection is just another fan-out: one audit row per event, keyed by
``(subject, version)``, carrying the label, timestamp, actor, metadata, and the
full payload snapshot.

`history_effects` handles the iteration + keying and leaves the row *shape* to
the caller (a `defaults_of(msg, event)` callback), so the audit model can match
whatever `/history` returns. Two convenience helpers cover the two fiddly bits
the audit consumers need: `label_marker` (label → ``+``/``~``/``-``) and
`envelope_actor` (the ``metadata['user']`` editor, falling back to the payload's
own owner FK).
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
    version_of: Callable[[StreamMessage], Any] | None = None,
) -> list[Effect]:
    """One idempotent audit-row upsert per event in `messages`.

    Each row is keyed by ``{subject_field: subject_of(event), version_field:
    <version>}`` so re-materialising is a no-op. ``defaults_of(msg, event)``
    shapes the row (typically via `label_marker` and `envelope_actor` plus the
    payload snapshot).

    **Version.** By default the version is the event's index in `messages`,
    which is correct only when `messages` is the **whole stream**. For
    incremental (tail `store.read(path, offset=…)`) or merged inputs — where the
    index restarts and would collide with earlier events of the same subject —
    pass ``version_of`` to derive a stable per-event version. Use the **opaque
    offset token itself**, ``version_of=lambda m: m.offset`` (stored in a
    string/char column): it is stable and never renumbered, matching the RFC's
    "audit keyed by (stream, offset)". Do **not** parse it with ``int()`` — an
    offset is opaque and its format is store-specific (the in-memory store's is a
    compound ``{seq}_{byte}`` string, not an integer); see the `ReadableStore`
    offset contract.

    Args:
        messages: the stream's messages (from ``store.read``), carrying the
            envelope ``label``/``metadata`` on each.
        model_label: 'app_label.ModelName' of the audit-row model.
        subject_of: maps a decoded event to its subject (the aggregate id — e.g.
            the Submission UUID).
        defaults_of: maps ``(message, decoded event)`` to the row's ``defaults=``.
        subject_field / version_field: the audit model's key columns.
        version_of: optional stable version per message; defaults to the list
            index (full-stream only).
    """
    effects: list[Effect] = []
    for index, msg in enumerate(messages):
        event = json.loads(msg.data)
        version = version_of(msg) if version_of is not None else index
        effects.append(
            Effect(
                op="update_or_create",
                model_label=model_label,
                lookup={subject_field: subject_of(event), version_field: version},
                defaults=defaults_of(msg, event),
            )
        )
    return effects
