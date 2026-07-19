"""
No-op-suppressed append — the write-side of the pghistory-parity story.

`django-pghistory` only records an event when the row actually changed
(``WHEN OLD.* IS DISTINCT FROM NEW.*``). A naive ``store.append(new_state)``
records an event on *every* save, so a stream-native audit log would diverge
from ``pgh_event`` on no-op saves, and bulk-import retries would duplicate rows.

`append_if_changed` closes that: it appends only when the new payload differs
from the subject's **current** snapshot (which the caller supplies — it's what
they're about to overwrite, from the current-state projection). Comparing
against the subject's own current state, not the stream's last message, is what
makes this correct for form-family streams that interleave many subjects.

    changed = append_if_changed(
        store, "submissions/tf", data,
        current=SubmissionRecord.objects.filter(key=sub).values_list("fields", flat=True).first(),
        snapshot_of=lambda ev: ev["fields"],   # compare just the form fields
        options=AppendOptions(label="update"),
    )

This is the append-layer analogue of the `skip_unchanged` executor (#18).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any


def snapshots_equal(a: Any, b: Any) -> bool:
    """Whether two decoded snapshots are equal.

    Plain ``==`` is a deep, key-order-insensitive comparison for JSON-native
    values (dicts compare unordered, lists ordered) — exactly the right
    semantics for "did anything change".
    """
    return a == b


def append_if_changed(
    store: Any,
    path: str,
    data: bytes,
    *,
    current: Any,
    options: Any = None,
    snapshot_of: Callable[[dict[str, Any]], Any] | None = None,
) -> bool:
    """Append `data` to `path` only if its snapshot differs from `current`.

    Args:
        store: any store with an ``append(path, data, options)`` method.
        path: the stream path.
        data: the JSON-encoded event bytes to append.
        current: the subject's current snapshot (in the same shape `snapshot_of`
            produces), or ``None`` if the subject is new — a new subject always
            appends.
        options: forwarded to ``store.append`` (e.g. the envelope
            ``AppendOptions(label=…, metadata=…)``).
        snapshot_of: extracts the comparable snapshot from the decoded payload
            (default: the whole payload). Use it to ignore volatile fields (a
            server timestamp) so they don't defeat suppression.

    Returns:
        True if the event was appended, False if it was suppressed as a no-op.
    """
    new_snapshot = json.loads(data)
    if snapshot_of is not None:
        new_snapshot = snapshot_of(new_snapshot)
    if current is not None and snapshots_equal(new_snapshot, current):
        return False
    store.append(path, data, options)
    return True
