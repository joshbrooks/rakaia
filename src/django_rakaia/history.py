"""Convenience: materialise a `/history` audit table from a stream in one call.

`rakaia.history_effects` builds the audit-row Effects from a stream's messages;
applying them to the Django ORM is then a `DjangoExecutor().apply(...)` away.
Every adopter otherwise repeats that read → build → apply dance (see the
examples). `materialize_history` collapses it into a single call.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from rakaia.effects import Effect
from rakaia.history import history_effects

from .effect_executor import DjangoExecutor


def materialize_history(
    store: Any,
    path: str,
    model_label: str,
    *,
    subject_of: Callable[[dict[str, Any]], Any],
    defaults_of: Callable[[Any, dict[str, Any]], dict[str, Any]],
    subject_field: str = "subject",
    version_field: str = "version",
    version_of: Callable[[Any], Any] | None = None,
    executor: Any | None = None,
) -> list[Effect]:
    """Read `path` and materialise its `/history` audit rows into `model_label`.

    Reads the **whole** stream each call (there is no incremental/tail mode
    here — for that, call `rakaia.history_effects` yourself on a
    ``store.read(path, offset=…)`` slice). Builds one audit-row upsert per event
    via `rakaia.history_effects` (keyed by ``(subject, version)`` so re-running
    is a no-op), applies them with a `DjangoExecutor`, and returns the applied
    Effects.

    Pass ``version_of=lambda m: int(m.offset)`` to key rows on the durable stream
    offset — never renumbered, so the keys match an incremental
    `history_effects` done elsewhere; the default list-index version is fine for
    this whole-stream read but is not offset-stable. Pass `executor` to reuse a
    shared executor (e.g. one with ``skip_unchanged=True``).
    """
    messages, _ = store.read(path)
    effects = history_effects(
        messages,
        model_label,
        subject_of=subject_of,
        defaults_of=defaults_of,
        subject_field=subject_field,
        version_field=version_field,
        version_of=version_of,
    )
    (executor or DjangoExecutor()).apply(effects)
    return effects
