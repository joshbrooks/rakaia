"""The two routes a form takes to its tables: a save, and a rebuild.

A save appends the form to the log and replays just that one event through the
handlers, then records anything a step decides about it, such as which project
a report is assigned to. A rebuild replays the whole log into an empty
database, decisions included, and decides nothing. Both take their handlers from
the same step list, which is what makes it fair to compare what they produce.

Nothing here touches the example's models, so the tests can run both routes
over in-memory tables. The demo command passes the Django executor and reader.
"""

from __future__ import annotations

from typing import Any

from django_rakaia import append_event
from rakaia import (
    Executor,
    ExternalEffect,
    HandlerRegistry,
    ProjectionReader,
    ReplayResult,
    WritableStore,
    replay,
)

from .steps import STEPS, Step


def save_form(
    store: WritableStore,
    stream: str,
    form: dict[str, Any],
    registry: HandlerRegistry,
    *,
    executor: Executor,
    reader: ProjectionReader,
    outbox: list[ExternalEffect],
    steps: tuple[Step, ...] = STEPS,
    actor: str = "field officer",
) -> list[dict[str, Any]]:
    """Save one form, then record what the steps decide about it.

    The form is appended and replayed on its own. Then each step that decides
    something on a save looks at the tables and returns events to record, such
    as "this report is assigned to that registration". Those are appended and
    replayed in turn, so the log holds the decision and a rebuild replays it.
    Returns the events recorded after the form.
    """
    _append_and_replay(
        store, stream, form, registry, executor, reader, outbox, "submit", actor
    )
    recorded: list[dict[str, Any]] = []
    for step in steps:
        if step.decide is None:
            continue
        for event in step.decide(form, reader):
            _append_and_replay(
                store,
                stream,
                event,
                registry,
                executor,
                reader,
                outbox,
                "assign",
                "system",
            )
            recorded.append(event)
    return recorded


def _append_and_replay(
    store: WritableStore,
    stream: str,
    event: dict[str, Any],
    registry: HandlerRegistry,
    executor: Executor,
    reader: ProjectionReader,
    outbox: list[ExternalEffect],
    label: str,
    actor: str,
) -> None:
    """Append one event and replay just that event.

    Its position is counted before the append, because that is the number a
    rebuild will give it, and a handler version is picked by that number.
    """
    position = len(store.read(stream)[0]) if store.has(stream) else 0
    append_event(store, stream, event, label=label, actor=actor)
    result = replay(
        store,
        stream,
        executor,
        handler_registry=registry,
        start_seq=position,
        end_seq=position + 1,
        reader=reader,
    )
    outbox.extend(result.external)


def not_on_a_rebuild(steps: tuple[Step, ...] = STEPS) -> list[Step]:
    """The steps a rebuild does not run as a save does, each with its reason."""
    return [step for step in steps if step.on_rebuild is not None]


def rebuild(
    store: WritableStore,
    stream: str,
    registry: HandlerRegistry,
    *,
    executor: Executor,
    reader: ProjectionReader,
) -> ReplayResult:
    """Replay the whole log into empty tables.

    The emails the steps would send come back in ``result.external``, unsent.
    Unlike `save_form`, this puts them in no outbox, because the step that
    produces them says a rebuild does not reproduce them. The demo's rebuild goes through `rebuild_and_verify`
    instead, which does the same replay and also compares the result with live.
    """
    return replay(store, stream, executor, handler_registry=registry, reader=reader)
