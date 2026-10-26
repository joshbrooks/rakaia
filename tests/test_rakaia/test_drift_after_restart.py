"""#344: an edited frozen handler is drift after a restart, not only in-process.

A "restart" here is a fresh `HandlerRegistry` built on the same store: the
in-memory registrations are gone and only the registration log remains.
"""

from __future__ import annotations

import json

import pytest

from rakaia import AppendOptions, seed_stream
from rakaia.drift import HandlerDriftError
from rakaia.executors import CollectingExecutor
from rakaia.registry import HANDLERS_META_STREAM, HandlerRegistry
from rakaia.replaying import replay
from rakaia.store import StreamStore


def _handler_as_first_registered(_event):
    return []


def _handler_after_an_edit(_event):
    effects: list = []  # a different body, hence a different hash
    return effects


def _events() -> StreamStore:
    return seed_stream("s", [({"x": 1}, AppendOptions(label="n"))])


def _restart_with_edit(store: StreamStore, name: str = "h") -> HandlerRegistry:
    HandlerRegistry(store=store).register(
        name, "s", _handler_as_first_registered, 0, None
    )
    after_restart = HandlerRegistry(store=store)
    after_restart.register(name, "s", _handler_after_an_edit, 0, None)
    return after_restart


def test_an_edit_after_restart_raises_under_on_drift_raise() -> None:
    registry = _restart_with_edit(StreamStore())

    with pytest.raises(HandlerDriftError, match="handler='h'"):
        replay(
            _events(),
            "s",
            CollectingExecutor(),
            handler_registry=registry,
            on_drift="raise",
        )


def test_an_edit_after_restart_is_listed_under_on_drift_warn() -> None:
    registry = _restart_with_edit(StreamStore())

    result = replay(_events(), "s", CollectingExecutor(), handler_registry=registry)

    assert result.drift_detected == ["h"]
    assert "RAKAIA_DRIFT handler='h'" in result.warnings[0]


def test_the_first_registration_stays_the_reference_after_a_second_restart() -> None:
    store = StreamStore()
    _restart_with_edit(store)
    third_start = HandlerRegistry(store=store)
    third_start.register("h", "s", _handler_after_an_edit, 0, None)

    result = replay(_events(), "s", CollectingExecutor(), handler_registry=third_start)

    assert result.drift_detected == ["h"]


def test_an_unedited_handler_after_restart_is_not_drift() -> None:
    store = StreamStore()
    HandlerRegistry(store=store).register(
        "h", "s", _handler_as_first_registered, 0, None
    )
    after_restart = HandlerRegistry(store=store)
    after_restart.register("h", "s", _handler_as_first_registered, 0, None)

    result = replay(
        _events(),
        "s",
        CollectingExecutor(),
        handler_registry=after_restart,
        on_drift="raise",
    )

    assert result.drift_detected == []
    assert len(store.read(HANDLERS_META_STREAM)[0]) == 1


def test_a_different_range_is_a_different_handler_not_drift() -> None:
    """Closing v1 and opening v2 with other code is the point of versioning."""
    store = StreamStore()
    HandlerRegistry(store=store).register("h", "s", _handler_as_first_registered, 0, 5)
    after_restart = HandlerRegistry(store=store)
    after_restart.register("h", "s", _handler_as_first_registered, 0, 5)
    after_restart.register("h", "s", _handler_after_an_edit, 5, None)

    result = replay(
        _events(),
        "s",
        CollectingExecutor(),
        handler_registry=after_restart,
        on_drift="raise",
    )

    assert result.drift_detected == []


def test_the_edited_registration_is_still_appended_to_the_log() -> None:
    store = StreamStore()
    _restart_with_edit(store)

    messages, _ = store.read(HANDLERS_META_STREAM)

    assert len(messages) == 2
    assert (
        json.loads(messages[0].data)["source_hash"]
        != json.loads(messages[1].data)["source_hash"]
    )


def test_closing_a_range_on_edited_code_is_a_new_registration_not_drift() -> None:
    """Same start, different end: the bound is part of which handler this is."""
    store = StreamStore()
    HandlerRegistry(store=store).register(
        "h", "s", _handler_as_first_registered, 0, None
    )
    after_restart = HandlerRegistry(store=store)
    after_restart.register("h", "s", _handler_after_an_edit, 0, 5)

    result = replay(
        _events(),
        "s",
        CollectingExecutor(),
        handler_registry=after_restart,
        on_drift="raise",
    )

    assert result.drift_detected == []


_FIRST = {
    "name": "h",
    "event_match": "s",
    "match_field": None,
    "effective_from": 0,
    "stage": 0,
}


@pytest.mark.parametrize(
    "changed",
    [
        {"name": "other"},
        {"event_match": "t"},
        {"match_field": "form_type"},
        {"effective_from": 5},
        {"stage": 1},
    ],
    ids=lambda c: next(iter(c)),
)
def test_a_registration_differing_in_one_part_of_the_range_is_not_the_same_handler(
    changed: dict,
) -> None:
    """Each part of a range is part of which handler this is, so edited code
    under a different one is a new registration, not drift."""
    store = StreamStore()
    first = _FIRST
    HandlerRegistry(store=store).register(
        first["name"],
        first["event_match"],
        _handler_as_first_registered,
        first["effective_from"],
        None,
        match_field=first["match_field"],
        stage=first["stage"],
    )
    spec = {**first, **changed}
    version = HandlerRegistry(store=store).register(
        spec["name"],
        spec["event_match"],
        _handler_after_an_edit,
        spec["effective_from"],
        None,
        match_field=spec["match_field"],
        stage=spec["stage"],
    )

    assert version.hash_to_check == version.source_hash


def test_the_same_range_with_edited_code_is_drift() -> None:
    """The control for the test above: nothing differs but the code."""
    store = StreamStore()
    first = _FIRST
    HandlerRegistry(store=store).register(
        first["name"], first["event_match"], _handler_as_first_registered, 0, None
    )
    version = HandlerRegistry(store=store).register(
        first["name"], first["event_match"], _handler_after_an_edit, 0, None
    )

    assert version.hash_to_check != version.source_hash
