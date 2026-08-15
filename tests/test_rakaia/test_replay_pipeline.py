"""Staged replay, driven from a list of events — no store, no JSON, no upcasters.

`replay()` reads one stream in order; `merge_replay()` k-way-merges several by an
order key. Those are genuinely different jobs. Everything *after* them was the
same and written twice: defaulting the registries, an identical `_upcaster_drift`
closure, a `_ReplayCtx` with the same seven fields, the `any(stage > 0) or
has_reducers()` test, the reader-required check, the stage-pass loop, and
`events_processed`.

Sixty-odd lines, in two different control-flow shapes — `replay` early-returns a
streaming single-stage loop and raises the reader error *after* it, while
`merge_replay` materialises everything and checks before. Any change to staging
semantics had to be made twice, correctly, in two shapes that no longer looked
alike.

The point of a separate pipeline is that the part which is genuinely shared can
be tested on a fabricated list of decoded events. Every test here would
previously have needed a real stream, real bytes, and a real registry with real
source-hashed functions.
"""

from __future__ import annotations

import pytest

from rakaia.effects import Effect
from rakaia.executors import CollectingExecutor
from rakaia.registry import HandlerRegistry
from rakaia.replay import build_pipeline, run_passes


def _effect(name: str, value):
    return Effect(
        op="update_or_create",
        model_label="app.Model",
        lookup={"name": name},
        defaults={"value": value},
    )


def _make_reducer(stage: int):
    """A factory, not a default argument: a second *positional* parameter is how
    a reducer opts in to the touched-subjects arg, so `lambda r, _s=stage: ...`
    would silently be handed the touched tuple instead of the stage."""
    return lambda _reader: _effect(f"reduced@{stage}", stage)


def _registry(*, stages=(0,), reducers=()) -> HandlerRegistry:
    registry = HandlerRegistry()
    for stage in stages:
        registry.register(
            name=f"h{stage}",
            event_match="s",
            fn=(lambda ev, *_a, _s=stage: _effect(f"{ev['id']}@{_s}", _s)),
            effective_from=0,
            stage=stage,
        )
    for i, stage in enumerate(reducers):
        registry.register_reducer(f"r{i}", stage=stage, fn=_make_reducer(stage))
    return registry


def _events(n: int):
    """`(seq, match_str, decoded_event)` — what a pipeline consumes."""
    return [(i, "s", {"id": f"e{i}"}) for i in range(n)]


def _pipeline(registry, executor, *, reader=None):
    return build_pipeline(
        handler_registry=registry,
        upcaster_registry=None,
        executor=executor,
        reader=reader,
        include_external=False,
        on_drift="warn",
    )


class TestSingleStage:
    def test_every_event_is_dispatched_once(self):
        ex = CollectingExecutor()
        result = run_passes(_pipeline(_registry(), ex), _events(3))
        assert result.events_processed == 3
        assert len(ex.effects) == 3

    def test_an_empty_event_list_is_a_no_op(self):
        ex = CollectingExecutor()
        result = run_passes(_pipeline(_registry(), ex), [])
        assert result.events_processed == 0
        assert ex.effects == []

    def test_events_are_dispatched_in_list_order(self):
        ex = CollectingExecutor()
        run_passes(_pipeline(_registry(), ex), _events(3))
        assert [e.lookup["name"] for e in ex.effects] == ["e0@0", "e1@0", "e2@0"]


class TestStagedPasses:
    def test_every_event_runs_through_stage_zero_before_stage_one(self):
        """The defining property of staged replay, and the one thing that was
        implemented twice."""
        ex = CollectingExecutor()
        reader = object()
        run_passes(_pipeline(_registry(stages=(0, 1)), ex, reader=reader), _events(2))

        stages = [e.defaults["value"] for e in ex.effects]
        assert stages == [0, 0, 1, 1], (
            "all of stage 0 must complete before stage 1 begins"
        )

    def test_reducers_run_once_per_stage_after_that_stage(self):
        ex = CollectingExecutor()
        registry = _registry(stages=(0,), reducers=(0,))
        run_passes(_pipeline(registry, ex, reader=object()), _events(3))

        names = [e.lookup["name"] for e in ex.effects]
        assert names == ["e0@0", "e1@0", "e2@0", "reduced@0"]


class TestTheReaderRequirement:
    def test_a_staged_registry_without_a_reader_is_refused(self):
        with pytest.raises(ValueError, match="reader"):
            run_passes(_pipeline(_registry(stages=(0, 1)), CollectingExecutor()), [])

    def test_a_reducer_without_a_reader_is_refused(self):
        registry = _registry(stages=(0,), reducers=(0,))
        with pytest.raises(ValueError, match="reader"):
            run_passes(_pipeline(registry, CollectingExecutor()), [])

    def test_a_single_stage_registry_needs_no_reader(self):
        run_passes(_pipeline(_registry(), CollectingExecutor()), _events(1))

    def test_the_message_names_the_caller(self):
        """`replay` and `merge_replay` each had their own wording; the caller is
        now a parameter so the guidance still points at the right function."""
        with pytest.raises(ValueError, match="merge_replay"):
            run_passes(
                _pipeline(_registry(stages=(0, 1)), CollectingExecutor()),
                [],
                what="merge_replay",
            )


class TestSeqIsCarriedThrough:
    def test_the_seq_on_each_event_selects_the_handler_version(self):
        """A pipeline event carries its own seq, so a merged replay can number
        by merged position while a single-stream replay numbers by offset —
        without the pipeline knowing which it is."""
        registry = HandlerRegistry()
        registry.register(
            name="v",
            event_match="s",
            fn=lambda ev: _effect(ev["id"], "old"),
            effective_from=0,
            effective_to=2,
        )
        registry.register(
            name="v",
            event_match="s",
            fn=lambda ev: _effect(ev["id"], "new"),
            effective_from=2,
        )
        ex = CollectingExecutor()
        run_passes(_pipeline(registry, ex), _events(4))
        assert [e.defaults["value"] for e in ex.effects] == [
            "old",
            "old",
            "new",
            "new",
        ]
