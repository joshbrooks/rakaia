"""The staged pipeline both replay entry points share, tested through them.

`replay()` reads one stream in order; `merge_replay()` k-way-merges several by an
order key. Everything *after* that is one shared pipeline: defaulting the
registries, the reader-required check, one pass per stage, the reducers, the
event count.

This file used to call `build_pipeline()` and `run_passes()` directly, on a
fabricated list of decoded events — testing the machinery through a side door,
which #189 filed because it is what a seam in the wrong place looks like. The
seam was in the wrong place: `replay()` kept a *second* hand-written loop, for a
reason it did not name — decoding one event at a time so a malformed event
partway through leaves the events before it applied. Now that difference has a
name (`EventSource`) and `run_passes` owns both shapes, there is nothing behind
the entry points worth reaching past, so these tests go through them.

Every shared behaviour is run against **both** entry points, because they are
exactly the self-covering pair CLAUDE.md warns about: a mutation in the pipeline
goes red through `replay()` while `merge_replay()` stays green, and either alone
reads as covered.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from rakaia.effects import Upsert
from rakaia.executors import CollectingExecutor
from rakaia.registry import HandlerRegistry
from rakaia.replay import ReplayResult, merge_replay, replay
from rakaia.seed import seed_stream
from rakaia.store import StreamStore

#: The one match string every handler here registers against. `replay()` gets it
#: as `event_match`; `merge_replay()` too, so both route merged and single-stream
#: events through the same registry regardless of which stream they came from.
MATCH = "s"

#: A malformed event: valid bytes in a stream, not valid JSON.
BAD_EVENT = b"not json {"


def _effect(name: str, value: Any) -> Upsert:
    return Upsert(
        model_label="app.Model", lookup={"name": name}, defaults={"value": value}
    )


def _make_reducer(stage: int) -> Callable[[Any], Upsert]:
    """A factory, not a default argument: a second *positional* parameter is how
    a reducer opts in to the touched-subjects arg, so `lambda r, _s=stage: ...`
    would silently be handed the touched tuple instead of the stage."""
    return lambda _reader: _effect(f"reduced@{stage}", stage)


def _registry(*, stages=(0,), reducers=()) -> HandlerRegistry:
    registry = HandlerRegistry()
    for stage in stages:
        registry.register(
            name=f"h{stage}",
            event_match=MATCH,
            fn=(lambda ev, *_a, _s=stage: _effect(f"{ev['id']}@{_s}", _s)),
            effective_from=0,
            stage=stage,
        )
    for i, stage in enumerate(reducers):
        registry.register_reducer(f"r{i}", stage=stage, fn=_make_reducer(stage))
    return registry


def _payloads(n: int) -> list[dict[str, Any]]:
    """`n` events carrying their own id and merge key, so the merged order of a
    two-stream replay is the same sequence a single-stream replay reads."""
    return [{"id": f"e{i}", "ts": i} for i in range(n)]


@dataclass(frozen=True)
class Entry:
    """One of the two public replay entry points, with the seeding it needs.

    Parametrising over this is what stops a pipeline mutation being covered on
    one twin and green on the other.
    """

    name: str
    """The word the reader-required error must name, which differs per entry."""

    seed: Callable[[StreamStore, list[Any]], None]
    run: Callable[..., ReplayResult]


def _seed_one(store: StreamStore, events: list[Any]) -> None:
    seed_stream("s", events, store=store)


def _seed_split(store: StreamStore, events: list[Any]) -> None:
    """Alternate the events between two streams, so the merge has to interleave
    them by their order key rather than concatenate the streams."""
    store.create("a")
    store.create("b")
    for i, event in enumerate(events):
        seed_stream("a" if i % 2 == 0 else "b", [event], store=store)


def _run_replay(store, registry, executor, *, reader=None) -> ReplayResult:
    return replay(
        store,
        "s",
        executor,
        handler_registry=registry,
        event_match=MATCH,
        reader=reader,
    )


def _run_merge_replay(store, registry, executor, *, reader=None) -> ReplayResult:
    return merge_replay(
        store,
        ["a", "b"],
        executor,
        order_key="ts",
        handler_registry=registry,
        event_match=MATCH,
        reader=reader,
    )


ENTRIES = [
    Entry(name="Replay", seed=_seed_one, run=_run_replay),
    Entry(name="merge_replay", seed=_seed_split, run=_run_merge_replay),
]

both_entries = pytest.mark.parametrize(
    "entry", ENTRIES, ids=[e.run.__name__.removeprefix("_run_") for e in ENTRIES]
)


@pytest.fixture
def store() -> StreamStore:
    return StreamStore()


@both_entries
class TestSingleStage:
    def test_every_event_is_dispatched_once(self, store, entry: Entry):
        entry.seed(store, _payloads(3))
        ex = CollectingExecutor()

        result = entry.run(store, _registry(), ex)

        assert result.events_processed == 3
        assert len(ex.effects) == 3

    def test_an_empty_stream_is_a_no_op(self, store, entry: Entry):
        entry.seed(store, [])
        ex = CollectingExecutor()

        result = entry.run(store, _registry(), ex)

        assert result.events_processed == 0
        assert ex.effects == []

    def test_events_are_dispatched_in_order(self, store, entry: Entry):
        entry.seed(store, _payloads(3))
        ex = CollectingExecutor()

        entry.run(store, _registry(), ex)

        assert [e.lookup["name"] for e in ex.effects] == ["e0@0", "e1@0", "e2@0"]


@both_entries
class TestStagedPasses:
    def test_every_event_runs_through_stage_zero_before_stage_one(
        self, store, entry: Entry
    ):
        """The defining property of staged replay, and the one thing that was
        implemented twice."""
        entry.seed(store, _payloads(2))
        ex = CollectingExecutor()

        entry.run(store, _registry(stages=(0, 1)), ex, reader=object())

        stages = [e.defaults["value"] for e in ex.effects]
        assert stages == [0, 0, 1, 1], (
            "all of stage 0 must complete before stage 1 begins"
        )

    def test_reducers_run_once_per_stage_after_that_stage(self, store, entry: Entry):
        entry.seed(store, _payloads(3))
        ex = CollectingExecutor()
        registry = _registry(stages=(0,), reducers=(0,))

        entry.run(store, registry, ex, reader=object())

        names = [e.lookup["name"] for e in ex.effects]
        assert names == ["e0@0", "e1@0", "e2@0", "reduced@0"]

    def test_each_event_is_counted_once_however_many_stages_ran(
        self, store, entry: Entry
    ):
        """Two passes over three events is still three events processed."""
        entry.seed(store, _payloads(3))

        result = entry.run(
            store, _registry(stages=(0, 1)), CollectingExecutor(), reader=object()
        )

        assert result.events_processed == 3


@both_entries
class TestTheReaderRequirement:
    def test_a_staged_registry_without_a_reader_is_refused(self, store, entry: Entry):
        entry.seed(store, _payloads(1))
        with pytest.raises(ValueError, match="reader"):
            entry.run(store, _registry(stages=(0, 1)), CollectingExecutor())

    def test_a_reducer_without_a_reader_is_refused(self, store, entry: Entry):
        entry.seed(store, _payloads(1))
        registry = _registry(stages=(0,), reducers=(0,))
        with pytest.raises(ValueError, match="reader"):
            entry.run(store, registry, CollectingExecutor())

    def test_a_single_stage_registry_needs_no_reader(self, store, entry: Entry):
        entry.seed(store, _payloads(1))
        entry.run(store, _registry(), CollectingExecutor())

    def test_the_message_names_the_caller(self, store, entry: Entry):
        """`replay` and `merge_replay` each had their own wording; the caller is
        a parameter of the shared check, so the guidance still points at the
        function the user called."""
        entry.seed(store, _payloads(1))
        with pytest.raises(ValueError, match=entry.name):
            entry.run(store, _registry(stages=(0, 1)), CollectingExecutor())

    def test_the_refusal_precedes_any_dispatch(self, store, entry: Entry):
        """Failing here beats failing inside the first handler that dereferences
        `None`, so nothing may have been applied by the time it raises."""
        entry.seed(store, _payloads(3))
        ex = CollectingExecutor()
        with pytest.raises(ValueError, match="reader"):
            entry.run(store, _registry(stages=(0, 1)), ex)
        assert ex.effects == []


@both_entries
class TestSeqIsCarriedThrough:
    def test_the_seq_on_each_event_selects_the_handler_version(
        self, store, entry: Entry
    ):
        """A pipeline event carries its own seq, so a merged replay numbers by
        merged position while a single-stream replay numbers by offset — and the
        shared pipeline never learns which it is."""
        registry = HandlerRegistry()
        registry.register(
            name="v",
            event_match=MATCH,
            fn=lambda ev: _effect(ev["id"], "old"),
            effective_from=0,
            effective_to=2,
        )
        registry.register(
            name="v",
            event_match=MATCH,
            fn=lambda ev: _effect(ev["id"], "new"),
            effective_from=2,
        )
        entry.seed(store, _payloads(4))
        ex = CollectingExecutor()

        entry.run(store, registry, ex)

        assert [e.defaults["value"] for e in ex.effects] == [
            "old",
            "old",
            "new",
            "new",
        ]


class TestHowFarAReplayGetsBeforeFailing:
    """`EventSource` — the thing the two entry points differ about, and the
    reason `replay()` used to carry a second loop.

    A malformed event is the cheapest way to observe it: how many of the good
    events reached the executor before the failure is the whole difference.
    """

    def test_a_single_pass_replay_applies_the_events_before_a_malformed_one(
        self, store
    ):
        seed_stream("s", [{"id": "e0"}, BAD_EVENT, {"id": "e2"}], store=store)
        ex = CollectingExecutor()

        with pytest.raises(ValueError, match="Cannot decode event"):
            replay(store, "s", ex, handler_registry=_registry(), event_match=MATCH)

        assert [e.lookup["name"] for e in ex.effects] == ["e0@0"], (
            "a single pass decodes one event at a time, so the events before "
            "the malformed one are already applied"
        )

    def test_a_staged_replay_applies_nothing_when_any_event_is_malformed(self, store):
        seed_stream("s", [{"id": "e0"}, BAD_EVENT, {"id": "e2"}], store=store)
        ex = CollectingExecutor()

        with pytest.raises(ValueError, match="Cannot decode event"):
            replay(
                store,
                "s",
                ex,
                handler_registry=_registry(stages=(0, 1)),
                event_match=MATCH,
                reader=object(),
            )

        assert ex.effects == [], (
            "more than one pass has to see every event first, so nothing is "
            "applied when any of them fails to decode"
        )

    def test_a_merge_applies_nothing_when_any_event_is_malformed(self, store):
        _seed_split(store, [{"id": "e0", "ts": 0}, {"id": "e1", "ts": 1}])
        store.append("a", BAD_EVENT)
        ex = CollectingExecutor()

        with pytest.raises(ValueError, match="Cannot decode event"):
            _run_merge_replay(store, _registry(), ex)

        assert ex.effects == [], (
            "a merge cannot know the order until every stream is read, so a "
            "malformed event anywhere applies nothing — even unstaged"
        )
