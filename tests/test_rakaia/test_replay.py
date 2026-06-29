"""End-to-end tests for the replay() orchestrator (without Django)."""

from __future__ import annotations

import json

import pytest

from rakaia.effects import Effect
from rakaia.registry import (
    HandlerDriftError,
    HandlerGapError,
    HandlerRegistry,
    UpcasterRegistry,
)
from rakaia.replay import replay
from rakaia.store import StreamStore


class CaptureExecutor:
    """Test executor that records every batch of effects it sees."""

    def __init__(self) -> None:
        self.batches: list[list[Effect]] = []

    def apply(self, effects):
        self.batches.append(list(effects))

    @property
    def all_effects(self) -> list[Effect]:
        out: list[Effect] = []
        for batch in self.batches:
            out.extend(batch)
        return out


@pytest.fixture
def store() -> StreamStore:
    return StreamStore()


def _seed_stream(store: StreamStore, path: str, events: list[dict]) -> None:
    store.create(path)
    for ev in events:
        store.append(path, json.dumps(ev).encode("utf-8"))


# ---------------------------------------------------------------------------
# Basic dispatch + apply
# ---------------------------------------------------------------------------


class TestBasicReplay:
    def test_single_event_single_handler(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):
            return Effect(
                op="update_or_create",
                model_label="x.X",
                lookup={"id": event["id"]},
                defaults={"name": event["name"]},
            )

        reg.register("h", "stream", h, 0, None)
        _seed_stream(store, "stream", [{"id": 1, "name": "alice"}])
        ex = CaptureExecutor()

        result = replay(store, "stream", ex, handler_registry=reg)

        assert result.events_processed == 1
        assert result.effects_applied == 1
        assert len(ex.all_effects) == 1
        assert ex.all_effects[0].defaults == {"name": "alice"}

    def test_multiple_handlers_run_per_event(self, store: StreamStore):
        reg = HandlerRegistry()

        def h1(event):
            return Effect("update_or_create", "x.X", {"id": event["id"]}, {"a": 1})

        def h2(event):
            return Effect("update_or_create", "x.X", {"id": event["id"]}, {"b": 2})

        reg.register("h1", "stream", h1, 0, None)
        reg.register("h2", "stream", h2, 0, None)
        _seed_stream(store, "stream", [{"id": 1}])
        ex = CaptureExecutor()

        result = replay(store, "stream", ex, handler_registry=reg)

        assert result.events_processed == 1
        assert len(ex.all_effects) == 2
        assert {
            e.defaults["a"] if e.defaults and "a" in e.defaults else None
            for e in ex.all_effects
        } == {1, None}

    def test_handler_returning_list_of_effects(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):
            return [
                Effect("update_or_create", "x.X", {"id": event["id"]}, {"a": 1}),
                Effect("update_or_create", "x.Y", {"id": event["id"]}, {"b": 2}),
            ]

        reg.register("h", "stream", h, 0, None)
        _seed_stream(store, "stream", [{"id": 1}])
        ex = CaptureExecutor()

        replay(store, "stream", ex, handler_registry=reg)
        assert len(ex.all_effects) == 2

    def test_handler_returning_none_emits_no_effects(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):  # noqa: ARG001
            return None

        reg.register("h", "stream", h, 0, None)
        _seed_stream(store, "stream", [{"id": 1}])
        ex = CaptureExecutor()

        result = replay(store, "stream", ex, handler_registry=reg)
        assert result.events_processed == 1
        assert result.effects_applied == 0
        assert ex.batches == []  # nothing applied


# ---------------------------------------------------------------------------
# Versioning: correct version per seq
# ---------------------------------------------------------------------------


class TestVersioning:
    def test_picks_correct_version_per_seq(self, store: StreamStore):
        reg = HandlerRegistry()

        def h_v1(event):
            return Effect("update_or_create", "x.X", {"id": event["id"]}, {"v": 1})

        def h_v2(event):
            return Effect("update_or_create", "x.X", {"id": event["id"]}, {"v": 2})

        reg.register("h", "s", h_v1, 0, 3)
        reg.register("h", "s", h_v2, 3, None)
        _seed_stream(
            store,
            "s",
            [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
        )
        ex = CaptureExecutor()

        replay(store, "s", ex, handler_registry=reg)

        assert [e.defaults["v"] for e in ex.all_effects] == [1, 1, 1, 2, 2]


# ---------------------------------------------------------------------------
# Idempotency: replay twice = same state (captured via executor)
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_replay_twice_produces_same_effects(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):
            return Effect(
                "update_or_create", "x.X", {"id": event["id"]}, {"name": event["name"]}
            )

        reg.register("h", "s", h, 0, None)
        _seed_stream(store, "s", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

        ex1 = CaptureExecutor()
        replay(store, "s", ex1, handler_registry=reg)

        ex2 = CaptureExecutor()
        replay(store, "s", ex2, handler_registry=reg)

        assert ex1.all_effects == ex2.all_effects


# ---------------------------------------------------------------------------
# External effects
# ---------------------------------------------------------------------------


class TestExternalEffects:
    def test_external_skipped_by_default(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):  # noqa: ARG001
            return [
                Effect(op="external", kind="email", payload={"to": "x@y"}),
                Effect("update_or_create", "x.X", {"id": 1}, {"a": 1}),
            ]

        reg.register("h", "s", h, 0, None)
        _seed_stream(store, "s", [{"x": 1}])
        ex = CaptureExecutor()

        result = replay(store, "s", ex, handler_registry=reg)

        assert result.external_effects_skipped == 1
        assert len(ex.all_effects) == 1
        assert ex.all_effects[0].op == "update_or_create"

    def test_external_included_when_flag_set(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):  # noqa: ARG001
            return Effect(op="external", kind="email", payload={"to": "x"})

        reg.register("h", "s", h, 0, None)
        _seed_stream(store, "s", [{"x": 1}])
        ex = CaptureExecutor()

        result = replay(store, "s", ex, handler_registry=reg, include_external=True)

        assert result.external_effects_skipped == 0
        assert len(ex.all_effects) == 1
        assert ex.all_effects[0].op == "external"


# ---------------------------------------------------------------------------
# Upcasting integration
# ---------------------------------------------------------------------------


class TestUpcasting:
    def test_handler_sees_upcasted_event(self, store: StreamStore):
        handlers = HandlerRegistry()
        upcasters = UpcasterRegistry()

        def add_currency(event):
            return {**event, "currency": "USD"}

        upcasters.register("s", 1, add_currency)

        captured: list[dict] = []

        def h(event):
            captured.append(event)
            return None

        handlers.register("h", "s", h, 0, None)
        _seed_stream(store, "s", [{"id": 1, "schema_version": 1}])
        ex = CaptureExecutor()

        replay(
            store,
            "s",
            ex,
            handler_registry=handlers,
            upcaster_registry=upcasters,
        )

        assert captured[0]["currency"] == "USD"
        assert captured[0]["schema_version"] == 2


# ---------------------------------------------------------------------------
# Range bounds
# ---------------------------------------------------------------------------


class TestRangeBounds:
    def test_start_seq_skips_earlier(self, store: StreamStore):
        reg = HandlerRegistry()

        captured: list[int] = []

        def h(event):
            captured.append(event["id"])
            return None

        reg.register("h", "s", h, 0, None)
        _seed_stream(store, "s", [{"id": i} for i in range(5)])

        result = replay(
            store, "s", CaptureExecutor(), handler_registry=reg, start_seq=2
        )

        assert captured == [2, 3, 4]
        assert result.events_processed == 3

    def test_end_seq_excludes(self, store: StreamStore):
        reg = HandlerRegistry()

        captured: list[int] = []

        def h(event):
            captured.append(event["id"])
            return None

        reg.register("h", "s", h, 0, None)
        _seed_stream(store, "s", [{"id": i} for i in range(5)])

        replay(store, "s", CaptureExecutor(), handler_registry=reg, end_seq=3)

        assert captured == [0, 1, 2]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class TestErrors:
    def test_gap_in_handler_coverage_raises(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):  # noqa: ARG001
            return None

        reg.register("h", "s", h, 0, 2)
        reg.register("h", "s", h, 5, None)
        _seed_stream(store, "s", [{"id": i} for i in range(6)])

        with pytest.raises(HandlerGapError):
            replay(store, "s", CaptureExecutor(), handler_registry=reg)

    def test_invalid_json_raises(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):  # noqa: ARG001
            return None

        reg.register("h", "s", h, 0, None)
        store.create("s")
        store.append("s", b"not json {")
        with pytest.raises(ValueError, match="Cannot decode event"):
            replay(store, "s", CaptureExecutor(), handler_registry=reg)

    def test_unknown_stream_raises(self, store: StreamStore):
        with pytest.raises(KeyError):
            replay(store, "nope", CaptureExecutor())


# ---------------------------------------------------------------------------
# No handlers
# ---------------------------------------------------------------------------


class TestNoHandlers:
    def test_replay_with_no_handlers_just_counts_events(self, store: StreamStore):
        reg = HandlerRegistry()
        _seed_stream(store, "s", [{"id": 1}, {"id": 2}])
        ex = CaptureExecutor()

        result = replay(store, "s", ex, handler_registry=reg)

        assert result.events_processed == 2
        assert result.effects_applied == 0
        assert ex.batches == []


# ---------------------------------------------------------------------------
# Drift detection
# ---------------------------------------------------------------------------


class TestDrift:
    def test_no_drift_no_warning(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):  # noqa: ARG001
            return None

        reg.register("h", "s", h, 0, None)
        _seed_stream(store, "s", [{"id": 1}])

        result = replay(store, "s", CaptureExecutor(), handler_registry=reg)

        assert result.drift_detected == []
        assert result.warnings == []

    def test_handler_drift_warns_and_continues(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):  # noqa: ARG001
            return None

        version = reg.register("h", "s", h, 0, None)
        # Tamper with the stored hash to simulate the live source having drifted
        object.__setattr__(version, "source_hash", "deadbeef" * 8)

        _seed_stream(store, "s", [{"id": 1}, {"id": 2}])

        result = replay(store, "s", CaptureExecutor(), handler_registry=reg)

        # Warned, did not raise
        assert result.events_processed == 2
        assert "h" in result.drift_detected
        # One entry per drifted handler name (deduplicated)
        assert result.drift_detected.count("h") == 1
        assert any("RAKAIA_DRIFT" in w for w in result.warnings)

    def test_handler_drift_raises_when_strict(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):  # noqa: ARG001
            return None

        version = reg.register("h", "s", h, 0, None)
        object.__setattr__(version, "source_hash", "deadbeef" * 8)

        _seed_stream(store, "s", [{"id": 1}])

        with pytest.raises(HandlerDriftError, match="handler='h'"):
            replay(
                store,
                "s",
                CaptureExecutor(),
                handler_registry=reg,
                on_drift="raise",
            )

    def test_upcaster_drift_warns_and_continues(self, store: StreamStore):
        handlers = HandlerRegistry()
        upcasters = UpcasterRegistry()

        def h(event):  # noqa: ARG001
            return None

        def upcast(event):
            return {**event, "added": True}

        handlers.register("h", "s", h, 0, None)
        up_version = upcasters.register("s", 1, upcast)
        object.__setattr__(up_version, "source_hash", "deadbeef" * 8)

        _seed_stream(store, "s", [{"id": 1, "schema_version": 1}])

        result = replay(
            store,
            "s",
            CaptureExecutor(),
            handler_registry=handlers,
            upcaster_registry=upcasters,
        )

        assert result.events_processed == 1
        assert any("upcaster" in w for w in result.warnings)

    def test_upcaster_drift_raises_when_strict(self, store: StreamStore):
        handlers = HandlerRegistry()
        upcasters = UpcasterRegistry()

        def h(event):  # noqa: ARG001
            return None

        def upcast(event):
            return {**event, "added": True}

        handlers.register("h", "s", h, 0, None)
        up_version = upcasters.register("s", 1, upcast)
        object.__setattr__(up_version, "source_hash", "deadbeef" * 8)

        _seed_stream(store, "s", [{"id": 1, "schema_version": 1}])

        with pytest.raises(HandlerDriftError, match="upcaster"):
            replay(
                store,
                "s",
                CaptureExecutor(),
                handler_registry=handlers,
                upcaster_registry=upcasters,
                on_drift="raise",
            )
