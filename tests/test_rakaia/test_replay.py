"""End-to-end tests for the replay() orchestrator (without Django)."""

from __future__ import annotations

import json

import pytest

from rakaia.effects import ApplyReport, Effect
from rakaia.executors import InMemoryProjections
from rakaia.registry import (
    HandlerDriftError,
    HandlerGapError,
    HandlerRegistry,
    UpcasterRegistry,
)
from rakaia.replay import (
    ENVELOPE_TS,
    TouchedSubject,
    _synth_transitions,
    merge_replay,
    replay,
)
from rakaia.seed import seed_stream
from rakaia.store import StreamStore
from rakaia.types import AppendOptions


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
        seed_stream("stream", [{"id": 1, "name": "alice"}], store=store)
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
        seed_stream("stream", [{"id": 1}], store=store)
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
        seed_stream("stream", [{"id": 1}], store=store)
        ex = CaptureExecutor()

        replay(store, "stream", ex, handler_registry=reg)
        assert len(ex.all_effects) == 2

    def test_handler_returning_none_emits_no_effects(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):  # noqa: ARG001
            return None

        reg.register("h", "stream", h, 0, None)
        seed_stream("stream", [{"id": 1}], store=store)
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
        seed_stream(
            "s",
            [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
            store=store,
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
        seed_stream("s", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], store=store)

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
        seed_stream("s", [{"x": 1}], store=store)
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
        seed_stream("s", [{"x": 1}], store=store)
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
        seed_stream("s", [{"id": 1, "schema_version": 1}], store=store)
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
        seed_stream("s", [{"id": i} for i in range(5)], store=store)

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
        seed_stream("s", [{"id": i} for i in range(5)], store=store)

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
        seed_stream("s", [{"id": i} for i in range(6)], store=store)

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
        seed_stream("s", [{"id": 1}, {"id": 2}], store=store)
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
        seed_stream("s", [{"id": 1}], store=store)

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

        seed_stream("s", [{"id": 1}, {"id": 2}], store=store)

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

        seed_stream("s", [{"id": 1}], store=store)

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

        seed_stream("s", [{"id": 1, "schema_version": 1}], store=store)

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

        seed_stream("s", [{"id": 1, "schema_version": 1}], store=store)

        with pytest.raises(HandlerDriftError, match="upcaster"):
            replay(
                store,
                "s",
                CaptureExecutor(),
                handler_registry=handlers,
                upcaster_registry=upcasters,
                on_drift="raise",
            )


# ---------------------------------------------------------------------------
# Delete effects
# ---------------------------------------------------------------------------


class TestDeleteEffects:
    def test_delete_effect_reaches_executor(self, store: StreamStore):
        reg = HandlerRegistry()

        def h(event):
            return Effect(
                op="delete",
                model_label="x.X",
                lookup={"parent_id": event["id"]},
                exclude={"idx__in": event["keep"]},
            )

        reg.register("h", "stream", h, 0, None)
        seed_stream("stream", [{"id": 1, "keep": [0, 1]}], store=store)
        ex = CaptureExecutor()

        result = replay(store, "stream", ex, handler_registry=reg)

        assert result.effects_applied == 1
        assert len(ex.all_effects) == 1
        eff = ex.all_effects[0]
        assert eff.op == "delete"
        assert eff.lookup == {"parent_id": 1}
        assert eff.exclude == {"idx__in": [0, 1]}


# ---------------------------------------------------------------------------
# Content-based routing (match_field)
# ---------------------------------------------------------------------------


class TestMatchFieldRouting:
    def test_replay_routes_by_form_type(self, store: StreamStore):
        reg = HandlerRegistry()

        def tf(event):
            return Effect(
                op="update_or_create",
                model_label="x.TF",
                lookup={"id": event["id"]},
                defaults={},
            )

        def sf(event):
            return Effect(
                op="update_or_create",
                model_label="x.SF",
                lookup={"id": event["id"]},
                defaults={},
            )

        # Both handlers live on the same stream; routing is by payload field.
        reg.register("tf", "tf_*", tf, 0, None, match_field="form_type")
        reg.register("sf", "sf_*", sf, 0, None, match_field="form_type")

        seed_stream(
            "submissions",
            [
                {"id": 1, "form_type": "tf_611"},
                {"id": 2, "form_type": "sf_12"},
                {"id": 3, "form_type": "tf_611"},
            ],
            store=store,
        )
        ex = CaptureExecutor()
        replay(store, "submissions", ex, handler_registry=reg)

        models = [(e.model_label, e.lookup["id"]) for e in ex.all_effects]
        assert ("x.TF", 1) in models
        assert ("x.SF", 2) in models
        assert ("x.TF", 3) in models
        assert ("x.SF", 1) not in models
        assert ("x.TF", 2) not in models


# ---------------------------------------------------------------------------
# Staged replay (stage= handlers + a projection reader)
# ---------------------------------------------------------------------------


def _project(event, reader):  # noqa: ARG001  (stage 0 ignores reader — see call)
    return Effect(
        op="update_or_create",
        model_label="app.Project",
        lookup={"suku": event["suku"], "output": event["output"]},
        defaults={"name": event["project_name"]},
    )


def _sf12(event, reader):
    project = reader.get("app.Project", suku=event["suku"], output=event["output"])
    return Effect(
        op="update_or_create",
        model_label="app.Sf12",
        lookup={"submission_id": event["key"]},
        defaults={"project_id": project.name if project else None},
    )


# TF (defines the project) deliberately arrives AFTER the SF that needs it.
_STAGED_EVENTS = [
    {
        "schema_version": 1,
        "form_type": "SF_1_2",
        "key": "sf-1",
        "suku": "Fatuberliu",
        "output": "WATER",
    },
    {
        "schema_version": 1,
        "form_type": "TF_6_1_1",
        "key": "tf-1",
        "suku": "Fatuberliu",
        "output": "WATER",
        "project_name": "WS-014",
    },
]


def _staged_registry():
    reg = HandlerRegistry()
    # stage 0 is defined with a reader param but registered at stage 0, so it is
    # called `fn(event)`; that must not error even though the fn accepts reader.
    reg.register(
        "project",
        "TF_6_1_1",
        lambda event: _project(event, None),
        0,
        None,
        match_field="form_type",
        stage=0,
    )
    reg.register("sf12", "SF_1_2", _sf12, 0, None, match_field="form_type", stage=1)
    return reg


class TestStagedReplay:
    def test_late_reference_still_links(self, store: StreamStore):
        seed_stream("s", _STAGED_EVENTS, store=store)
        reg = _staged_registry()
        proj = InMemoryProjections()
        result = replay(
            store,
            "s",
            proj,
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        # The SF arrived before its TF, yet stage 0 built every Project before
        # stage 1 linked, so the link resolves.
        sf = proj.get("app.Sf12", submission_id="sf-1")
        assert sf.project_id == "WS-014"
        assert result.events_processed == 2  # counted once, not per stage

    def test_missing_reader_raises(self, store: StreamStore):
        seed_stream("s", _STAGED_EVENTS, store=store)
        reg = _staged_registry()
        with pytest.raises(ValueError, match="reader"):
            replay(
                store,
                "s",
                InMemoryProjections(),
                handler_registry=reg,
                upcaster_registry=UpcasterRegistry(),
            )  # no reader=

    def test_self_heals_on_re_replay(self, store: StreamStore):
        # Only the SF exists: its project is unresolved.
        seed_stream("s", [_STAGED_EVENTS[0]], store=store)
        reg = _staged_registry()
        proj = InMemoryProjections()
        replay(
            store,
            "s",
            proj,
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        assert proj.get("app.Sf12", submission_id="sf-1").project_id is None

        # The TF finally arrives; re-replaying links it — no backfill.
        store.append("s", json.dumps(_STAGED_EVENTS[1]).encode("utf-8"))
        proj2 = InMemoryProjections()
        replay(
            store,
            "s",
            proj2,
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=proj2,
        )
        assert proj2.get("app.Sf12", submission_id="sf-1").project_id == "WS-014"

    def test_stage_zero_only_is_single_pass_without_reader(self, store: StreamStore):
        # A registry with only stage-0 handlers needs no reader (backward compat).
        seed_stream("s", [_STAGED_EVENTS[1]], store=store)
        reg = HandlerRegistry()
        reg.register(
            "project",
            "TF_6_1_1",
            lambda event: _project(event, None),
            0,
            None,
            match_field="form_type",
            stage=0,
        )
        proj = InMemoryProjections()
        replay(
            store, "s", proj, handler_registry=reg, upcaster_registry=UpcasterRegistry()
        )  # no reader needed
        assert proj.get("app.Project", suku="Fatuberliu", output="WATER") is not None


# ---------------------------------------------------------------------------
# Reducers (per-stage recompute steps)
# ---------------------------------------------------------------------------


def _finance_line(event):
    return Effect(
        op="update_or_create",
        model_label="app.FinanceLine",
        lookup={"submission_id": event["key"]},
        defaults={"suku": event["suku"], "delta": event["delta"]},
    )


def _balance_reducer(reader):
    from rakaia.projections import reconcile_aggregate

    groups: dict[str, int] = {}
    for line in reader.query("app.FinanceLine"):
        groups[line.suku] = groups.get(line.suku, 0) + line.delta
    return reconcile_aggregate(
        "app.Balance", {}, "suku", {s: {"total": t} for s, t in groups.items()}
    )


_FINANCE_EVENTS = [
    {
        "schema_version": 1,
        "form_type": "FINANCE",
        "key": "f1",
        "suku": "A",
        "delta": 100,
    },
    {
        "schema_version": 1,
        "form_type": "FINANCE",
        "key": "f2",
        "suku": "A",
        "delta": -30,
    },
    {
        "schema_version": 1,
        "form_type": "FINANCE",
        "key": "f3",
        "suku": "B",
        "delta": 50,
    },
]


def _finance_registry(reducer=_balance_reducer):
    reg = HandlerRegistry()
    reg.register(
        "finance", "FINANCE", _finance_line, 0, None, match_field="form_type", stage=0
    )
    reg.register_reducer("balance", 1, reducer)
    return reg


class TestReducers:
    def test_reducer_recomputes_aggregate(self, store: StreamStore):
        seed_stream("s", _FINANCE_EVENTS, store=store)
        proj = InMemoryProjections()
        replay(
            store,
            "s",
            proj,
            handler_registry=_finance_registry(),
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        assert proj.get("app.Balance", suku="A").total == 70  # 100 - 30
        assert proj.get("app.Balance", suku="B").total == 50

    def test_reducer_runs_once_per_stage_not_per_event(self, store: StreamStore):
        seed_stream("s", _FINANCE_EVENTS, store=store)  # 3 events
        calls: list[int] = []

        def counting(reader):  # noqa: ARG001
            calls.append(1)
            return []

        proj = InMemoryProjections()
        replay(
            store,
            "s",
            proj,
            handler_registry=_finance_registry(counting),
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        assert len(calls) == 1  # once for stage 1, not 3x (per event)

    def test_reducer_requires_reader(self, store: StreamStore):
        seed_stream("s", _FINANCE_EVENTS, store=store)
        with pytest.raises(ValueError, match="reader"):
            replay(
                store,
                "s",
                InMemoryProjections(),
                handler_registry=_finance_registry(),
                upcaster_registry=UpcasterRegistry(),
            )  # no reader=

    def test_reducer_recompute_is_replay_safe(self, store: StreamStore):
        seed_stream("s", _FINANCE_EVENTS, store=store)
        reg = _finance_registry()
        proj = InMemoryProjections()
        replay(
            store,
            "s",
            proj,
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        # Re-replay onto existing state: recompute, not increment -> stable.
        replay(
            store,
            "s",
            proj,
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        assert proj.get("app.Balance", suku="A").total == 70  # not 140

    def test_reducer_sees_only_committed_stage0_rows(self, store: StreamStore):
        # A reducer at stage 1 reads FinanceLine rows all built in stage 0.
        seed_stream("s", _FINANCE_EVENTS, store=store)
        proj = InMemoryProjections()
        replay(
            store,
            "s",
            proj,
            handler_registry=_finance_registry(),
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        assert len(proj.query("app.FinanceLine")) == 3
        assert {b.suku for b in proj.query("app.Balance")} == {"A", "B"}


def _capturing_reducer(sink: list):
    # A two-argument reducer opts in to the touched-subjects signal.
    def reducer(reader, touched):  # noqa: ARG001
        sink.append(touched)
        return []

    return reducer


class TestReducerTouchedSubjects:
    """A reducer that declares a second parameter receives the deterministic set
    of subjects the pass's per-event handlers wrote (#51)."""

    def test_two_arg_reducer_receives_touched_subjects(self, store: StreamStore):
        seed_stream("s", _FINANCE_EVENTS, store=store)  # keys f1, f2, f3
        sink: list = []
        proj = InMemoryProjections()
        replay(
            store,
            "s",
            proj,
            handler_registry=_finance_registry(_capturing_reducer(sink)),
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        (touched,) = sink
        # In event order, one subject per FinanceLine write, each identified by
        # the effect's (model_label, lookup).
        assert [(t.model_label, t.lookup) for t in touched] == [
            ("app.FinanceLine", {"submission_id": "f1"}),
            ("app.FinanceLine", {"submission_id": "f2"}),
            ("app.FinanceLine", {"submission_id": "f3"}),
        ]
        assert all(isinstance(t, TouchedSubject) for t in touched)

    def test_one_arg_reducer_is_called_unchanged(self, store: StreamStore):
        # A legacy fn(reader) reducer keeps working — no touched arg is forced.
        seed_stream("s", _FINANCE_EVENTS, store=store)
        proj = InMemoryProjections()
        replay(
            store,
            "s",
            proj,
            handler_registry=_finance_registry(_balance_reducer),
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        assert proj.get("app.Balance", suku="A").total == 70

    def test_touched_is_incremental_on_a_tail_replay(self, store: StreamStore):
        # Replaying only the tail touches only the tail's subjects — the same
        # reducer scopes to what the pass changed, no code change at the reducer.
        seed_stream("s", _FINANCE_EVENTS, store=store)
        sink: list = []
        proj = InMemoryProjections()
        replay(
            store,
            "s",
            proj,
            handler_registry=_finance_registry(_capturing_reducer(sink)),
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
            start_seq=2,  # only f3
        )
        (touched,) = sink
        assert [t.lookup for t in touched] == [{"submission_id": "f3"}]

    def test_touched_excludes_reducer_outputs(self, store: StreamStore):
        # touched reflects per-event *handler* writes, not what an earlier
        # reducer at the same stage emitted. Two stage-1 reducers, name-ordered:
        # "a_writes" recomputes app.Balance; "b_captures" runs after and must not
        # see app.Balance in its touched set.
        seed_stream("s", _FINANCE_EVENTS, store=store)
        sink: list = []
        reg = HandlerRegistry()
        reg.register(
            "finance", "FINANCE", _finance_line, 0, None, match_field="form_type"
        )
        reg.register_reducer("a_writes", 1, _balance_reducer)
        reg.register_reducer("b_captures", 1, _capturing_reducer(sink))
        proj = InMemoryProjections()
        replay(
            store,
            "s",
            proj,
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        (touched,) = sink
        assert proj.get("app.Balance", suku="A").total == 70  # a_writes ran first
        assert {t.model_label for t in touched} == {"app.FinanceLine"}

    def test_touched_deduplicates_repeated_subjects(self, store: StreamStore):
        # Two events writing the same subject appear once in touched.
        events = [
            {
                "schema_version": 1,
                "form_type": "FINANCE",
                "key": "f1",
                "suku": "A",
                "delta": 100,
            },
            {
                "schema_version": 1,
                "form_type": "FINANCE",
                "key": "f1",
                "suku": "A",
                "delta": 5,
            },
        ]
        seed_stream("s", events, store=store)
        sink: list = []
        proj = InMemoryProjections()
        replay(
            store,
            "s",
            proj,
            handler_registry=_finance_registry(_capturing_reducer(sink)),
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        (touched,) = sink
        assert [t.lookup for t in touched] == [{"submission_id": "f1"}]

    def test_touched_flows_through_merge_replay(self, store: StreamStore):
        # merge_replay shares the same pipeline; a two-arg reducer gets the
        # touched subjects merged across streams, in merged order.
        seed_stream(
            "fin/a",
            [
                {
                    "schema_version": 1,
                    "form_type": "FINANCE",
                    "key": "f1",
                    "suku": "A",
                    "delta": 100,
                    "ts": "2026-01-01T00:00:00Z",
                }
            ],
            store=store,
        )
        seed_stream(
            "fin/b",
            [
                {
                    "schema_version": 1,
                    "form_type": "FINANCE",
                    "key": "f2",
                    "suku": "A",
                    "delta": -30,
                    "ts": "2026-01-01T01:00:00Z",
                }
            ],
            store=store,
        )
        sink: list = []
        proj = InMemoryProjections()
        merge_replay(
            store,
            ["fin/a", "fin/b"],
            proj,
            handler_registry=_finance_registry(_capturing_reducer(sink)),
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        (touched,) = sink
        assert {t.lookup["submission_id"] for t in touched} == {"f1", "f2"}


# ---------------------------------------------------------------------------
# merge_replay (several streams merged by an order key)
# ---------------------------------------------------------------------------


def _touch(event):
    return Effect(
        op="update_or_create",
        model_label="app.Claim",
        lookup={"slot": event["slot"]},
        defaults={"claimed_by": event["key"]},
    )


def _touch_registry():
    reg = HandlerRegistry()
    reg.register("touch", "TOUCH", _touch, 0, None, match_field="form_type", stage=0)
    return reg


def _ev(key, ts):
    return {
        "schema_version": 1,
        "form_type": "TOUCH",
        "key": key,
        "slot": "S",
        "ts": ts,
    }


# a1 and b1 share a timestamp across streams (the cross-stream tie); they are
# also the two latest events, so the tie's winner is the final claimant.
_A = [_ev("a0", "2026-01-01T00:00:00Z"), _ev("a1", "2026-01-01T02:00:00Z")]
_B = [_ev("b0", "2026-01-01T01:00:00Z"), _ev("b1", "2026-01-01T02:00:00Z")]
# Canonical merged order by (ts, path, offset): a0, b0, a1, b1  (forms/a < forms/b)
_CANONICAL = [_A[0], _B[0], _A[1], _B[1]]


class TestMergeReplay:
    def _seed_two(self, store: StreamStore):
        seed_stream("forms/a", _A, store=store)
        seed_stream("forms/b", _B, store=store)

    def test_tie_resolved_by_stream_path(self, store: StreamStore):
        self._seed_two(store)
        proj = InMemoryProjections()
        merge_replay(
            store,
            ["forms/a", "forms/b"],
            proj,
            handler_registry=_touch_registry(),
            upcaster_registry=UpcasterRegistry(),
        )
        # a1 (forms/a) sorts before b1 (forms/b) at the same ts, so b1 is later
        # in the merged order and wins the shared slot.
        assert proj.get("app.Claim", slot="S").claimed_by == "b1"

    def test_deterministic_regardless_of_path_order(self, store: StreamStore):
        self._seed_two(store)
        proj = InMemoryProjections()
        merge_replay(
            store,
            ["forms/b", "forms/a"],
            proj,  # reversed
            handler_registry=_touch_registry(),
            upcaster_registry=UpcasterRegistry(),
        )
        assert proj.get("app.Claim", slot="S").claimed_by == "b1"  # unchanged

    def test_parity_with_single_combined_stream(self, store: StreamStore):
        self._seed_two(store)
        merged = InMemoryProjections()
        merge_replay(
            store,
            ["forms/a", "forms/b"],
            merged,
            handler_registry=_touch_registry(),
            upcaster_registry=UpcasterRegistry(),
        )

        seed_stream("combined", _CANONICAL, store=store)
        single = InMemoryProjections()
        replay(
            store,
            "combined",
            single,
            handler_registry=_touch_registry(),
            upcaster_registry=UpcasterRegistry(),
        )

        assert (
            merged.get("app.Claim", slot="S").claimed_by
            == single.get("app.Claim", slot="S").claimed_by
            == "b1"
        )

    def test_missing_order_key_raises(self, store: StreamStore):
        seed_stream(
            "forms/a",
            [{"schema_version": 1, "form_type": "TOUCH", "key": "x", "slot": "S"}],
            store=store,
        )  # no ts
        with pytest.raises(ValueError, match="order_key"):
            merge_replay(
                store,
                ["forms/a"],
                InMemoryProjections(),
                handler_registry=_touch_registry(),
                upcaster_registry=UpcasterRegistry(),
            )

    def test_duplicate_stream_paths_raise(self, store: StreamStore):
        seed_stream("forms/a", _A, store=store)
        with pytest.raises(ValueError, match="duplicate"):
            merge_replay(
                store,
                ["forms/a", "forms/a"],  # same stream twice
                InMemoryProjections(),
                handler_registry=_touch_registry(),
                upcaster_registry=UpcasterRegistry(),
            )

    def test_incomparable_order_key_raises_clearly(self, store: StreamStore):
        # One stream's ts is an int, the other's a str -> sort would TypeError;
        # merge_replay should surface a clear ValueError, not the bare crash.
        seed_stream(
            "forms/a",
            [
                {
                    "schema_version": 1,
                    "form_type": "TOUCH",
                    "key": "a",
                    "slot": "S",
                    "ts": 1,
                }
            ],
            store=store,
        )
        seed_stream(
            "forms/b",
            [
                {
                    "schema_version": 1,
                    "form_type": "TOUCH",
                    "key": "b",
                    "slot": "S",
                    "ts": "2026-01-01T00:00:00Z",
                }
            ],
            store=store,
        )
        with pytest.raises(ValueError, match="not mutually comparable"):
            merge_replay(
                store,
                ["forms/a", "forms/b"],
                InMemoryProjections(),
                handler_registry=_touch_registry(),
                upcaster_registry=UpcasterRegistry(),
            )

    def test_requires_reader_when_staged(self, store: StreamStore):
        seed_stream("forms/a", _A, store=store)
        reg = _touch_registry()
        reg.register(
            "dep", "TOUCH", lambda *_: None, 0, None, match_field="form_type", stage=1
        )
        with pytest.raises(ValueError, match="reader"):
            merge_replay(
                store,
                ["forms/a"],
                InMemoryProjections(),
                handler_registry=reg,
                upcaster_registry=UpcasterRegistry(),
            )

    def test_reducer_runs_over_merged_streams(self, store: StreamStore):
        seed_stream(
            "fin/a",
            [
                {
                    "schema_version": 1,
                    "form_type": "FINANCE",
                    "key": "f1",
                    "suku": "A",
                    "delta": 100,
                    "ts": "2026-01-01T00:00:00Z",
                }
            ],
            store=store,
        )
        seed_stream(
            "fin/b",
            [
                {
                    "schema_version": 1,
                    "form_type": "FINANCE",
                    "key": "f2",
                    "suku": "A",
                    "delta": -30,
                    "ts": "2026-01-01T01:00:00Z",
                },
                {
                    "schema_version": 1,
                    "form_type": "FINANCE",
                    "key": "f3",
                    "suku": "B",
                    "delta": 50,
                    "ts": "2026-01-01T02:00:00Z",
                },
            ],
            store=store,
        )
        proj = InMemoryProjections()
        merge_replay(
            store,
            ["fin/a", "fin/b"],
            proj,
            handler_registry=_finance_registry(),
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
        assert proj.get("app.Balance", suku="A").total == 70
        assert proj.get("app.Balance", suku="B").total == 50

    def test_order_by_envelope_ts_ignores_append_order(self, store: StreamStore):
        # Backfill scenario: events are appended in an order that differs from
        # their logical event order, and the payloads carry NO "ts" field — so
        # ordering can only come from the producer-set envelope event_ts.
        def _touch(key: str, logical_ts: float):
            event = {
                "schema_version": 1,
                "form_type": "TOUCH",
                "key": key,
                "slot": "S",
            }
            return event, AppendOptions(event_ts=logical_ts)

        # Append order (transport) would make b1 the last write; but by logical
        # envelope ts the last event is a1 (400), so a1 must win the slot.
        seed_stream("forms/a", [_touch("a0", 100.0), _touch("a1", 400.0)], store=store)
        seed_stream("forms/b", [_touch("b0", 200.0), _touch("b1", 300.0)], store=store)

        proj = InMemoryProjections()
        merge_replay(
            store,
            ["forms/a", "forms/b"],
            proj,
            order_key=ENVELOPE_TS,
            handler_registry=_touch_registry(),
            upcaster_registry=UpcasterRegistry(),
        )
        assert proj.get("app.Claim", slot="S").claimed_by == "a1"

    def test_envelope_ts_missing_raises(self):
        # A hand-built message with event_ts=None under ENVELOPE_TS is a clear error.
        from rakaia.types import StreamMessage

        class _HandBuilt:
            def read(self, path, offset=None):  # noqa: ARG002
                return [
                    StreamMessage(
                        data=b'{"form_type":"TOUCH","key":"x","slot":"S"}',
                        offset="0",
                        timestamp=1.0,
                        event_ts=None,
                    )
                ], True

        with pytest.raises(ValueError, match="envelope event_ts"):
            merge_replay(
                _HandBuilt(),
                ["forms/a"],
                InMemoryProjections(),
                order_key=ENVELOPE_TS,
                handler_registry=_touch_registry(),
                upcaster_registry=UpcasterRegistry(),
            )


class TestSynthTransitions:
    """`_synth_transitions` turns an executor's retire-flip report into one
    external transition per flipped row (issue #32)."""

    @staticmethod
    def _retire(patch):
        return Effect(
            op="retire",
            model_label="app.Alert",
            lookup={"stream_key": "s"},
            patch=patch,
            transition_kind="alert_transition",
            transition_key_fields=("alert_type",),
        )

    def test_one_transition_per_flipped_row(self):
        eff = self._retire({"resolved_at": "t2", "resolved_by": "system"})
        rows = [
            {"stream_key": "s", "alert_type": "a"},
            {"stream_key": "s", "alert_type": "b"},
        ]
        out = _synth_transitions(ApplyReport(retire_flips=[(eff, rows)]))
        assert [e.payload["key"]["alert_type"] for e in out] == ["a", "b"]
        assert all(e.kind == "alert_transition" for e in out)
        assert all(e.payload["state"] == "resolved" for e in out)
        assert all(e.payload["resolved_by"] == "system" for e in out)

    def test_patch_columns_never_clobber_identity_or_state(self):
        # A generic reconcile whose soft-delete patch touches columns literally
        # named `key`/`state` must not overwrite the transition's row identity
        # or its resolved state.
        eff = self._retire({"state": "archived", "key": "nope", "resolved_at": "t"})
        rows = [{"stream_key": "s", "alert_type": "a"}]
        (t,) = _synth_transitions(ApplyReport(retire_flips=[(eff, rows)]))
        assert t.payload["key"] == {"stream_key": "s", "alert_type": "a"}
        assert t.payload["state"] == "resolved"
        assert t.payload["resolved_at"] == "t"  # non-colliding patch cols kept

    def test_none_report_yields_nothing(self):
        assert _synth_transitions(None) == []
