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
        _seed_stream(store, "stream", [{"id": 1, "keep": [0, 1]}])
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

        _seed_stream(
            store,
            "submissions",
            [
                {"id": 1, "form_type": "tf_611"},
                {"id": 2, "form_type": "sf_12"},
                {"id": 3, "form_type": "tf_611"},
            ],
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

from types import SimpleNamespace  # noqa: E402


class DictProjections:
    """In-memory materialiser: an Executor that applies update_or_create/delete
    effects, and a ProjectionReader over the same rows. Enough to exercise
    staged replay without Django — stage 1 reads what stage 0 committed."""

    def __init__(self) -> None:
        self.rows: dict[str, list[dict]] = {}

    # -- Executor side --
    def apply(self, effects) -> None:
        for e in effects:
            table = self.rows.setdefault(e.model_label, [])
            if e.op == "update_or_create":
                row = self._find(table, e.lookup)
                if row is None:
                    row = dict(e.lookup)
                    table.append(row)
                row.update(e.defaults or {})
            elif e.op == "delete":
                self.rows[e.model_label] = [
                    r
                    for r in table
                    if not (
                        self._match(r, e.lookup or {})
                        and not self._excluded(r, e.exclude or {})
                    )
                ]

    @staticmethod
    def _find(table, lookup):
        return next((r for r in table if DictProjections._match(r, lookup)), None)

    @staticmethod
    def _match(row, lookup):
        return all(row.get(k) == v for k, v in lookup.items())

    @staticmethod
    def _excluded(row, exclude):
        for key, val in exclude.items():
            if key.endswith("__in") and row.get(key[:-4]) in val:
                return True
        return False

    # -- ProjectionReader side --
    def get(self, model_label, **lookup):
        row = self._find(self.rows.get(model_label, []), lookup)
        return SimpleNamespace(**row) if row is not None else None

    def filter(self, model_label, **lookup):
        return [
            SimpleNamespace(**r)
            for r in self.rows.get(model_label, [])
            if self._match(r, lookup)
        ]

    def query(self, model_label):
        return [SimpleNamespace(**r) for r in self.rows.get(model_label, [])]


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
        _seed_stream(store, "s", _STAGED_EVENTS)
        reg = _staged_registry()
        proj = DictProjections()
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
        _seed_stream(store, "s", _STAGED_EVENTS)
        reg = _staged_registry()
        with pytest.raises(ValueError, match="reader"):
            replay(
                store,
                "s",
                DictProjections(),
                handler_registry=reg,
                upcaster_registry=UpcasterRegistry(),
            )  # no reader=

    def test_self_heals_on_re_replay(self, store: StreamStore):
        # Only the SF exists: its project is unresolved.
        _seed_stream(store, "s", [_STAGED_EVENTS[0]])
        reg = _staged_registry()
        proj = DictProjections()
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
        proj2 = DictProjections()
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
        _seed_stream(store, "s", [_STAGED_EVENTS[1]])
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
        proj = DictProjections()
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
        _seed_stream(store, "s", _FINANCE_EVENTS)
        proj = DictProjections()
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
        _seed_stream(store, "s", _FINANCE_EVENTS)  # 3 events
        calls: list[int] = []

        def counting(reader):  # noqa: ARG001
            calls.append(1)
            return []

        proj = DictProjections()
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
        _seed_stream(store, "s", _FINANCE_EVENTS)
        with pytest.raises(ValueError, match="reader"):
            replay(
                store,
                "s",
                DictProjections(),
                handler_registry=_finance_registry(),
                upcaster_registry=UpcasterRegistry(),
            )  # no reader=

    def test_reducer_recompute_is_replay_safe(self, store: StreamStore):
        _seed_stream(store, "s", _FINANCE_EVENTS)
        reg = _finance_registry()
        proj = DictProjections()
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
        _seed_stream(store, "s", _FINANCE_EVENTS)
        proj = DictProjections()
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
