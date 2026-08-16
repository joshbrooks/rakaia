"""Alerts projection tests.

Phase 1 (this file, ``TestAuthoredAlerts``) proves the *authored* alert layer
needs **zero** rakaia-core changes: raise/dismiss are plain natural-key
``update_or_create`` upserts on the soft-delete ``Alert`` row.

Phase 2 (``TestMachineReconciledAlerts``) exercises the new ``reconcile_by_key``
+ ``retire`` primitives through the DjangoExecutor — including the headline
**zero-clobber** property: a machine reconcile scoped to machine ``alert_type``s
never touches authored alerts.
"""

from __future__ import annotations

from typing import Any

import pytest

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from rakaia.effects import Effect, ExternalEffect, Upsert
from rakaia.projections import reconcile_by_key
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.seed import seed_stream

from .models import Alert

ALERT = "test_django_rakaia.Alert"


class _RecordingExecutor(DjangoExecutor):
    """DjangoExecutor that records every effect handed to it, so a test can
    assert that no ExternalEffect ever arrives."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.seen: list[Effect] = []

    def apply(self, effects):
        effects = list(effects)
        self.seen.extend(effects)
        return super().apply(effects)


# The machine-owned alert types (partisipa's NON_USER_RESOLVABLE_FLAG_TYPES) —
# non-user-resolvable, so they ignore dismissals (machine wins).
MACHINE_TYPES = ["ff4_operational_exceeds_ksp", "sf11_smt_exceeds_cap"]

# Rule warnings a human may dismiss (partisipa's user-resolvable flag types).
USER_RESOLVABLE_TYPES = [
    "tf1321_monotonic",
    "tf1321_duplicate_month",
    "aldeia_mismatch",
]

# The reconcile domain = every rule-derived type. Authored-only types (e.g.
# "alert") are outside it, so the reconcile never touches them (zero clobber).
RULE_TYPES = MACHINE_TYPES + USER_RESOLVABLE_TYPES


def _key(ev: dict[str, Any]) -> dict[str, Any]:
    return {
        "stream_key": ev["stream_key"],
        "alert_type": ev["alert_type"],
        "field_key": ev.get("field_key", ""),
    }


def raise_effects(ev: dict[str, Any]) -> list[Effect]:
    """Authored ``alert_raised`` -> open the natural-key row + notify."""
    key = _key(ev)
    return [
        Upsert(
            model_label=ALERT,
            lookup=key,
            defaults={
                "severity": ev.get("severity", "info"),
                "message": ev.get("message", ""),
                "created_at": ev["ts"],
                "resolved_at": None,
                "resolved_by": None,
            },
        ),
        ExternalEffect(
            kind="alert_transition",
            payload={"key": key, "state": "open", "actor": ev.get("actor")},
        ),
    ]


def dismiss_effects(ev: dict[str, Any]) -> list[Effect]:
    """Authored ``alert_dismissed`` -> soft-resolve the same key + notify.

    No ``retire`` op needed: an authored dismissal targets one exact key, so a
    plain upsert setting ``resolved_at`` to the *event* timestamp is enough.
    """
    key = _key(ev)
    return [
        Upsert(
            model_label=ALERT,
            lookup=key,
            defaults={"resolved_at": ev["ts"], "resolved_by": ev.get("actor")},
        ),
        ExternalEffect(
            kind="alert_transition",
            payload={"key": key, "state": "resolved", "actor": ev.get("actor")},
        ),
    ]


@pytest.mark.django_db
class TestAuthoredAlerts:
    def test_raise_creates_open_alert(self):
        DjangoExecutor().apply(
            raise_effects(
                {
                    "stream_key": "sub-1",
                    "alert_type": "alert",
                    "severity": "warning",
                    "message": "double-check the budget",
                    "actor": "josh",
                    "ts": "2026-07-20T09:00:00Z",
                }
            )
        )
        alert = Alert.objects.get(stream_key="sub-1", alert_type="alert")
        assert alert.is_open
        assert alert.severity == "warning"
        assert alert.message == "double-check the budget"

    def test_dismiss_resolves_and_retains_row(self):
        ev = {
            "stream_key": "sub-1",
            "alert_type": "alert",
            "ts": "2026-07-20T09:00:00Z",
        }
        ex = DjangoExecutor()
        ex.apply(raise_effects({**ev, "actor": "josh"}))
        ex.apply(dismiss_effects({**ev, "ts": "2026-07-20T10:00:00Z", "actor": "amy"}))

        # Retained (soft-delete), not removed.
        alert = Alert.objects.get(stream_key="sub-1", alert_type="alert")
        assert not alert.is_open
        assert alert.resolved_at == "2026-07-20T10:00:00Z"
        assert alert.resolved_by == "amy"

    def test_raise_is_idempotent(self):
        ev = {
            "stream_key": "sub-1",
            "alert_type": "alert",
            "ts": "2026-07-20T09:00:00Z",
        }
        ex = DjangoExecutor()
        ex.apply(raise_effects(ev))
        ex.apply(raise_effects(ev))
        assert Alert.objects.filter(stream_key="sub-1", alert_type="alert").count() == 1

    def test_field_keys_are_distinct_rows(self):
        ex = DjangoExecutor()
        for fk in ("budget", "progress"):
            ex.apply(
                raise_effects(
                    {
                        "stream_key": "sub-1",
                        "alert_type": "alert",
                        "field_key": fk,
                        "ts": "2026-07-20T09:00:00Z",
                    }
                )
            )
        assert Alert.objects.filter(stream_key="sub-1", alert_type="alert").count() == 2

    def test_reraise_after_dismiss_reopens(self):
        ev = {"stream_key": "sub-1", "alert_type": "alert"}
        ex = DjangoExecutor()
        ex.apply(raise_effects({**ev, "ts": "2026-07-20T09:00:00Z"}))
        ex.apply(dismiss_effects({**ev, "ts": "2026-07-20T10:00:00Z", "actor": "amy"}))
        ex.apply(raise_effects({**ev, "ts": "2026-07-20T11:00:00Z"}))

        alert = Alert.objects.get(stream_key="sub-1", alert_type="alert")
        assert alert.is_open  # a fresh authored raise clears the resolution
        assert Alert.objects.count() == 1


# ===========================================================================
# Phase 2 — machine-reconciled alerts via reconcile_by_key + retire
# ===========================================================================


def _seed(stream_key: str, alert_type: str, *, field_key: str = "", resolved_at=None):
    return Alert.objects.create(
        stream_key=stream_key,
        alert_type=alert_type,
        field_key=field_key,
        resolved_at=resolved_at,
    )


def _machine_reconcile(
    stream_key: str,
    violations: list[dict],
    ts: str,
    *,
    transition_kind: str | None = None,
) -> list[Effect]:
    """One reconcile pass over the current machine violations for an entity."""
    return reconcile_by_key(
        model_label=ALERT,
        scope={"stream_key": stream_key},
        key_fields=("alert_type", "field_key"),
        items=violations,
        key_fn=lambda v: {
            "alert_type": v["alert_type"],
            "field_key": v.get("field_key", ""),
        },
        defaults_fn=lambda v: {
            "severity": "error",
            "message": v.get("message", ""),
            "created_at": ts,
            "resolved_at": None,
            "resolved_by": None,
        },
        retire_filter={"alert_type__in": MACHINE_TYPES},
        retire={"resolved_at": ts, "resolved_by": "system"},
        transition_kind=transition_kind,
    )


@pytest.mark.django_db
class TestMachineReconciledAlerts:
    def test_upserts_current_violations(self):
        DjangoExecutor().apply(
            _machine_reconcile(
                "sub-1",
                [{"alert_type": "ff4_operational_exceeds_ksp", "field_key": "row0"}],
                ts="2026-07-20T09:00:00Z",
            )
        )
        alert = Alert.objects.get(stream_key="sub-1")
        assert alert.alert_type == "ff4_operational_exceeds_ksp"
        assert alert.is_open

    def test_retire_resolves_absent_machine_flag(self):
        # An open machine flag that no longer violates.
        _seed("sub-1", "sf11_smt_exceeds_cap")
        DjangoExecutor().apply(
            _machine_reconcile("sub-1", [], ts="2026-07-20T09:00:00Z")
        )
        alert = Alert.objects.get(stream_key="sub-1", alert_type="sf11_smt_exceeds_cap")
        assert not alert.is_open  # retired (soft-deleted), still present
        assert alert.resolved_at == "2026-07-20T09:00:00Z"
        assert alert.resolved_by == "system"

    def test_retire_spares_still_violating_key(self):
        _seed("sub-1", "sf11_smt_exceeds_cap", field_key="a")
        _seed("sub-1", "sf11_smt_exceeds_cap", field_key="b")
        DjangoExecutor().apply(
            _machine_reconcile(
                "sub-1",
                [{"alert_type": "sf11_smt_exceeds_cap", "field_key": "a"}],
                ts="2026-07-20T09:00:00Z",
            )
        )
        a = Alert.objects.get(stream_key="sub-1", field_key="a")
        b = Alert.objects.get(stream_key="sub-1", field_key="b")
        assert a.is_open  # still violating -> spared
        assert not b.is_open  # absent -> retired

    def test_zero_clobber_authored_untouched(self):
        # THE headline property (oracle criterion 2): a machine reconcile scoped
        # to machine types must never resolve an authored ("alert") flag.
        _seed("sub-1", "alert", field_key="")  # authored, open
        _seed("sub-1", "sf11_smt_exceeds_cap")  # machine, open, no longer violating
        DjangoExecutor().apply(
            _machine_reconcile("sub-1", [], ts="2026-07-20T09:00:00Z")
        )
        authored = Alert.objects.get(stream_key="sub-1", alert_type="alert")
        machine = Alert.objects.get(
            stream_key="sub-1", alert_type="sf11_smt_exceeds_cap"
        )
        assert authored.is_open  # untouched by the scoped retire
        assert not machine.is_open

    def test_retire_only_touches_open_rows(self):
        # A machine flag already resolved earlier must keep its original
        # resolution timestamp — the reconcile must not re-stamp it.
        _seed("sub-1", "sf11_smt_exceeds_cap", resolved_at="2026-07-19T00:00:00Z")
        DjangoExecutor().apply(
            _machine_reconcile("sub-1", [], ts="2026-07-20T09:00:00Z")
        )
        alert = Alert.objects.get(stream_key="sub-1", alert_type="sf11_smt_exceeds_cap")
        assert alert.resolved_at == "2026-07-19T00:00:00Z"  # not re-stamped

    def test_reconcile_scoped_per_entity(self):
        # Another entity's flags are out of scope and untouched.
        _seed("sub-2", "sf11_smt_exceeds_cap")
        DjangoExecutor().apply(
            _machine_reconcile("sub-1", [], ts="2026-07-20T09:00:00Z")
        )
        assert Alert.objects.get(stream_key="sub-2").is_open

    def test_retire_reopen_retire_cycle(self):
        # Regression: a flag retired, then re-violating (reopened), then no
        # longer violating must retire again. The open-guard keys off resolved_at
        # alone, so a stale resolved_by left on the reopened row does not wedge it
        # permanently open.
        viol = [{"alert_type": "sf11_smt_exceeds_cap"}]
        ex = DjangoExecutor()
        ex.apply(_machine_reconcile("sub-1", viol, ts="t1"))  # open
        ex.apply(_machine_reconcile("sub-1", [], ts="t2"))  # retire
        ex.apply(_machine_reconcile("sub-1", viol, ts="t3"))  # reopen
        assert Alert.objects.get(stream_key="sub-1").is_open

        ex.apply(_machine_reconcile("sub-1", [], ts="t4"))  # retire again
        row = Alert.objects.get(stream_key="sub-1")
        assert not row.is_open  # correctly retired despite the reopen cycle
        assert row.resolved_at == "t4"


# transaction=True: the retire-with-transition path takes a real row lock
# (`select_for_update()` in DjangoExecutor._retire) to keep the reported flip
# set in step with the rows the UPDATE flips. Only a committing test can hold
# that lock to its actual contract.
@pytest.mark.django_db(transaction=True)
class TestRetireFlipReport:
    """Issue #32 R3: the executor reports which rows a retire actually flipped
    (NULL->set) so the orchestrator can emit one transition per real resolution.
    Opt-in: only a retire carrying a ``Transition`` pays for the extra SELECT.
    """

    def test_apply_reports_flipped_open_rows_only(self):
        # b is open (will flip); c is already resolved (open-guard excludes it).
        _seed("sub-1", "sf11_smt_exceeds_cap", field_key="b")
        _seed(
            "sub-1",
            "sf11_smt_exceeds_cap",
            field_key="c",
            resolved_at="2026-01-01T00:00:00Z",
        )
        report = DjangoExecutor().apply(
            _machine_reconcile(
                "sub-1",
                [],
                ts="2026-07-20T09:00:00Z",
                transition_kind="alert_transition",
            )
        )
        assert len(report.retire_flips) == 1
        eff, rows = report.retire_flips[0]
        assert eff.transition is not None
        assert eff.transition.kind == "alert_transition"
        # Only the open row flipped; identity is the full natural key.
        assert rows == [
            {
                "stream_key": "sub-1",
                "alert_type": "sf11_smt_exceeds_cap",
                "field_key": "b",
            }
        ]

    def test_flip_order_is_deterministic(self):
        for fk in ("z", "a", "m"):
            _seed("sub-1", "sf11_smt_exceeds_cap", field_key=fk)
        report = DjangoExecutor().apply(
            _machine_reconcile("sub-1", [], ts="t", transition_kind="alert_transition")
        )
        _, rows = report.retire_flips[0]
        assert [r["field_key"] for r in rows] == ["a", "m", "z"]  # ordered by key

    def test_no_report_and_no_extra_query_without_a_transition(self):
        _seed("sub-1", "sf11_smt_exceeds_cap", field_key="b")
        report = DjangoExecutor().apply(
            _machine_reconcile("sub-1", [], ts="t")  # no transition_kind
        )
        assert report.retire_flips == []


# transaction=True: the retire-with-transition path takes a real row lock
# (`select_for_update()` in DjangoExecutor._retire) to keep the reported flip
# set in step with the rows the UPDATE flips. Only a committing test can hold
# that lock to its actual contract.
@pytest.mark.django_db(transaction=True)
class TestMachineResolutionTransitions:
    """Issue #32 R4 (headline): replaying machine reconciles produces exactly one
    ``alert_transition`` per *real* resolution, returned to the caller in
    ``ReplayResult.external`` rather than applied — so a rebuild never
    re-spams."""

    @staticmethod
    def _registry() -> HandlerRegistry:
        reg = HandlerRegistry()

        def reconcile_h(ev):
            return _machine_reconcile(
                ev["stream_key"],
                ev["violations"],
                ev["ts"],
                transition_kind="alert_transition",
            )

        reg.register("reconcile_h", "sub:1", reconcile_h, 0, None)
        return reg

    # A = still violating on the 2nd pass; B = cleared -> the one real resolution.
    A = {"alert_type": "ff4_operational_exceeds_ksp", "field_key": "a"}
    B = {"alert_type": "sf11_smt_exceeds_cap", "field_key": "b"}

    @property
    def _events(self) -> list[dict]:
        return [
            {"stream_key": "sub-1", "ts": "t1", "violations": [self.A, self.B]},
            {"stream_key": "sub-1", "ts": "t2", "violations": [self.A]},
        ]

    def test_one_transition_per_real_resolution(self):
        store = seed_stream("sub:1", self._events)
        result = replay(
            store, "sub:1", DjangoExecutor(), handler_registry=self._registry()
        )
        assert len(result.external) == 1
        t = result.external[0]
        assert t.kind == "alert_transition"
        assert t.payload["state"] == "resolved"
        assert t.payload["key"] == {
            "stream_key": "sub-1",
            "alert_type": "sf11_smt_exceeds_cap",
            "field_key": "b",
        }
        assert t.payload["resolved_by"] == "system"
        assert t.payload["resolved_at"] == "t2"

    def test_still_violating_key_emits_no_transition(self):
        store = seed_stream("sub:1", self._events)
        result = replay(
            store, "sub:1", DjangoExecutor(), handler_registry=self._registry()
        )
        fired = {t.payload["key"]["alert_type"] for t in result.external}
        assert "ff4_operational_exceeds_ksp" not in fired  # A stayed open

    def test_the_transition_never_reaches_the_executor(self):
        store = seed_stream("sub:1", self._events)
        ex = _RecordingExecutor()
        result = replay(store, "sub:1", ex, handler_registry=self._registry())
        assert not any(isinstance(e, ExternalEffect) for e in ex.seen)
        assert len(result.external) == 1

    def test_rebuild_delivers_nothing_by_itself(self):
        store = seed_stream("sub:1", self._events)
        reg = self._registry()
        for _ in range(2):  # a rebuild replays from scratch
            ex = _RecordingExecutor()
            result = replay(store, "sub:1", ex, handler_registry=reg)
            assert not any(isinstance(e, ExternalEffect) for e in ex.seen)
            assert len(result.external) == 1


# ===========================================================================
# Phase 3 — composition (derived ⊕ authored) via staged replay + reader
# ===========================================================================
#
# Two stages on the submission stream:
#   * stage 0 — authored `alert_raised` / `alert_dismissed` project Alert rows;
#     a dismissal records the data `version` it was made against.
#   * stage 1 — the rule reconcile reads the committed dismissals via the reader.
#     A user-resolvable violation with a standing dismissal (dismissed_version ≥
#     the current violation's version) is omitted from the reconcile items, so it
#     is neither re-opened (stays resolved) nor retired (the open-guard protects
#     an already-resolved row). Machine types ignore dismissals.


def _sev(alert_type: str) -> str:
    return "error" if alert_type in MACHINE_TYPES else "warning"


def authored_raise_h(event: dict[str, Any]) -> Effect:
    """stage 0: `alert_raised` -> open the natural-key row."""
    return Upsert(
        model_label=ALERT,
        lookup=_key(event),
        defaults={
            "severity": event.get("severity", "info"),
            "message": event.get("message", ""),
            "created_at": event["ts"],
            "resolved_at": None,
            "resolved_by": None,
            "dismissed_version": None,
        },
    )


def authored_dismiss_h(event: dict[str, Any]) -> Effect:
    """stage 0: `alert_dismissed` -> soft-resolve + record dismissed_version."""
    return Upsert(
        model_label=ALERT,
        lookup=_key(event),
        defaults={
            "resolved_at": event["ts"],
            "resolved_by": event.get("actor"),
            "dismissed_version": event["against_version"],
        },
    )


def rule_reconcile_h(event: dict[str, Any], reader: Any) -> list[Effect]:
    """stage 1: reconcile the current rule violations, honoring standing
    dismissals for user-resolvable types (authored wins) and ignoring them for
    machine types (machine wins)."""
    sk = event["stream_key"]
    version = event["version"]
    ts = event["ts"]

    # Standing dismissals for this entity, from committed stage-0 projections.
    dismissed = {
        (row.alert_type, row.field_key): row.dismissed_version
        for row in reader.filter(ALERT, stream_key=sk)
        if row.dismissed_version is not None
    }

    def stands(v: dict[str, Any]) -> bool:
        at = v["alert_type"]
        fk = v.get("field_key", "")
        return at in USER_RESOLVABLE_TYPES and dismissed.get((at, fk), -1) >= version

    # Omit still-dismissed violations: not re-opened, and not retired (the
    # already-resolved row is protected by reconcile_by_key's open-guard).
    items = [v for v in event["violations"] if not stands(v)]
    return reconcile_by_key(
        model_label=ALERT,
        scope={"stream_key": sk},
        key_fields=("alert_type", "field_key"),
        items=items,
        key_fn=lambda v: {
            "alert_type": v["alert_type"],
            "field_key": v.get("field_key", ""),
        },
        defaults_fn=lambda v: {
            "severity": _sev(v["alert_type"]),
            "message": v.get("message", ""),
            "created_at": ts,
            "resolved_at": None,
            "resolved_by": None,
            "dismissed_version": None,
        },
        retire_filter={"alert_type__in": RULE_TYPES},
        retire={"resolved_at": ts, "resolved_by": "system"},
    )


def _staged_registry() -> HandlerRegistry:
    reg = HandlerRegistry()
    reg.register(
        "authored_raise",
        "alert_raised",
        authored_raise_h,
        0,
        None,
        match_field="type",
        stage=0,
    )
    reg.register(
        "authored_dismiss",
        "alert_dismissed",
        authored_dismiss_h,
        0,
        None,
        match_field="type",
        stage=0,
    )
    reg.register(
        "rule_reconcile",
        "submission",
        rule_reconcile_h,
        0,
        None,
        match_field="type",
        stage=1,
    )
    return reg


def _run(events: list[dict], **kw: Any):
    store = seed_stream("sub:1", events)
    return replay(
        store,
        "sub:1",
        DjangoExecutor(),
        handler_registry=_staged_registry(),
        reader=DjangoProjectionReader(),
        **kw,
    )


def _submission(version: int, violations: list[dict], ts: str) -> dict:
    return {
        "type": "submission",
        "stream_key": "sub-1",
        "version": version,
        "violations": violations,
        "ts": ts,
    }


def _dismiss(
    alert_type: str, against_version: int, ts: str, field_key: str = ""
) -> dict:
    return {
        "type": "alert_dismissed",
        "stream_key": "sub-1",
        "alert_type": alert_type,
        "field_key": field_key,
        "against_version": against_version,
        "actor": "amy",
        "ts": ts,
    }


def _authored(alert_type: str, ts: str, field_key: str = "") -> dict:
    return {
        "type": "alert_raised",
        "stream_key": "sub-1",
        "alert_type": alert_type,
        "field_key": field_key,
        "severity": "warning",
        "ts": ts,
    }


_VIOL = [{"alert_type": "tf1321_monotonic", "field_key": "m1"}]


@pytest.mark.django_db
class TestComposedAlerts:
    def test_dismissed_warning_not_reraised(self):
        # Oracle criterion 3: a dismissed user-resolvable warning whose rule
        # still fails must NOT be re-raised.
        _run(
            [
                _submission(1, _VIOL, ts="t1"),
                _dismiss(
                    "tf1321_monotonic", against_version=1, ts="t2", field_key="m1"
                ),
            ]
        )
        row = Alert.objects.get(stream_key="sub-1", alert_type="tf1321_monotonic")
        assert not row.is_open  # dismissal honored
        assert row.resolved_by == "amy"

    def test_machine_type_ignores_dismissal(self):
        _run(
            [
                _submission(1, [{"alert_type": "sf11_smt_exceeds_cap"}], ts="t1"),
                _dismiss("sf11_smt_exceeds_cap", against_version=1, ts="t2"),
            ]
        )
        row = Alert.objects.get(stream_key="sub-1", alert_type="sf11_smt_exceeds_cap")
        assert row.is_open  # machine wins — dismissal ignored

    def test_newer_violation_supersedes_dismissal(self):
        _run(
            [
                _submission(1, _VIOL, ts="t1"),
                _dismiss(
                    "tf1321_monotonic", against_version=1, ts="t2", field_key="m1"
                ),
                _submission(2, _VIOL, ts="t3"),  # newer data, rule still fails
            ]
        )
        row = Alert.objects.get(stream_key="sub-1", alert_type="tf1321_monotonic")
        assert row.is_open  # v2 > dismissed v1 -> re-raised

    def test_zero_clobber_authored_untouched(self):
        _run(
            [
                _authored("alert", ts="t1"),  # authored informational, open
                _submission(1, [], ts="t2"),  # no rule violations
            ]
        )
        assert Alert.objects.get(stream_key="sub-1", alert_type="alert").is_open

    def test_full_replay_honors_dismissal(self):
        _run(
            [
                _submission(1, _VIOL, ts="t1"),
                _dismiss(
                    "tf1321_monotonic", against_version=1, ts="t2", field_key="m1"
                ),
            ]
        )
        assert not Alert.objects.get(alert_type="tf1321_monotonic").is_open

    def test_partial_replay_before_dismissal_leaves_open(self):
        # Determinism: refs is a function of events in range only. Replaying just
        # the submission (end_seq=1 excludes the dismissal) leaves the flag open.
        _run(
            [
                _submission(1, _VIOL, ts="t1"),
                _dismiss(
                    "tf1321_monotonic", against_version=1, ts="t2", field_key="m1"
                ),
            ],
            end_seq=1,
        )
        assert Alert.objects.get(alert_type="tf1321_monotonic").is_open
