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
from rakaia.effects import Effect
from rakaia.projections import reconcile_by_key

from .models import Alert

ALERT = "test_django_rakaia.Alert"

# The machine-owned alert types (partisipa's NON_USER_RESOLVABLE_FLAG_TYPES).
# Everything else — notably "alert" — is authored / user-resolvable.
MACHINE_TYPES = ["ff4_operational_exceeds_ksp", "sf11_smt_exceeds_cap"]


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
        Effect(
            op="update_or_create",
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
        Effect(
            op="external",
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
        Effect(
            op="update_or_create",
            model_label=ALERT,
            lookup=key,
            defaults={"resolved_at": ev["ts"], "resolved_by": ev.get("actor")},
        ),
        Effect(
            op="external",
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
    stream_key: str, violations: list[dict], ts: str
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
        },
        retire_filter={"alert_type__in": MACHINE_TYPES},
        retire={"resolved_at": ts, "resolved_by": "system"},
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
