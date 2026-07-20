"""Authored-alert replay discipline (Phase 1, no Django).

Proves the notification half of the authored layer: an ``alert_raised`` /
``alert_dismissed`` handler emits an ``op="external"`` transition alongside its
natural-key upsert, and replay drops that external effect by default (one
transition per real state change, **none on replay**) while the materialized
upsert is idempotent.
"""

from __future__ import annotations

import json

from rakaia.effects import Effect
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.store import StreamStore

ALERT = "app.Alert"


def _key(ev: dict) -> dict:
    return {
        "stream_key": ev["stream_key"],
        "alert_type": ev["alert_type"],
        "field_key": ev.get("field_key", ""),
    }


def alert_authored(event: dict) -> list[Effect]:
    """Single authored handler: ``op`` in the event selects raise vs dismiss."""
    key = _key(event)
    if event["op"] == "dismiss":
        upsert = Effect(
            op="update_or_create",
            model_label=ALERT,
            lookup=key,
            defaults={"resolved_at": event["ts"], "resolved_by": event.get("actor")},
        )
        state = "resolved"
    else:
        upsert = Effect(
            op="update_or_create",
            model_label=ALERT,
            lookup=key,
            defaults={"message": event.get("message", ""), "resolved_at": None},
        )
        state = "open"
    return [
        upsert,
        Effect(
            op="external", kind="alert_transition", payload={"key": key, "state": state}
        ),
    ]


class CaptureExecutor:
    def __init__(self) -> None:
        self.batches: list[list[Effect]] = []

    def apply(self, effects):
        self.batches.append(list(effects))

    @property
    def all_effects(self) -> list[Effect]:
        return [e for b in self.batches for e in b]


def _seed(store: StreamStore, path: str, events: list[dict]) -> None:
    store.create(path)
    for ev in events:
        store.append(path, json.dumps(ev).encode("utf-8"))


def _registry() -> HandlerRegistry:
    reg = HandlerRegistry()
    reg.register("alert_authored", "sub:1:alerts", alert_authored, 0, None)
    return reg


_EVENTS = [
    {
        "op": "raise",
        "stream_key": "sub-1",
        "alert_type": "alert",
        "message": "check",
        "ts": "t1",
    },
    {
        "op": "dismiss",
        "stream_key": "sub-1",
        "alert_type": "alert",
        "actor": "amy",
        "ts": "t2",
    },
]


class TestAuthoredAlertReplayDiscipline:
    def test_transitions_skipped_on_replay_by_default(self):
        store = StreamStore()
        _seed(store, "sub:1:alerts", _EVENTS)
        ex = CaptureExecutor()

        result = replay(store, "sub:1:alerts", ex, handler_registry=_registry())

        # Two external transitions produced, both skipped (none reach executor).
        assert result.external_effects_skipped == 2
        assert all(e.op != "external" for e in ex.all_effects)
        # The materialized upserts still ran.
        assert sum(1 for e in ex.all_effects if e.op == "update_or_create") == 2

    def test_transitions_delivered_when_included(self):
        store = StreamStore()
        _seed(store, "sub:1:alerts", _EVENTS)
        ex = CaptureExecutor()

        replay(
            store,
            "sub:1:alerts",
            ex,
            handler_registry=_registry(),
            include_external=True,
        )

        externals = [e for e in ex.all_effects if e.op == "external"]
        assert [e.payload["state"] for e in externals] == ["open", "resolved"]

    def test_rereplay_is_idempotent_and_silent(self):
        store = StreamStore()
        _seed(store, "sub:1:alerts", _EVENTS)
        reg = _registry()

        first = replay(store, "sub:1:alerts", CaptureExecutor(), handler_registry=reg)
        second = replay(store, "sub:1:alerts", CaptureExecutor(), handler_registry=reg)

        # Same effect shape each pass; externals never re-spam on the rebuild.
        assert first.effects_applied == second.effects_applied
        assert second.external_effects_skipped == 2
