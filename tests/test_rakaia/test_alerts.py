"""Authored-alert replay discipline (Phase 1, no Django).

Proves the notification half of the authored layer: an ``alert_raised`` /
``alert_dismissed`` handler emits an ``ExternalEffect`` transition alongside its
natural-key upsert, and replay hands that back in ``ReplayResult.external``
instead of applying it (one transition per real state change, **never delivered
by the replay itself**) while the materialized upsert is idempotent.
"""

from __future__ import annotations

from rakaia.effects import AnyEffect, Effect, ExternalEffect, Upsert
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.seed import seed_stream
from rakaia.store import StreamStore

ALERT = "app.Alert"


def _key(ev: dict) -> dict:
    return {
        "stream_key": ev["stream_key"],
        "alert_type": ev["alert_type"],
        "field_key": ev.get("field_key", ""),
    }


def alert_authored(event: dict) -> list[AnyEffect]:
    """Single authored handler: ``op`` in the event selects raise vs dismiss."""
    key = _key(event)
    if event["op"] == "dismiss":
        upsert = Upsert(
            model_label=ALERT,
            lookup=key,
            defaults={"resolved_at": event["ts"], "resolved_by": event.get("actor")},
        )
        state = "resolved"
    else:
        upsert = Upsert(
            model_label=ALERT,
            lookup=key,
            defaults={"message": event.get("message", ""), "resolved_at": None},
        )
        state = "open"
    return [
        upsert,
        ExternalEffect(kind="alert_transition", payload={"key": key, "state": state}),
    ]


class CaptureExecutor:
    def __init__(self) -> None:
        self.batches: list[list[Effect]] = []

    def apply(self, effects):
        self.batches.append(list(effects))

    @property
    def all_effects(self) -> list[Effect]:
        return [e for b in self.batches for e in b]


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
    def test_transitions_never_reach_the_executor(self):
        store = StreamStore()
        seed_stream("sub:1:alerts", _EVENTS, store=store)
        ex = CaptureExecutor()

        result = replay(store, "sub:1:alerts", ex, handler_registry=_registry())

        # Two external transitions produced; none of them reach the executor.
        assert len(result.external) == 2
        assert all(not isinstance(e, ExternalEffect) for e in ex.all_effects)
        # The materialized upserts still ran.
        assert sum(1 for e in ex.all_effects if isinstance(e, Upsert)) == 2

    def test_transitions_are_returned_to_the_caller(self):
        store = StreamStore()
        seed_stream("sub:1:alerts", _EVENTS, store=store)

        result = replay(
            store, "sub:1:alerts", CaptureExecutor(), handler_registry=_registry()
        )

        # The caller gets the effects themselves, in handler order — enough to
        # deliver them, which a bare skipped-count never was.
        assert [e.kind for e in result.external] == [
            "alert_transition",
            "alert_transition",
        ]
        assert [e.payload["state"] for e in result.external] == ["open", "resolved"]

    def test_rereplay_is_idempotent_and_silent(self):
        store = StreamStore()
        seed_stream("sub:1:alerts", _EVENTS, store=store)
        reg = _registry()

        first = replay(store, "sub:1:alerts", CaptureExecutor(), handler_registry=reg)
        second = replay(store, "sub:1:alerts", CaptureExecutor(), handler_registry=reg)

        # Same effect shape each pass; a rebuild delivers nothing on its own, so
        # the transitions cannot re-spam however often it is run.
        assert first.effects_applied == second.effects_applied
        assert len(second.external) == 2
