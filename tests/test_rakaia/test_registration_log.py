"""The meta-stream, testable without decorators, imports or real handlers.

Registries persist what has been registered to a meta-stream, so a fresh process
can re-import the modules that hold the registrations. Three kinds of record
(handlers, reducers, upcasters) each had their own `_ensure_stream` /
`_load_persisted_ids` / `_persist_if_new` trio and their own hand-mirrored pair
of identity functions — one building a tuple from the object, one rebuilding the
same tuple from the stored JSON. Six functions that must agree, with nothing
checking that they do.

Worse, the tuples were read back **positionally**. `rehydrate()` did `ident[4]`
for a handler's dotted path and `ident[2]` for a reducer's and an upcaster's, and
two of the identity builders carried comments warning future editors to append
new fields at the end so those indices stayed valid. Adding one field to a
registration meant editing four functions and re-checking two index comments in
a third.

`RegistrationLog` owns the mechanism once, and each record kind owns its own
identity and serialization — so the round trip is a property of the type rather
than an agreement between two functions. These tests drive it against a bare
in-memory store: no `@register_handler`, no `hash_function_source`, no import
machinery. None of that was reachable before.
"""

from __future__ import annotations

import json

import pytest

from rakaia.registration_log import RegistrationLog
from rakaia.registry import HandlerVersion, ReducerVersion, UpcasterVersion
from rakaia.store import StreamStore

PATH = "_meta/test"


def _at(field: str) -> int:
    """Position of `field` in an identity tuple, from the declared field list."""
    return [f.name for f in HandlerVersion._PAYLOAD_FIELDS].index(field)


def _handler(
    name="h",
    event_match="orders",
    stage=0,
    match_field=None,
    registered_in="pkg.mod",
    dotted_path=None,
):
    return HandlerVersion(
        name=name,
        event_match=event_match,
        effective_from=0,
        effective_to=None,
        fn=lambda _event: [],
        dotted_path=dotted_path or f"pkg.mod.{name}",
        source_hash="abc123",
        stage=stage,
        match_field=match_field,
        registered_in=registered_in,
    )


def _log(store=None, kind=HandlerVersion):
    log = RegistrationLog(store or StreamStore(), PATH, kind)
    log.load()
    return log


class TestRecordingAndDedup:
    def test_a_new_record_is_appended(self):
        log = _log()
        assert log.record(_handler()) is True

    def test_the_same_record_twice_appends_once(self):
        log = _log()
        version = _handler()
        assert log.record(version) is True
        assert log.record(version) is False

    def test_an_equal_but_distinct_object_is_still_a_duplicate(self):
        """Dedup is by identity, not by object — two separate registrations of
        the same handler must not both persist."""
        log = _log()
        log.record(_handler())
        assert log.record(_handler()) is False

    def test_a_differing_field_is_a_different_record(self):
        log = _log()
        log.record(_handler(stage=0))
        assert log.record(_handler(stage=1)) is True

    def test_known_reports_what_has_been_recorded(self):
        log = _log()
        log.record(_handler(name="a"))
        log.record(_handler(name="b"))
        assert len(log.known()) == 2


class TestSurvivingAProcessRestart:
    def test_a_second_log_over_the_same_store_dedups_against_the_first(self):
        """The whole point of persisting: a fresh registry must not re-append
        what a previous process already recorded."""
        store = StreamStore()
        first = _log(store)
        first.record(_handler())

        second = _log(store)
        assert second.record(_handler()) is False
        assert len(second.known()) == 1

    def test_the_stream_holds_one_message_per_record(self):
        store = StreamStore()
        log = _log(store)
        log.record(_handler(name="a"))
        log.record(_handler(name="b"))
        log.record(_handler(name="a"))  # duplicate

        messages, _ = store.read(PATH)
        assert len(messages) == 2

    def test_the_stream_is_created_if_absent(self):
        store = StreamStore()
        assert store.has(PATH) is False
        _log(store)
        assert store.has(PATH) is True


class TestModulesAreResolvedByName:
    """`rehydrate()` used to pull the dotted path out of the identity tuple by
    index — `ident[4]` for handlers, `ident[2]` for reducers and upcasters — with
    comments warning editors to keep those positions stable. The log reads the
    field by name, so field order stops being load-bearing."""

    def test_modules_returns_the_importable_module_of_each_record(self):
        log = _log()
        log.record(_handler(name="a"))
        log.record(_handler(name="b"))
        assert log.modules() == {"pkg.mod"}

    def test_modules_survives_a_reload(self):
        store = StreamStore()
        _log(store).record(_handler())
        assert _log(store).modules() == {"pkg.mod"}

    def test_modules_names_the_registration_site_not_the_function(self):
        """The module to re-import is the one whose decorator ran. Those differ
        as soon as a function is wired up somewhere other than where it lives —
        the normal shape once a dependency is bound with `functools.partial`."""
        log = _log()
        log.record(_handler(registered_in="app.wiring"))
        assert log.modules() == {"app.wiring"}

    def test_a_method_qualname_does_not_leak_a_class_into_the_modules(self):
        """`pkg.mod.Class.method` chopped by one segment is `pkg.mod.Class`,
        which is not importable — the registration site sidesteps it."""
        log = _log()
        log.record(
            _handler(
                registered_in="app.wiring", dotted_path="pkg.mod.Projector.project"
            )
        )
        assert log.modules() == {"app.wiring"}


class TestIdentityRoundTrip:
    """The bug class the hand-mirrored function pairs invited: an object's
    identity and the identity rebuilt from its own stored payload must match."""

    @pytest.mark.parametrize(
        "version",
        [
            _handler(),
            _handler(stage=2),
            _handler(match_field="form_type"),
            _handler(event_match=frozenset({"a", "b"})),
        ],
        ids=["plain", "staged", "content-routed", "set-matched"],
    )
    def test_a_record_round_trips_through_its_payload(self, version):
        payload = version.to_payload()
        # Must survive JSON, not just a dict round trip — a frozenset does not.
        decoded = json.loads(json.dumps(payload))
        assert HandlerVersion.identity_from_payload(decoded) == version.identity

    def test_a_set_event_match_serializes_stably(self):
        """Sorted on the way out, so the meta-stream does not change with set
        iteration order — otherwise a restart re-appends the same handler."""
        one = _handler(event_match=frozenset({"b", "a"})).to_payload()
        two = _handler(event_match=frozenset({"a", "b"})).to_payload()
        assert one["event_match"] == two["event_match"] == ["a", "b"]


class TestBackwardCompatibilityWithOlderPayloads:
    """Meta-streams written by earlier versions are still out there and must
    keep loading — these fields were added after the format shipped."""

    def test_a_payload_without_match_field_loads(self):
        payload = _handler().to_payload()
        del payload["match_field"]
        assert HandlerVersion.identity_from_payload(payload)[_at("match_field")] is None

    def test_a_payload_without_stage_loads_as_stage_zero(self):
        payload = _handler().to_payload()
        del payload["stage"]
        assert HandlerVersion.identity_from_payload(payload)[_at("stage")] == 0

    def test_a_payload_without_a_registration_site_falls_back_to_the_module(self):
        """Meta-streams written before `registered_in` existed still restore —
        via the derivation they were written under, which is right whenever the
        function was registered where it was defined."""
        payload = _handler().to_payload()
        del payload["registered_in"]
        assert HandlerVersion.identity_from_payload(payload)[_at("registered_in")] == (
            "pkg.mod"
        )


class TestUnreadableMessagesAreSkipped:
    def test_a_non_json_message_does_not_break_loading(self):
        store = StreamStore()
        store.create(PATH)
        store.append(PATH, b"not json at all")
        log = _log(store)
        assert log.known() == set()
        assert log.record(_handler()) is True


class TestEveryRecordKind:
    """One mechanism, three kinds — the duplication this replaces."""

    def test_a_reducer_round_trips(self):
        reducer = ReducerVersion(
            name="r",
            stage=1,
            fn=lambda *_args: [],
            dotted_path="pkg.red.r",
            source_hash="deadbeef",
        )
        log = _log(kind=ReducerVersion)
        assert log.record(reducer) is True
        assert log.record(reducer) is False
        assert ReducerVersion.identity_from_payload(reducer.to_payload()) == (
            reducer.identity
        )

    def test_an_upcaster_round_trips(self):
        upcaster = UpcasterVersion(
            event_match="orders",
            from_version=1,
            fn=lambda e: e,
            dotted_path="pkg.up.u",
            source_hash="cafe",
            match_field=None,
        )
        log = _log(kind=UpcasterVersion)
        assert log.record(upcaster) is True
        assert log.record(upcaster) is False
        assert UpcasterVersion.identity_from_payload(upcaster.to_payload()) == (
            upcaster.identity
        )

    def test_each_kind_keeps_its_own_stream(self):
        store = StreamStore()
        handlers = RegistrationLog(store, "_meta/h", HandlerVersion)
        handlers.load()
        reducers = RegistrationLog(store, "_meta/r", ReducerVersion)
        reducers.load()

        handlers.record(_handler())
        assert reducers.known() == set()


class TestNoStore:
    """A registry with no backing store must still work — persistence is
    optional, and this was previously an `if self._store is None` at every one
    of the trio's call sites."""

    def test_recording_without_a_store_is_a_no_op(self):
        log = RegistrationLog(None, PATH, HandlerVersion)
        log.load()
        assert log.record(_handler()) is False
        assert log.known() == set()
        assert log.modules() == set()
