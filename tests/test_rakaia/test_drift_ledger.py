"""The one drift check, seen from each interface it is reached through (#187).

Two distinct failure modes are pinned here, and they are not the same thing.

**The duplication.** "Has this rule's code changed since we recorded it?" used to
be answered by three near-identical blocks — one for handlers, one for reducers,
one handed to the upcaster chain as a callback — differing only in the word
naming the kind. Nothing compared them, and the reducer copy had *no* test at
all: `grep -ri drift tests/ | grep reducer` was empty before this file. That is
the recurring shape of this epic (#192) — two self-covering common paths hiding
an uncovered third. `TestEveryKindIsCheckedTheSameWay` reaches each kind through
`replay()`, the interface a caller actually uses, and asserts the three answers
are identical modulo the kind word.

**The mis-pairable options.** The upcaster chain used to take two options that
had to agree: `drift_callback` (report it) and `hasher` (memoise it). Passing one
and forgetting the other failed *silently* — the hasher alone skipped the check
entirely, the callback alone re-read the upcaster's source once per event. The
correct pairing existed in exactly one place, inside `replay()`.
`TestTheDriftOptionCannotBeHalfPassed` records the mis-pairing that is now
unrepresentable, and proves callers get both halves from the one object.
"""

from __future__ import annotations

import inspect

import pytest

from rakaia.drift import DriftLedger, HandlerDriftError
from rakaia.effects import Upsert
from rakaia.executors import InMemoryProjections
from rakaia.registry import HandlerRegistry, UpcasterRegistry, upcast
from rakaia.replay import merge_replay, replay
from rakaia.seed import seed_stream
from rakaia.source_hash import hash_function_source
from rakaia.store import StreamStore

STORED = "deadbeef" * 8
"""A stored hash no live source can match — the drift, simulated."""


def _handler(event: dict) -> Upsert:
    return Upsert(
        model_label="app.Row", lookup={"key": event.get("id", 0)}, defaults={}
    )


def _reducer(reader) -> None:  # noqa: ARG001
    return None


def _upcaster(event: dict) -> dict:
    return {**event, "added": True}


# ---------------------------------------------------------------------------
# The duplication: three kinds, one answer
# ---------------------------------------------------------------------------


def _drifted_handler_replay(store: StreamStore, **kwargs):
    handlers = HandlerRegistry()
    version = handlers.register("h", "s", _handler, 0, None)
    object.__setattr__(version, "source_hash", STORED)
    return (
        version.name,
        _handler,
        lambda: replay(
            store,
            "s",
            InMemoryProjections(),
            handler_registry=handlers,
            upcaster_registry=UpcasterRegistry(),
            **kwargs,
        ),
    )


def _drifted_reducer_replay(store: StreamStore, **kwargs):
    handlers = HandlerRegistry()
    handlers.register("h", "s", _handler, 0, None)
    version = handlers.register_reducer("r", 1, _reducer)
    object.__setattr__(version, "source_hash", STORED)
    proj = InMemoryProjections()
    return (
        version.name,
        _reducer,
        lambda: replay(
            store,
            "s",
            proj,
            handler_registry=handlers,
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
            **kwargs,
        ),
    )


def _drifted_upcaster_replay(store: StreamStore, **kwargs):
    handlers = HandlerRegistry()
    handlers.register("h", "s", _handler, 0, None)
    upcasters = UpcasterRegistry()
    version = upcasters.register("s", 1, _upcaster)
    object.__setattr__(version, "source_hash", STORED)
    return (
        version.dotted_path,
        _upcaster,
        lambda: replay(
            store,
            "s",
            InMemoryProjections(),
            handler_registry=handlers,
            upcaster_registry=upcasters,
            **kwargs,
        ),
    )


#: kind word -> a builder that drifts exactly one rule of that kind and returns
#: (reported name, the live function, a callable running the replay).
BUILDERS = {
    "handler": _drifted_handler_replay,
    "reducer": _drifted_reducer_replay,
    "upcaster": _drifted_upcaster_replay,
}


@pytest.fixture
def seeded() -> StreamStore:
    """Ten events, so "once per registration" is distinguishable from "once"."""
    return seed_stream("s", [{"id": i, "schema_version": 1} for i in range(10)])


class TestMergeReplayAsksTheSameQuestion:
    """`merge_replay` decodes and upcasts at its own call site, so it is its own
    interface into the drift check — and it was the third path nothing covered.

    Round 1 of the review on #212 found the mutation: pointing `merge_replay`'s
    upcast at no ledger left the whole suite green, while the identical mutation
    on `replay()`'s call went red. Pre-existing, and the same shape #187 is
    about, so it is pinned here rather than left for a fourth round of it.
    """

    @pytest.fixture
    def two_streams(self) -> StreamStore:
        store = seed_stream("a", [{"id": i, "schema_version": 1} for i in range(5)])
        return seed_stream(
            "b", [{"id": i + 5, "schema_version": 1} for i in range(5)], store=store
        )

    def _drifted(self):
        handlers = HandlerRegistry()
        handlers.register("h", "s", _handler, 0, None)
        upcasters = UpcasterRegistry()
        version = upcasters.register("s", 1, _upcaster)
        object.__setattr__(version, "source_hash", STORED)
        return handlers, upcasters, version

    def test_a_drifted_upcaster_is_reported_once_across_every_stream(
        self, two_streams: StreamStore
    ) -> None:
        handlers, upcasters, version = self._drifted()

        result = merge_replay(
            two_streams,
            ["a", "b"],
            InMemoryProjections(),
            order_key="id",
            handler_registry=handlers,
            upcaster_registry=upcasters,
            event_match="s",
        )

        assert result.events_processed == 10
        current = hash_function_source(_upcaster)
        assert result.warnings == [
            (
                f"RAKAIA_DRIFT upcaster={version.dotted_path!r} "
                f"stored={STORED[:12]} current={current[:12]}"
            )
        ]

    def test_strict_drift_raises_from_a_merge_too(
        self, two_streams: StreamStore
    ) -> None:
        handlers, upcasters, _version = self._drifted()

        with pytest.raises(HandlerDriftError, match="upcaster"):
            merge_replay(
                two_streams,
                ["a", "b"],
                InMemoryProjections(),
                order_key="id",
                handler_registry=handlers,
                upcaster_registry=upcasters,
                event_match="s",
                on_drift="raise",
            )


class TestEveryKindIsCheckedTheSameWay:
    """One input, three interfaces, one answer.

    Reached through `replay()` rather than through the shared helper on purpose:
    a test of the helper cannot see a call site that forgot to use it, which is
    exactly how the three copies were able to differ.
    """

    @pytest.mark.parametrize("kind", sorted(BUILDERS))
    def test_the_warning_says_the_same_thing_whatever_drifted(
        self, kind: str, seeded: StreamStore
    ) -> None:
        name, fn, run = BUILDERS[kind](seeded)

        result = run()

        assert result.events_processed == 10
        current = hash_function_source(fn)
        assert result.warnings == [
            f"RAKAIA_DRIFT {kind}={name!r} stored={STORED[:12]} current={current[:12]}"
        ]
        assert result.drift_detected == [name]

    @pytest.mark.parametrize("kind", sorted(BUILDERS))
    def test_one_drifted_registration_is_reported_once_over_ten_events(
        self, kind: str, seeded: StreamStore
    ) -> None:
        """Every kind's rule runs per event; none of them may warn per event."""
        _name, _fn, run = BUILDERS[kind](seeded)

        result = run()

        assert len([w for w in result.warnings if "RAKAIA_DRIFT" in w]) == 1

    @pytest.mark.parametrize("kind", sorted(BUILDERS))
    def test_strict_drift_raises_for_every_kind(
        self, kind: str, seeded: StreamStore
    ) -> None:
        """`--strict-drift` must be strict about all three, not two of three."""
        name, _fn, run = BUILDERS[kind](seeded, on_drift="raise")

        with pytest.raises(HandlerDriftError, match=f"{kind}={name!r}"):
            run()

    @pytest.mark.parametrize("kind", sorted(BUILDERS))
    def test_an_undrifted_rule_of_every_kind_is_silent(
        self, kind: str, seeded: StreamStore
    ) -> None:
        """The other half of the same claim: no false positives, per kind.

        Same wiring as the drifted case with the stored hash left correct, under
        `on_drift="raise"` so a spurious answer is a failure, not a warning.
        """
        result = _undrifted(kind, seeded)()

        assert result.warnings == []
        assert result.drift_detected == []


def _undrifted(kind: str, store: StreamStore):
    """The same replay as `BUILDERS[kind]`, with the stored hash left correct."""
    handlers = HandlerRegistry()
    handlers.register("h", "s", _handler, 0, None)
    upcasters = UpcasterRegistry()
    proj = InMemoryProjections()
    kwargs = {}
    if kind == "reducer":
        handlers.register_reducer("r", 1, _reducer)
        kwargs["reader"] = proj
    if kind == "upcaster":
        upcasters.register("s", 1, _upcaster)
    return lambda: replay(
        store,
        "s",
        proj,
        handler_registry=handlers,
        upcaster_registry=upcasters,
        on_drift="raise",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# The mis-pairable options
# ---------------------------------------------------------------------------


class TestTheDriftOptionCannotBeHalfPassed:
    def test_the_upcaster_chain_takes_one_drift_option(self) -> None:
        """The mis-pairing is now unrepresentable, and this is what keeps it so.

        Until #187 this signature had `drift_callback` *and* `hasher`. Either one
        alone was accepted and lost something in silence: `hasher=` without
        `drift_callback=` skipped the check entirely, `drift_callback=` without
        `hasher=` re-read the upcaster's source for every event. There was no
        assertion that could go red for that, because both spellings type-check
        and both return the right event.

        So the assertion is about the shape of the door: one object carries the
        policy, the warn-once memory and the memo, and a second drift-related
        option would be a second thing to keep in agreement. If one is added,
        this fails and asks why.
        """
        for method in (
            UpcasterRegistry.apply_chain,
            UpcasterRegistry.upcast_to_current,
        ):
            keyword_only = [
                p.name
                for p in inspect.signature(method).parameters.values()
                if p.kind is inspect.Parameter.KEYWORD_ONLY
            ]
            assert keyword_only == ["drift"], (
                f"{method.__name__} takes {keyword_only} — drift detection is one "
                "object (#187), not several options that have to agree"
            )

    def test_a_replay_gets_both_halves_without_asking(
        self, seeded: StreamStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The correct pairing is the default, and both halves are observable.

        Ten events through one drifted upcaster: exactly one warning (the
        report-once half) and exactly one hashing of the upcaster's source (the
        memo half). Before #187 a caller could have either without the other.
        """
        from rakaia import drift as drift_module

        hashed: list[object] = []
        original = drift_module.hash_function_source

        def counting(fn: object) -> str:
            hashed.append(fn)
            return original(fn)

        monkeypatch.setattr(drift_module, "hash_function_source", counting)

        _name, _fn, run = _drifted_upcaster_replay(seeded)
        result = run()

        assert len([w for w in result.warnings if "upcaster" in w]) == 1
        assert hashed.count(_upcaster) == 1, (
            f"one upcaster over ten events, hashed {hashed.count(_upcaster)} "
            "times — the memo and the report came apart"
        )

    def test_normalise_on_read_can_opt_in_to_the_same_check(self) -> None:
        """`upcast()` is the read path that had no way to ask.

        It is not drift-checked by default (a one-off read has nowhere to report
        to), and before #187 it could not be: the two options were assembled only
        inside `replay()`. Handing it the same ledger gives the same answer.
        """
        upcasters = UpcasterRegistry()
        version = upcasters.register("s", 1, _upcaster)
        object.__setattr__(version, "source_hash", STORED)

        silent = upcast({"schema_version": 1}, "s", registry=upcasters)
        assert silent["added"] is True

        ledger = DriftLedger()
        upcast({"schema_version": 1}, "s", registry=upcasters, drift=ledger)

        assert ledger.drifted == [version.dotted_path]
        assert len(ledger.warnings) == 1


class TestTheLedgerItself:
    def test_a_registration_is_reported_once_a_function_hashed_once(self) -> None:
        ledger = DriftLedger()

        assert (
            ledger.check(kind="handler", name="h", stored_hash=STORED, fn=_handler)
            is True
        )
        assert (
            ledger.check(kind="handler", name="h", stored_hash=STORED, fn=_handler)
            is True
        )

        assert len(ledger.warnings) == 1
        assert ledger.drifted == ["h"]

    def test_two_registrations_sharing_a_name_are_both_reported(self) -> None:
        """Keyed by registration, not by name — a name is stable across versions."""
        ledger = DriftLedger()

        ledger.check(kind="handler", name="h", stored_hash=STORED, fn=_handler)
        ledger.check(kind="handler", name="h", stored_hash="cafebabe" * 8, fn=_handler)

        assert len(ledger.warnings) == 2
        assert ledger.drifted == ["h"]

    def test_an_unchanged_rule_is_not_drift(self) -> None:
        ledger = DriftLedger(on_drift="raise")

        assert (
            ledger.check(
                kind="handler",
                name="h",
                stored_hash=hash_function_source(_handler),
                fn=_handler,
            )
            is False
        )
        assert ledger.warnings == []
