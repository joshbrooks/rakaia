"""Reason codes for the failures rakaia raises itself.

ADR 0007 Decision 6 leaves a consumer's reason codes opaque to rakaia. It never
said what happens when *rakaia* is what failed, and the loop was answering with
``type(exc).__name__`` — so a rename of an internal class rewrote a code an
operator had been reading, and every unanticipated bug arrived as ``ValueError``,
indistinguishable from a decode failure.

The codes are a promised, closed set. The test that carries the whole point is
`TestACodeSurvivesARename`: a code derived from a class name is the defect being
fixed, so a rename must not move it.
"""

from __future__ import annotations

import pytest

from rakaia.drift import HandlerDriftError
from rakaia.effects import (
    DuplicateProducesError,
    EffectCollisionError,
    UnresolvedRefError,
)
from rakaia.errors import REASON_CODES, UNHANDLED, RakaiaError
from rakaia.executors import CollectingExecutor
from rakaia.outcomes import InMemoryOutcomeStore, Outcome
from rakaia.registry import HandlerGapError, UpcasterChainError
from rakaia.replay import MergeKeyError, MissingReaderError, UndecodableEventError
from rakaia.store import StreamStore
from rakaia.subscription import consume
from rakaia.types import StreamMessage

#: Every class rakaia raises from the apply path, against the code it promises.
#: Nine sites, and the pairing is the promise — not a derivation from a name.
THE_NINE: list[tuple[type[RakaiaError], str]] = [
    (HandlerGapError, "handler_gap"),
    (UpcasterChainError, "upcaster_chain"),
    (EffectCollisionError, "effect_collision"),
    (UnresolvedRefError, "unresolved_ref"),
    (DuplicateProducesError, "duplicate_produces"),
    (HandlerDriftError, "handler_drift"),
    (MissingReaderError, "missing_reader"),
    (UndecodableEventError, "undecodable_event"),
    (MergeKeyError, "merge_key"),
]


def _reasons_and_params(exc: BaseException) -> tuple[tuple[str, ...], dict[str, str]]:
    """Run one failing apply through the loop and return what it recorded."""
    store = StreamStore()
    store.create("s")
    store.append("s", b'{"a": 1}')
    outcomes = InMemoryOutcomeStore()

    def apply(_message: StreamMessage) -> None:
        raise exc

    result = consume(
        store,
        "s",
        apply,
        consumer="c",
        on_error="skip",
        outcomes=outcomes,
    )
    assert len(result.outcomes) == 1
    assert outcomes.latest("c", "s") == list(result.outcomes)
    return result.outcomes[0].reasons, dict(result.outcomes[0].params)


class TestEachSiteRecordsItsOwnCode:
    @pytest.mark.parametrize(
        ("cls", "code"), THE_NINE, ids=[c.__name__ for c, _ in THE_NINE]
    )
    def test_the_loop_records_the_class_s_promised_code(
        self, cls: type[RakaiaError], code: str
    ) -> None:
        reasons, params = _reasons_and_params(cls("boom"))
        assert reasons == (code,)
        assert params == {}

    def test_the_nine_codes_are_distinct(self) -> None:
        codes = [code for _, code in THE_NINE]
        assert len(set(codes)) == len(codes)

    def test_every_promised_code_is_in_the_closed_set(self) -> None:
        assert {code for _, code in THE_NINE} | {UNHANDLED} == set(REASON_CODES)


class TestAnythingElseIsUnhandled:
    def test_a_consumer_s_own_exception_records_unhandled(self) -> None:
        class ConsumerBlewUp(RuntimeError):
            pass

        reasons, params = _reasons_and_params(ConsumerBlewUp("nope"))
        assert reasons == (UNHANDLED,)
        assert params == {"exception_type": "ConsumerBlewUp"}

    def test_a_bare_value_error_is_no_longer_confusable_with_a_decode_failure(
        self,
    ) -> None:
        """The defect in the issue: everything unanticipated looked like a decode."""
        bare, _ = _reasons_and_params(ValueError("something else entirely"))
        decode, _ = _reasons_and_params(UndecodableEventError("bad json"))
        assert bare != decode

    def test_the_type_name_is_the_only_thing_carried(self) -> None:
        """No interpolated message — Decision 6 forbids it, and this is the path
        where a field value would most easily leak."""
        _, params = _reasons_and_params(ValueError("row 7 for taxpayer 12345"))
        assert params == {"exception_type": "ValueError"}


class TestACodeSurvivesARename:
    """The whole point. A code derived from `__name__` moves when a class is
    renamed, which is what silently rewrote the vocabulary operators read."""

    @pytest.mark.parametrize(
        ("cls", "code"), THE_NINE, ids=[c.__name__ for c, _ in THE_NINE]
    )
    def test_renaming_the_class_does_not_move_the_code(
        self, cls: type[RakaiaError], code: str
    ) -> None:
        renamed = type("ThoroughlyDifferentName", (cls,), {})
        assert renamed.__name__ != cls.__name__
        assert renamed.code == code
        reasons, _ = _reasons_and_params(renamed("boom"))
        assert reasons == (code,)

    def test_no_code_is_spelled_the_way_a_name_derivation_would_spell_it(self) -> None:
        """A derivation that happened to agree would pass the rename test by
        luck for one class; this fails if any code is literally its class name."""
        for cls, code in THE_NINE:
            assert code != cls.__name__


class TestOneClauseCatchesThemAll:
    @pytest.mark.parametrize(
        "cls", [c for c, _ in THE_NINE], ids=[c.__name__ for c, _ in THE_NINE]
    )
    def test_the_base_type_catches_every_one_of_the_nine(
        self, cls: type[RakaiaError]
    ) -> None:
        with pytest.raises(RakaiaError):
            raise cls("boom")

    def test_the_base_type_is_exported_from_the_package_root(self) -> None:
        import rakaia

        assert rakaia.RakaiaError is RakaiaError

    @pytest.mark.parametrize(
        "cls", [MissingReaderError, UndecodableEventError, MergeKeyError]
    )
    def test_the_replay_errors_are_still_value_errors(
        self, cls: type[RakaiaError]
    ) -> None:
        """Additive, not breaking: a caller already catching `ValueError` from a
        merge or a decode keeps catching it."""
        assert issubclass(cls, ValueError)


class TestTheSetIsClosed:
    def test_every_rakaia_error_in_the_tree_declares_a_promised_code(self) -> None:
        """A new exception that forgets its code would otherwise record
        `unhandled` — quietly, and only in production."""
        seen: set[type[RakaiaError]] = set()

        def walk(cls: type[RakaiaError]) -> None:
            for sub in cls.__subclasses__():
                if sub.__module__.startswith("rakaia."):
                    seen.add(sub)
                    walk(sub)

        walk(RakaiaError)
        assert seen == {cls for cls, _ in THE_NINE}
        for cls in seen:
            assert cls.code in REASON_CODES
            assert cls.code != UNHANDLED

    def test_an_outcome_will_accept_every_promised_code(self) -> None:
        """The codes have to survive `encode_outcome`'s string check."""
        for code in REASON_CODES:
            Outcome(
                consumer="c",
                stream_path="s",
                subject="1",
                offset="1",
                sequence_key="1",
                stage="project",
                status="failed",
                reasons=(code,),
            )


class TestTheRealRaisingSitesRaiseTheNewTypes:
    def test_a_staged_replay_with_no_reader_raises_missing_reader(self) -> None:
        from rakaia.registry import HandlerRegistry, UpcasterRegistry
        from rakaia.replay import build_pipeline, require_reader

        registry = HandlerRegistry()

        def handler(_event: dict, _reader: object) -> None:
            return None

        registry.register("h", "x.*", handler, 0, stage=1)
        ctx = build_pipeline(
            handler_registry=registry,
            upcaster_registry=UpcasterRegistry(),
            executor=CollectingExecutor(),
            reader=None,
            on_drift="warn",
        )
        with pytest.raises(MissingReaderError):
            require_reader(ctx)

    def test_an_undecodable_event_raises_undecodable_event(self) -> None:
        from rakaia.replay import _decode_event

        with pytest.raises(UndecodableEventError):
            _decode_event(b"not json at all", "s", 0)

    def test_a_missing_merge_key_raises_merge_key(self) -> None:
        from rakaia.registry import HandlerRegistry, UpcasterRegistry
        from rakaia.replay import merge_replay

        store = StreamStore()
        store.create("a")
        store.append("a", b'{"type": "x.done"}')
        with pytest.raises(MergeKeyError):
            merge_replay(
                store,
                ["a"],
                CollectingExecutor(),
                order_key="when",
                handler_registry=HandlerRegistry(),
                upcaster_registry=UpcasterRegistry(),
            )
