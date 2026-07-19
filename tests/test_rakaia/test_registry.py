"""Tests for rakaia.registry: HandlerRegistry + register_handler decorator."""

from __future__ import annotations

import pytest

from rakaia.registry import (
    HandlerGapError,
    HandlerOverlapError,
    HandlerRegistry,
    register_handler,
)


@pytest.fixture
def reg() -> HandlerRegistry:
    return HandlerRegistry()


def _fn(tag: str):
    def handler(event):  # noqa: ARG001
        return tag

    handler.__qualname__ = f"_fn.<{tag}>"
    return handler


class TestRegistration:
    def test_register_single(self, reg: HandlerRegistry):
        v = reg.register("mogrify", "room:*", _fn("v1"), 0, 10)
        assert v.name == "mogrify"
        assert v.event_match == "room:*"
        assert v.effective_from == 0
        assert v.effective_to == 10
        assert v.source_hash  # non-empty
        assert v.dotted_path  # non-empty

    def test_register_open_ended(self, reg: HandlerRegistry):
        v = reg.register("mogrify", "room:*", _fn("v1"), 0, None)
        assert v.effective_to is None

    def test_register_multiple_versions(self, reg: HandlerRegistry):
        reg.register("mogrify", "room:*", _fn("v1"), 0, 10)
        reg.register("mogrify", "room:*", _fn("v2"), 10, 20)
        reg.register("mogrify", "room:*", _fn("v3"), 20, None)
        assert len(reg.all_versions()) == 3

    def test_register_different_names_share_range(self, reg: HandlerRegistry):
        # Two distinct handler names can both be active in the same range
        reg.register("a", "room:*", _fn("a"), 0, 10)
        reg.register("b", "room:*", _fn("b"), 0, 10)
        assert len(reg.all_versions()) == 2

    def test_register_different_patterns_share_range(self, reg: HandlerRegistry):
        # Same name, different event_match patterns are independent series
        reg.register("mogrify", "room:*", _fn("v1"), 0, 10)
        reg.register("mogrify", "chat:*", _fn("v1"), 0, 10)
        assert len(reg.all_versions()) == 2

    def test_register_source_hash_captured(self, reg: HandlerRegistry):
        v = reg.register("mogrify", "room:*", _fn("v1"), 0, 10)
        # hex SHA-256 is 64 chars
        assert len(v.source_hash) == 64
        assert all(c in "0123456789abcdef" for c in v.source_hash)


class TestRegistrationValidation:
    def test_negative_from_raises(self, reg: HandlerRegistry):
        with pytest.raises(ValueError, match="non-negative"):
            reg.register("m", "e", _fn("v"), -1)

    def test_to_lte_from_raises(self, reg: HandlerRegistry):
        with pytest.raises(ValueError, match="must be >"):
            reg.register("m", "e", _fn("v"), 10, 10)
        with pytest.raises(ValueError, match="must be >"):
            reg.register("m", "e", _fn("v"), 10, 5)


class TestOverlap:
    def test_exact_duplicate_rejected(self, reg: HandlerRegistry):
        reg.register("m", "e", _fn("v1"), 0, 10)
        with pytest.raises(HandlerOverlapError, match="overlaps"):
            reg.register("m", "e", _fn("v2"), 0, 10)

    def test_partial_overlap_rejected(self, reg: HandlerRegistry):
        reg.register("m", "e", _fn("v1"), 0, 10)
        with pytest.raises(HandlerOverlapError):
            reg.register("m", "e", _fn("v2"), 5, 15)

    def test_adjacent_ok_half_open(self, reg: HandlerRegistry):
        # [0, 10) and [10, 20) are adjacent, not overlapping
        reg.register("m", "e", _fn("v1"), 0, 10)
        reg.register("m", "e", _fn("v2"), 10, 20)

    def test_open_ended_overlap_rejected(self, reg: HandlerRegistry):
        reg.register("m", "e", _fn("v1"), 10, None)
        with pytest.raises(HandlerOverlapError):
            reg.register("m", "e", _fn("v2"), 15, 20)

    def test_two_open_ended_rejected(self, reg: HandlerRegistry):
        reg.register("m", "e", _fn("v1"), 10, None)
        with pytest.raises(HandlerOverlapError):
            reg.register("m", "e", _fn("v2"), 20, None)


class TestResolve:
    def test_picks_correct_version(self, reg: HandlerRegistry):
        v1 = reg.register("m", "room:*", _fn("v1"), 0, 10)
        v2 = reg.register("m", "room:*", _fn("v2"), 10, 20)
        v3 = reg.register("m", "room:*", _fn("v3"), 20, None)

        assert reg.resolve("room:5", 0) == [v1]
        assert reg.resolve("room:5", 9) == [v1]
        assert reg.resolve("room:5", 10) == [v2]
        assert reg.resolve("room:5", 19) == [v2]
        assert reg.resolve("room:5", 20) == [v3]
        assert reg.resolve("room:5", 9_999) == [v3]

    def test_glob_pattern_match(self, reg: HandlerRegistry):
        v = reg.register("m", "room:*:messages", _fn("v1"), 0, None)
        assert reg.resolve("room:5:messages", 0) == [v]
        assert reg.resolve("room:abc:messages", 0) == [v]

    def test_glob_non_match_returns_empty(self, reg: HandlerRegistry):
        reg.register("m", "room:*:messages", _fn("v1"), 0, None)
        assert reg.resolve("chat:5", 0) == []

    def test_multiple_handlers_for_one_event(self, reg: HandlerRegistry):
        v1 = reg.register("a", "room:*", _fn("a"), 0, None)
        v2 = reg.register("b", "room:*", _fn("b"), 0, None)
        result = reg.resolve("room:5", 0)
        assert set(result) == {v1, v2}

    def test_gap_in_coverage_raises(self, reg: HandlerRegistry):
        reg.register("m", "e", _fn("v1"), 0, 10)
        reg.register("m", "e", _fn("v2"), 20, None)
        with pytest.raises(HandlerGapError, match="seq=15"):
            reg.resolve("e", 15)

    def test_unregistered_handler_no_gap(self, reg: HandlerRegistry):
        # No handler registered for this event_match at all -> empty, no raise
        assert reg.resolve("nonexistent", 100) == []

    def test_negative_seq_raises(self, reg: HandlerRegistry):
        with pytest.raises(ValueError, match="non-negative"):
            reg.resolve("e", -1)


class TestMatchField:
    def test_register_with_match_field(self, reg: HandlerRegistry):
        v = reg.register("m", "tf_*", _fn("v1"), 0, None, match_field="form_type")
        assert v.match_field == "form_type"

    def test_resolve_matches_on_payload_field(self, reg: HandlerRegistry):
        v = reg.register("m", "tf_*", _fn("v1"), 0, None, match_field="form_type")
        # The stream path is irrelevant; matching is against event["form_type"].
        assert reg.resolve("submissions", 0, event={"form_type": "tf_611"}) == [v]
        assert reg.resolve("submissions", 0, event={"form_type": "sf_12"}) == []

    def test_resolve_falls_back_to_stream_path(self, reg: HandlerRegistry):
        # No match_field -> current behaviour: match the event_match string,
        # event argument ignored.
        v = reg.register("m", "room:*", _fn("v1"), 0, None)
        assert reg.resolve("room:5", 0) == [v]
        assert reg.resolve("room:5", 0, event={"form_type": "x"}) == [v]

    def test_resolve_match_field_requires_event(self, reg: HandlerRegistry):
        reg.register("m", "tf_*", _fn("v1"), 0, None, match_field="form_type")
        with pytest.raises(ValueError, match="match_field"):
            reg.resolve("submissions", 0)  # no event provided

    def test_resolve_tolerates_empty_series(self, reg: HandlerRegistry):
        # A registration that failed after setdefault() (e.g. a persist error)
        # can leave a (name, pattern) -> [] entry. resolve() must fall back to
        # stream-path routing, not IndexError on versions[0].
        reg._versions[("ghost", "room:*")] = []  # type: ignore[attr-defined]
        assert reg.resolve("chat:5", 0) == []  # non-matching pattern: skipped
        with pytest.raises(HandlerGapError):  # matching, no covering version
            reg.resolve("room:5", 0)


class TestStage:
    def test_register_with_stage(self, reg: HandlerRegistry):
        v = reg.register("m", "e", _fn("v1"), 0, None, stage=1)
        assert v.stage == 1

    def test_default_stage_is_zero(self, reg: HandlerRegistry):
        v = reg.register("m", "e", _fn("v1"), 0, None)
        assert v.stage == 0

    def test_negative_stage_raises(self, reg: HandlerRegistry):
        with pytest.raises(ValueError, match="stage must be non-negative"):
            reg.register("m", "e", _fn("v1"), 0, None, stage=-1)

    def test_stages_returns_sorted_distinct(self, reg: HandlerRegistry):
        reg.register("a", "e", _fn("a"), 0, None, stage=2)
        reg.register("b", "e", _fn("b"), 0, None, stage=0)
        reg.register("c", "e", _fn("c"), 0, None, stage=2)
        assert reg.stages() == [0, 2]

    def test_stages_empty_registry(self, reg: HandlerRegistry):
        assert reg.stages() == []

    def test_resolve_filters_by_stage(self, reg: HandlerRegistry):
        v0 = reg.register("ref", "room:*", _fn("ref"), 0, None, stage=0)
        v1 = reg.register("dep", "room:*", _fn("dep"), 0, None, stage=1)
        assert reg.resolve("room:5", 0, stage=0) == [v0]
        assert reg.resolve("room:5", 0, stage=1) == [v1]
        assert reg.resolve("room:5", 0, stage=2) == []

    def test_resolve_without_stage_returns_all_stages(self, reg: HandlerRegistry):
        v0 = reg.register("ref", "room:*", _fn("ref"), 0, None, stage=0)
        v1 = reg.register("dep", "room:*", _fn("dep"), 0, None, stage=1)
        assert set(reg.resolve("room:5", 0)) == {v0, v1}

    def test_resolve_stage_filter_still_detects_gap(self, reg: HandlerRegistry):
        # A coverage gap in ANY matching series is an error, even while
        # resolving a different stage — a gap is a misconfiguration.
        reg.register("m", "e", _fn("v1"), 0, 10, stage=0)
        reg.register("m", "e", _fn("v2"), 20, None, stage=0)
        with pytest.raises(HandlerGapError):
            reg.resolve("e", 15, stage=1)


class TestReducers:
    def test_register_reducer(self, reg: HandlerRegistry):
        r = reg.register_reducer("balance", 1, _fn("b"))
        assert r.name == "balance"
        assert r.stage == 1
        assert r.source_hash
        assert r.dotted_path

    def test_negative_stage_raises(self, reg: HandlerRegistry):
        with pytest.raises(ValueError, match="stage must be non-negative"):
            reg.register_reducer("b", -1, _fn("b"))

    def test_reducers_for_stage_sorted_by_name(self, reg: HandlerRegistry):
        reg.register_reducer("zeta", 1, _fn("z"))
        reg.register_reducer("alpha", 1, _fn("a"))
        reg.register_reducer("other", 2, _fn("o"))
        assert [r.name for r in reg.reducers_for_stage(1)] == ["alpha", "zeta"]
        assert [r.name for r in reg.reducers_for_stage(2)] == ["other"]
        assert reg.reducers_for_stage(3) == []

    def test_stages_includes_reducer_stages(self, reg: HandlerRegistry):
        reg.register("h", "e", _fn("h"), 0, None, stage=0)
        reg.register_reducer("agg", 2, _fn("a"))
        assert reg.stages() == [0, 2]

    def test_has_reducers(self, reg: HandlerRegistry):
        assert reg.has_reducers() is False
        reg.register_reducer("agg", 1, _fn("a"))
        assert reg.has_reducers() is True

    def test_identical_reducer_is_noop(self, reg: HandlerRegistry):
        fn = _fn("b")
        r1 = reg.register_reducer("balance", 1, fn)
        r2 = reg.register_reducer("balance", 1, fn)
        assert r1 is r2
        assert len(reg.all_reducers()) == 1

    def test_reregister_replaces_definition(self, reg: HandlerRegistry):
        reg.register_reducer("balance", 1, _fn("v1"))

        def v2(reader):  # noqa: ARG001
            return "v2"

        v2.__qualname__ = "reducer.<v2>"
        reg.register_reducer("balance", 2, v2)  # same name, new fn + stage
        assert len(reg.all_reducers()) == 1
        assert reg.all_reducers()[0].stage == 2


class TestDecorator:
    def test_decorator_registers_with_explicit_registry(self):
        reg = HandlerRegistry()

        @register_handler("m", "room:*", 0, 10, registry=reg)
        def handler(event):
            return event

        versions = reg.all_versions()
        assert len(versions) == 1
        assert versions[0].name == "m"
        assert versions[0].fn is handler

    def test_decorator_returns_original_function(self):
        reg = HandlerRegistry()

        @register_handler("m", "e", 0, registry=reg)
        def handler(event):
            return event

        assert callable(handler)
        assert handler({"x": 1}) == {"x": 1}

    def test_decorator_default_registry_singleton(self):
        from rakaia.registry import get_default_registry

        assert get_default_registry() is get_default_registry()
