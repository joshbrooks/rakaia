"""One content-routing rule, driven through both registries at once.

Handlers and upcasters answer the same question — *what string does this
registration's glob get tested against?* — and for a long time each answered it
in its own method: `HandlerRegistry._match_subject` and
`UpcasterRegistry._subject`, the second carrying a comment saying it mirrored
the first. A comment is not a test. These cases put the same inputs through both
sides so the rule is pinned as one rule:

* no `match_field` → the stream/event-match string, and the event is irrelevant;
* `match_field` set → `str(event[match_field])`;
* `match_field` set, field **absent** → `""`, which simply does not match
  (a different form_type on the same stream is normal, not an error);
* `match_field` set, event **missing entirely** → `ValueError`, because a caller
  who forgot the event would otherwise silently leave content-routed
  registrations unmatched.

Written before the two implementations were merged into one, so a silent change
in matching behaviour would have failed here.
"""

from __future__ import annotations

import pytest

from rakaia.registry import HandlerRegistry, UpcasterChainError, UpcasterRegistry


def _handler(event):  # noqa: ARG001
    return []


def _upcaster(event):
    return {**event, "upcasted": True}


@pytest.fixture
def handlers() -> HandlerRegistry:
    return HandlerRegistry()


@pytest.fixture
def upcasters() -> UpcasterRegistry:
    return UpcasterRegistry()


def _handler_matches(reg: HandlerRegistry, subject: str, event) -> bool:
    """Whether the registered handler matched — the routing question, isolated
    from sequence coverage (every handler here is registered open-ended)."""
    return bool(reg.resolve(subject, 0, event))


def _upcaster_matches(reg: UpcasterRegistry, subject: str, event) -> bool:
    """Whether the registered upcaster matched. `current_version` returns 2 when
    a v1 upcaster applies and 1 when none does."""
    return reg.current_version(subject, event) == 2


class TestStreamPathRoutingIgnoresTheEvent:
    """The default: the pattern is tested against the stream/event-match string.
    The event, present or not, has no say."""

    @pytest.mark.parametrize(
        "event",
        [None, {}, {"form_type": "SOMETHING_ELSE"}],
        ids=["none", "empty", "other"],
    )
    def test_a_handler_matches_on_the_stream_path(self, handlers, event):
        handlers.register("h", "room:*", _handler, 0, None)
        assert _handler_matches(handlers, "room:5", event) is True
        assert _handler_matches(handlers, "chat:5", event) is False

    @pytest.mark.parametrize(
        "event",
        [None, {}, {"form_type": "SOMETHING_ELSE"}],
        ids=["none", "empty", "other"],
    )
    def test_an_upcaster_matches_on_the_stream_path(self, upcasters, event):
        upcasters.register("room:*", 1, _upcaster)
        assert _upcaster_matches(upcasters, "room:5", event) is True
        assert _upcaster_matches(upcasters, "chat:5", event) is False


class TestContentRoutingReadsTheField:
    """`match_field` set → the pattern is tested against `event[match_field]`,
    and the stream path stops mattering."""

    def test_a_handler_matches_the_field_not_the_path(self, handlers):
        handlers.register("h", "TF_*", _handler, 0, None, match_field="form_type")
        event = {"form_type": "TF_6_1_1"}
        assert _handler_matches(handlers, "submissions:abc", event) is True
        assert _handler_matches(handlers, "anything-at-all", event) is True

    def test_an_upcaster_matches_the_field_not_the_path(self, upcasters):
        upcasters.register("TF_*", 1, _upcaster, match_field="form_type")
        event = {"form_type": "TF_6_1_1"}
        assert _upcaster_matches(upcasters, "submissions:abc", event) is True
        assert _upcaster_matches(upcasters, "anything-at-all", event) is True

    def test_a_handler_does_not_match_a_different_field_value(self, handlers):
        handlers.register("h", "TF_*", _handler, 0, None, match_field="form_type")
        other = {"form_type": "SF_1"}
        assert _handler_matches(handlers, "submissions:abc", other) is False

    def test_an_upcaster_does_not_match_a_different_field_value(self, upcasters):
        upcasters.register("TF_*", 1, _upcaster, match_field="form_type")
        other = {"form_type": "SF_1"}
        assert _upcaster_matches(upcasters, "submissions:abc", other) is False

    def test_a_handler_stringifies_a_non_string_field(self, handlers):
        """`str()` on the value, so an integer discriminator is globbable."""
        handlers.register("h", "42", _handler, 0, None, match_field="version_code")
        assert _handler_matches(handlers, "s", {"version_code": 42}) is True

    def test_an_upcaster_stringifies_a_non_string_field(self, upcasters):
        upcasters.register("42", 1, _upcaster, match_field="version_code")
        assert _upcaster_matches(upcasters, "s", {"version_code": 42}) is True


class TestAMissingKeyIsTheEmptyString:
    """Load-bearing: `event.get(field, "")`. An event on the stream that simply
    carries a different shape is normal traffic, not a configuration error — so
    the subject is `""`, which matches nothing but `""`-shaped globs, and no
    exception is raised."""

    def test_a_handler_with_the_key_absent_does_not_match(self, handlers):
        handlers.register("h", "TF_*", _handler, 0, None, match_field="form_type")
        assert _handler_matches(handlers, "submissions:abc", {"other": 1}) is False

    def test_an_upcaster_with_the_key_absent_does_not_match(self, upcasters):
        upcasters.register("TF_*", 1, _upcaster, match_field="form_type")
        assert _upcaster_matches(upcasters, "submissions:abc", {"other": 1}) is False

    def test_a_handler_glob_that_accepts_the_empty_string_still_matches(self, handlers):
        """`*` matches `""`, so an absent key is genuinely routed as `""` rather
        than being skipped — the distinction a `""`-accepting glob exposes."""
        handlers.register("h", "*", _handler, 0, None, match_field="form_type")
        assert _handler_matches(handlers, "submissions:abc", {"other": 1}) is True

    def test_an_upcaster_glob_that_accepts_the_empty_string_still_matches(
        self, upcasters
    ):
        upcasters.register("*", 1, _upcaster, match_field="form_type")
        assert _upcaster_matches(upcasters, "submissions:abc", {"other": 1}) is True

    def test_a_handler_with_an_empty_string_value_matches_like_an_absent_key(
        self, handlers
    ):
        handlers.register("h", "", _handler, 0, None, match_field="form_type")
        assert _handler_matches(handlers, "s", {"form_type": ""}) is True
        assert _handler_matches(handlers, "s", {}) is True

    def test_an_upcaster_with_an_empty_string_value_matches_like_an_absent_key(
        self, upcasters
    ):
        upcasters.register("", 1, _upcaster, match_field="form_type")
        assert _upcaster_matches(upcasters, "s", {"form_type": ""}) is True
        assert _upcaster_matches(upcasters, "s", {}) is True


class TestNoEventAtAllRaises:
    """A content-routed registration matched without an event is a caller bug:
    silently skipping it would leave the event un-handled or un-upcast with no
    signal at all."""

    def test_a_handler_raises(self, handlers):
        handlers.register("h", "TF_*", _handler, 0, None, match_field="form_type")
        with pytest.raises(ValueError, match="match_field"):
            handlers.resolve("submissions:abc", 0, None)

    def test_an_upcaster_raises(self, upcasters):
        upcasters.register("TF_*", 1, _upcaster, match_field="form_type")
        with pytest.raises(ValueError, match="match_field"):
            upcasters.current_version("submissions:abc", None)

    def test_apply_chain_always_has_an_event_so_it_reports_a_missing_link(
        self, upcasters
    ):
        """`apply_chain` takes the event by value, so it can never hit the
        no-event raise; an event that lacks the discriminator routes as `""` and
        surfaces as a missing chain link instead."""
        upcasters.register("TF_*", 1, _upcaster, match_field="form_type")
        with pytest.raises(UpcasterChainError, match="Missing upcaster"):
            upcasters.apply_chain({"schema_version": 1}, "submissions:abc", 2)

    def test_the_handler_error_names_the_handler_and_the_field(self, handlers):
        handlers.register("h", "TF_*", _handler, 0, None, match_field="form_type")
        with pytest.raises(ValueError) as excinfo:
            handlers.resolve("submissions:abc", 0, None)
        assert "'h'" in str(excinfo.value)
        assert "form_type" in str(excinfo.value)

    def test_the_upcaster_error_names_the_pattern_and_the_field(self, upcasters):
        upcasters.register("TF_*", 1, _upcaster, match_field="form_type")
        with pytest.raises(ValueError) as excinfo:
            upcasters.current_version("submissions:abc", None)
        assert "TF_*" in str(excinfo.value)
        assert "form_type" in str(excinfo.value)

    def test_a_stream_routed_registration_never_raises_without_an_event(
        self, handlers, upcasters
    ):
        """The raise is specific to content routing — the default path must stay
        callable with no event, which is how most of the library calls it."""
        handlers.register("h", "room:*", _handler, 0, None)
        upcasters.register("room:*", 1, _upcaster)
        assert handlers.resolve("room:5", 0) != []
        assert upcasters.current_version("room:5") == 2


class TestBothRegistriesAgreeOnTheSameInputs:
    """The consolidation's actual claim: given the same pattern, field, subject
    and event, a handler and an upcaster match or don't match together."""

    CASES = [
        ("TF_*", "form_type", "submissions:1", {"form_type": "TF_6"}, True),
        ("TF_*", "form_type", "submissions:1", {"form_type": "SF_1"}, False),
        ("TF_*", "form_type", "TF_6", {"form_type": "SF_1"}, False),
        ("*", "form_type", "submissions:1", {}, True),
        ("TF_*", "form_type", "submissions:1", {}, False),
        ("", "form_type", "submissions:1", {"form_type": ""}, True),
        ("42", "code", "submissions:1", {"code": 42}, True),
    ]

    @pytest.mark.parametrize(
        ("pattern", "field", "subject", "event", "expected"),
        CASES,
        ids=[f"{c[0]}|{c[3]}" for c in CASES],
    )
    def test_the_two_registries_route_identically(
        self, handlers, upcasters, pattern, field, subject, event, expected
    ):
        handlers.register("h", pattern, _handler, 0, None, match_field=field)
        upcasters.register(pattern, 1, _upcaster, match_field=field)
        assert _handler_matches(handlers, subject, event) is expected
        assert _upcaster_matches(upcasters, subject, event) is expected
