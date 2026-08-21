"""What a read answers with, asserted as a function call.

`_handle_read` was ~190 lines with eight `send_response` exits, so every rule
about a read's status, headers, ETag and body could only be observed by standing
up an ASGI app and reading the bytes back — the same three-line
`httpx.ASGITransport` fixture in three different test files. The rules now live
in `rakaia.read_decision`, the read-side twin of `rakaia.append_decision`, and
this file is the exhaustive cover for them: one class per exit, plus the
agreement between the plain read and the live push that used to be two separate
answers to the same question.

`tests/test_rakaia/test_handler.py` still drives the whole thing over HTTP. That
coverage is wiring — it proves the handler asks these functions and sends what
they say — and is deliberately not duplicated here.
"""

from __future__ import annotations

import base64
import itertools

import pytest

from rakaia.read_decision import (
    STREAM_CLOSED_HEADER_RESP,
    STREAM_CURSOR_HEADER_RESP,
    STREAM_EXPIRES_AT_HEADER_RESP,
    STREAM_OFFSET_HEADER_RESP,
    STREAM_TTL_HEADER_RESP,
    STREAM_UP_TO_DATE_HEADER_RESP,
    ReadFacts,
    ReadRequest,
    closed_at_tail,
    decide_catch_up,
    decide_head,
    decide_read,
    decide_wait_ended,
    read_etag,
    read_param_error,
    sse_control_fields,
    stream_ended,
)
from rakaia.types import (
    SSE_CLOSED_FIELD,
    SSE_CURSOR_FIELD,
    SSE_OFFSET_FIELD,
    SSE_UP_TO_DATE_FIELD,
)

JSON = "application/json"


def facts(
    *,
    tail: str = "0000000005",
    closed: bool = False,
    content_type: str | None = JSON,
    ttl_seconds: int | None = None,
    expires_at: str | None = None,
) -> ReadFacts:
    return ReadFacts(
        path="/s",
        tail_offset=tail,
        closed=closed,
        content_type=content_type,
        ttl_seconds=ttl_seconds,
        expires_at=expires_at,
    )


# =============================================================================
# The two predicates, and why there are two
# =============================================================================


class TestClosedAtTail:
    """The one rule for "tell the client the stream is closed"."""

    @pytest.mark.parametrize(
        ("closed", "at_tail", "up_to_date"),
        [c for c in itertools.product([True, False], repeat=3) if not all(c)],
    )
    def test_every_combination_short_of_all_three_is_not_closed(
        self, closed: bool, at_tail: bool, up_to_date: bool
    ):
        assert not closed_at_tail(closed=closed, at_tail=at_tail, up_to_date=up_to_date)

    def test_all_three_together_is_closed(self):
        assert closed_at_tail(closed=True, at_tail=True, up_to_date=True)

    def test_a_closed_stream_read_from_behind_is_not_reported_closed(self):
        """The client still has messages to fetch; `Stream-Closed` would stop it."""
        assert not closed_at_tail(closed=True, at_tail=False, up_to_date=True)


class TestStreamEnded:
    """Termination is a liveness rule, deliberately not the header rule."""

    def test_a_closed_stream_at_the_tail_ends_whatever_up_to_date_says(self):
        assert stream_ended(closed=True, at_tail=True)

    def test_it_differs_from_the_header_rule_exactly_when_not_up_to_date(self):
        # This is why the two are separate functions: collapsing them would let a
        # store reporting `up_to_date=False` wait forever on a closed stream.
        assert stream_ended(closed=True, at_tail=True)
        assert not closed_at_tail(closed=True, at_tail=True, up_to_date=False)

    def test_an_open_stream_at_the_tail_does_not_end(self):
        assert not stream_ended(closed=True, at_tail=False)
        assert not stream_ended(closed=False, at_tail=True)


# =============================================================================
# The ETag
# =============================================================================


class TestTheEtag:
    def test_it_is_path_b64_then_from_then_to(self):
        expected_b64 = base64.b64encode(b"/s").decode()
        assert (
            read_etag("/s", start_offset="-1", response_offset="7")
            == f'"{expected_b64}:-1:7"'
        )

    def test_closed_at_tail_adds_the_c_suffix(self):
        assert read_etag(
            "/s", start_offset="3", response_offset="7", closed=True
        ).endswith(':7:c"')

    def test_the_suffix_tracks_the_header_not_the_raw_closed_flag(self):
        """A closed stream read from behind caches under a key with no `:c`.

        Otherwise a client would cache a partial body under a key that claims
        the stream is finished.
        """
        verdict = decide_read(
            facts(closed=True),
            ReadRequest(),
            latest=facts(closed=True),
            last_message_offset="0000000003",
            up_to_date=True,
        )
        assert STREAM_CLOSED_HEADER_RESP not in verdict.headers
        assert not verdict.headers["etag"].endswith(':c"')

    def test_head_and_a_full_get_at_the_tail_agree(self):
        """#158 suspected these had drifted. They had not, and now they cannot.

        A HEAD passes `-1` and its own tail because it has no client position; a
        GET with no `offset` reads from the start and ends at the tail, so the
        two are the same key by construction.
        """
        f = facts()
        head = decide_head(f)
        get = decide_read(
            f,
            ReadRequest(),
            latest=f,
            last_message_offset=f.tail_offset,
            up_to_date=True,
        )
        assert head.headers["etag"] == get.headers["etag"]


# =============================================================================
# Exits 1-4: a malformed request
# =============================================================================


class TestTheParameterRefusals:
    def test_a_well_formed_request_is_not_refused(self):
        assert read_param_error(offset="0000000001", live=None) is None

    def test_no_offset_at_all_is_fine_for_a_plain_read(self):
        assert read_param_error(offset=None, live=None) is None

    def test_an_empty_offset_is_400(self):
        assert read_param_error(offset="", live=None) == (
            400,
            b"Empty offset parameter",
        )

    def test_two_offset_parameters_are_400(self):
        assert read_param_error(offset="1", live=None, offset_count=2) == (
            400,
            b"Multiple offset parameters not allowed",
        )

    def test_an_unparseable_offset_is_400(self):
        assert read_param_error(offset="not an offset!", live=None) == (
            400,
            b"Invalid offset format",
        )

    @pytest.mark.parametrize(
        ("live", "label"),
        [("long-poll", b"Long-poll"), ("sse", b"SSE")],
    )
    def test_a_live_read_without_an_offset_is_400_naming_the_mode(
        self, live: str, label: bytes
    ):
        assert read_param_error(offset=None, live=live) == (
            400,
            label + b" requires offset parameter",
        )

    def test_the_offset_refusals_come_first(self):
        """Order is observable: a request can be wrong twice and only one is sent."""
        assert read_param_error(offset="", live="sse") == (
            400,
            b"Empty offset parameter",
        )

    def test_the_count_check_comes_before_the_format_check(self):
        assert read_param_error(offset="bad!", live=None, offset_count=2) == (
            400,
            b"Multiple offset parameters not allowed",
        )


# =============================================================================
# Exit 5: HEAD
# =============================================================================


class TestHead:
    def test_it_reports_the_tail_and_refuses_caching(self):
        verdict = decide_head(facts())
        assert verdict.status == 200
        assert verdict.headers[STREAM_OFFSET_HEADER_RESP] == "0000000005"
        assert verdict.headers["cache-control"] == "no-store"
        assert verdict.body == b""

    def test_it_carries_the_content_type_when_the_stream_has_one(self):
        assert decide_head(facts()).headers["content-type"] == JSON
        assert "content-type" not in decide_head(facts(content_type=None)).headers

    def test_a_closed_stream_is_flagged(self):
        assert decide_head(facts(closed=True)).headers[STREAM_CLOSED_HEADER_RESP] == (
            "true"
        )
        assert STREAM_CLOSED_HEADER_RESP not in decide_head(facts()).headers

    def test_expiry_metadata_is_reported_when_configured(self):
        verdict = decide_head(facts(ttl_seconds=60, expires_at="2030-01-01T00:00:00Z"))
        assert verdict.headers[STREAM_TTL_HEADER_RESP] == "60"
        assert verdict.headers[STREAM_EXPIRES_AT_HEADER_RESP] == "2030-01-01T00:00:00Z"

    def test_a_zero_ttl_is_still_reported(self):
        """`0` is a configured TTL, not an absent one."""
        assert decide_head(facts(ttl_seconds=0)).headers[STREAM_TTL_HEADER_RESP] == "0"

    def test_cors_headers_are_merged_in(self):
        verdict = decide_head(facts(), cors={"access-control-allow-origin": "*"})
        assert verdict.headers["access-control-allow-origin"] == "*"


# =============================================================================
# Exit 6: ?offset=now on a plain read
# =============================================================================


class TestCatchUp:
    def test_it_is_an_empty_up_to_date_200_at_the_tail(self):
        verdict = decide_catch_up(facts())
        assert verdict.status == 200
        assert verdict.headers[STREAM_OFFSET_HEADER_RESP] == "0000000005"
        assert verdict.headers[STREAM_UP_TO_DATE_HEADER_RESP] == "true"
        assert verdict.headers["cache-control"] == "no-store"

    def test_a_json_stream_gets_an_empty_array_not_an_empty_body(self):
        assert decide_catch_up(facts()).body == b"[]"

    def test_a_non_json_stream_gets_no_bytes(self):
        assert decide_catch_up(facts(content_type="text/plain")).body == b""

    def test_a_typeless_stream_gets_no_bytes(self):
        assert decide_catch_up(facts(content_type=None)).body == b""

    def test_a_closed_stream_is_flagged(self):
        headers = decide_catch_up(facts(closed=True)).headers
        assert headers[STREAM_CLOSED_HEADER_RESP] == "true"

    def test_there_is_nothing_to_cache_against(self):
        assert "etag" not in decide_catch_up(facts()).headers


# =============================================================================
# Exit 7: a long poll that ended with nothing to deliver
# =============================================================================


class TestWaitEnded:
    def test_it_is_a_bodiless_204_at_the_clients_position(self):
        verdict = decide_wait_ended(
            response_offset="0000000002", closed=False, response_cursor="41"
        )
        assert verdict.status == 204
        assert verdict.body == b""
        assert verdict.headers[STREAM_OFFSET_HEADER_RESP] == "0000000002"
        assert verdict.headers[STREAM_UP_TO_DATE_HEADER_RESP] == "true"

    def test_the_cursor_is_sent_when_the_caller_has_one(self):
        verdict = decide_wait_ended(
            response_offset="0", closed=False, response_cursor="41"
        )
        assert verdict.headers[STREAM_CURSOR_HEADER_RESP] == "41"

    def test_no_cursor_means_the_header_is_omitted_not_empty(self):
        """The exit taken *before* waiting has never sent one."""
        verdict = decide_wait_ended(
            response_offset="0", closed=False, response_cursor=None
        )
        assert STREAM_CURSOR_HEADER_RESP not in verdict.headers

    def test_closure_is_flagged_when_the_caller_knows_it(self):
        assert (
            decide_wait_ended(
                response_offset="0", closed=True, response_cursor=None
            ).headers[STREAM_CLOSED_HEADER_RESP]
            == "true"
        )

    def test_an_open_stream_is_not_flagged(self):
        verdict = decide_wait_ended(
            response_offset="0", closed=False, response_cursor="41"
        )
        assert STREAM_CLOSED_HEADER_RESP not in verdict.headers

    def test_it_carries_no_body_type_because_it_has_no_body(self):
        verdict = decide_wait_ended(
            response_offset="0", closed=False, response_cursor="41"
        )
        assert "content-type" not in verdict.headers


# =============================================================================
# Exit 8: the read that has something to say
# =============================================================================


class TestRead:
    def test_the_reported_offset_is_the_last_message(self):
        verdict = decide_read(
            facts(),
            ReadRequest(),
            latest=facts(),
            last_message_offset="0000000003",
            up_to_date=True,
        )
        assert verdict.headers[STREAM_OFFSET_HEADER_RESP] == "0000000003"

    def test_with_no_messages_it_falls_back_to_the_tail(self):
        verdict = decide_read(
            facts(),
            ReadRequest(),
            latest=facts(),
            last_message_offset=None,
            up_to_date=True,
        )
        assert verdict.headers[STREAM_OFFSET_HEADER_RESP] == "0000000005"

    def test_a_body_of_none_means_the_caller_formats_the_messages(self):
        verdict = decide_read(
            facts(),
            ReadRequest(),
            latest=facts(),
            last_message_offset="0000000005",
            up_to_date=True,
        )
        assert verdict.status == 200
        assert verdict.body is None

    def test_the_content_type_comes_from_the_stream(self):
        verdict = decide_read(
            facts(content_type="text/plain"),
            ReadRequest(),
            latest=facts(),
            last_message_offset=None,
            up_to_date=True,
        )
        assert verdict.headers["content-type"] == "text/plain"

    def test_up_to_date_is_reported_only_when_the_store_says_so(self):
        common = {
            "latest": facts(),
            "last_message_offset": "0000000005",
        }
        assert (
            decide_read(facts(), ReadRequest(), up_to_date=True, **common).headers[
                STREAM_UP_TO_DATE_HEADER_RESP
            ]
            == "true"
        )
        assert (
            STREAM_UP_TO_DATE_HEADER_RESP
            not in decide_read(
                facts(), ReadRequest(), up_to_date=False, **common
            ).headers
        )

    def test_the_cursor_is_a_long_poll_header_only(self):
        common = {
            "latest": facts(),
            "last_message_offset": "0000000005",
            "up_to_date": True,
            "response_cursor": "41",
        }
        assert (
            decide_read(facts(), ReadRequest(live="long-poll"), **common).headers[
                STREAM_CURSOR_HEADER_RESP
            ]
            == "41"
        )
        assert (
            STREAM_CURSOR_HEADER_RESP
            not in decide_read(facts(), ReadRequest(), **common).headers
        )

    def test_closure_is_judged_against_the_re_fetched_tail(self):
        """An append landing mid-read must not be reported as closed-at-tail.

        `facts` is the stream as first seen; `latest` is it after the messages
        were read. Here the tail moved on, so the client is behind.
        """
        verdict = decide_read(
            facts(closed=True),
            ReadRequest(),
            latest=facts(tail="0000000009", closed=True),
            last_message_offset="0000000005",
            up_to_date=True,
        )
        assert STREAM_CLOSED_HEADER_RESP not in verdict.headers

    def test_a_close_landing_mid_read_is_reported(self):
        verdict = decide_read(
            facts(closed=False),
            ReadRequest(),
            latest=facts(closed=True),
            last_message_offset="0000000005",
            up_to_date=True,
        )
        assert verdict.headers[STREAM_CLOSED_HEADER_RESP] == "true"

    def test_a_stream_that_vanished_mid_read_is_neither_at_tail_nor_closed(self):
        verdict = decide_read(
            facts(closed=True),
            ReadRequest(),
            latest=None,
            last_message_offset="0000000005",
            up_to_date=True,
        )
        assert STREAM_CLOSED_HEADER_RESP not in verdict.headers
        assert not verdict.headers["etag"].endswith(':c"')

    def test_the_etag_is_keyed_on_the_offset_as_sent(self):
        """Including the literal `now` — that is what a client caches against."""
        verdict = decide_read(
            facts(),
            ReadRequest(offset="now", live="long-poll"),
            latest=facts(),
            last_message_offset=None,
            up_to_date=True,
            response_cursor="41",
        )
        assert ":now:" in verdict.headers["etag"]

    def test_no_offset_is_keyed_as_minus_one(self):
        verdict = decide_read(
            facts(),
            ReadRequest(),
            latest=facts(),
            last_message_offset=None,
            up_to_date=True,
        )
        assert ":-1:" in verdict.headers["etag"]


class TestTheConditionalRead:
    def _verdict(self, if_none_match: str | None):
        return decide_read(
            facts(),
            ReadRequest(if_none_match=if_none_match),
            latest=facts(),
            last_message_offset="0000000005",
            up_to_date=True,
            cors={"access-control-allow-origin": "*"},
        )

    def test_a_matching_etag_is_304(self):
        etag = self._verdict(None).headers["etag"]
        assert self._verdict(etag).status == 304

    def test_a_stale_etag_is_a_full_200(self):
        assert self._verdict('"something-else"').status == 200

    def test_no_if_none_match_is_a_full_200(self):
        assert self._verdict(None).status == 200

    def test_a_304_carries_only_its_etag(self):
        """Preserved as the handler has always sent it, CORS and all else absent.

        Written down here so it is a decision on the record rather than an
        accident of where the `return` sat.
        """
        etag = self._verdict(None).headers["etag"]
        verdict = self._verdict(etag)
        assert verdict.headers == {"etag": etag}
        assert verdict.body == b""


# =============================================================================
# One answer, not two: the plain read and the live push
# =============================================================================


CASES = list(itertools.product([True, False], repeat=3))


class TestThePlainReadAndTheLivePushAgree:
    """The duplication #188 is about.

    `_handle_read` and `_build_sse_control` each decided closure independently,
    and the second one omitted `up_to_date`. These would go red if they diverged
    again.
    """

    @pytest.mark.parametrize(("closed", "at_tail", "up_to_date"), CASES)
    def test_they_agree_on_whether_the_stream_is_closed(
        self, closed: bool, at_tail: bool, up_to_date: bool
    ):
        tail = "0000000005"
        offset = tail if at_tail else "0000000003"

        get = decide_read(
            facts(closed=closed),
            ReadRequest(),
            latest=facts(tail=tail, closed=closed),
            last_message_offset=offset,
            up_to_date=up_to_date,
        )
        sse = sse_control_fields(
            control_offset=offset,
            tail_offset=tail,
            closed=closed,
            up_to_date=up_to_date,
            response_cursor="41",
        )

        assert (STREAM_CLOSED_HEADER_RESP in get.headers) == (SSE_CLOSED_FIELD in sse)

    @pytest.mark.parametrize(("closed", "at_tail", "up_to_date"), CASES)
    def test_they_agree_on_the_offset_they_report(
        self, closed: bool, at_tail: bool, up_to_date: bool
    ):
        tail = "0000000005"
        offset = tail if at_tail else "0000000003"

        get = decide_read(
            facts(closed=closed),
            ReadRequest(),
            latest=facts(tail=tail, closed=closed),
            last_message_offset=offset,
            up_to_date=up_to_date,
        )
        sse = sse_control_fields(
            control_offset=offset,
            tail_offset=tail,
            closed=closed,
            up_to_date=up_to_date,
            response_cursor="41",
        )

        assert get.headers[STREAM_OFFSET_HEADER_RESP] == sse[SSE_OFFSET_FIELD]


class TestTheLivePushControlFrame:
    def test_a_closed_tail_carries_closure_and_no_cursor(self):
        fields = sse_control_fields(
            control_offset="5",
            tail_offset="5",
            closed=True,
            up_to_date=True,
            response_cursor="41",
        )
        assert fields[SSE_CLOSED_FIELD] is True
        assert SSE_CURSOR_FIELD not in fields
        assert SSE_UP_TO_DATE_FIELD not in fields

    def test_an_open_tail_carries_the_cursor_and_up_to_date(self):
        fields = sse_control_fields(
            control_offset="5",
            tail_offset="5",
            closed=False,
            up_to_date=True,
            response_cursor="41",
        )
        assert fields[SSE_CURSOR_FIELD] == "41"
        assert fields[SSE_UP_TO_DATE_FIELD] is True
        assert SSE_CLOSED_FIELD not in fields

    def test_a_catch_up_frame_behind_the_tail_is_not_up_to_date(self):
        """Every message during catch-up gets its own control frame; only the
        one at the tail claims to be up to date."""
        fields = sse_control_fields(
            control_offset="3",
            tail_offset="5",
            closed=False,
            up_to_date=True,
            response_cursor="41",
        )
        assert SSE_UP_TO_DATE_FIELD not in fields
        assert fields[SSE_CURSOR_FIELD] == "41"

    def test_a_closed_stream_mid_catch_up_still_gets_a_cursor(self):
        fields = sse_control_fields(
            control_offset="3",
            tail_offset="5",
            closed=True,
            up_to_date=True,
            response_cursor="41",
        )
        assert SSE_CLOSED_FIELD not in fields
        assert fields[SSE_CURSOR_FIELD] == "41"
