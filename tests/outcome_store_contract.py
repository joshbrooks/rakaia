"""Shared conformance contract for `OutcomeStore` (ADR 0007).

An outcome is as store-agnostic as a cursor: the decision about what to record is
made once, and only the keeping of it is backend-shaped. So every backend that
keeps outcomes is held to the same behaviour here, and "does this work on the
file store too?" is a red test rather than a question. Subclass
`OutcomeStoreContract` in each backend's test package and provide an `outcomes`
fixture returning a fresh, empty store. See ADR 0002 / #36 for the pattern.

This module is intentionally not named `test_*`, so pytest does not collect it
directly; only the backend subclasses run it.

**Scope, and what is deliberately not pinned.** Offsets here are illustrative
opaque strings, shorter than any real store issues, and the contract only ever
compares them lexicographically — so a backend issuing a compound
`{seq}_{byte}` token and one issuing a zero-padded integer both satisfy it.
Offset *opacity* is not pinned here and cannot usefully be: both formats are
zero-padded precisely so that lexicographic and numeric order agree, which means
an ordering assertion cannot catch a store that parses one. That property lives
in `test_cross_backend_cursors.py`, where a foreign format is refused rather than
misread. Retention, admin surfaces and
re-drive are out of scope: the first is a policy a consumer schedules, and the
last is deliberately not in this version (ADR 0007, "Not in v1").

What *is* contract: an outcome is never updated in place, and `latest` collapses
attempts and nothing else. There is no gap query to pin — ADR 0007 Decision 3
records why one cannot exist when success leaves no trace.
"""

from __future__ import annotations

from rakaia.outcomes import Outcome


def project(offset: str, *, consumer: str = "c", path: str = "s", **kw) -> Outcome:
    """A projection-stage outcome, the common case in these tests."""
    kw.setdefault("sequence_key", "seq")
    kw.setdefault("status", "failed")
    return Outcome(
        consumer=consumer, stream_path=path, offset=offset, stage="project", **kw
    )


class OutcomeStoreContract:
    """Contract every outcome store must uphold.

    Subclasses provide::

        @pytest.fixture
        def outcomes(self):
            return MyOutcomeStore()
    """

    # --- the seam itself ----------------------------------------------------

    def test_satisfies_the_outcome_store_protocol(self, outcomes):
        from rakaia.protocols import OutcomeStore

        assert isinstance(outcomes, OutcomeStore)

    def test_an_empty_store_reports_nothing(self, outcomes):
        assert outcomes.latest("c", "s") == []

    # --- recording ----------------------------------------------------------

    def test_a_recorded_outcome_is_returned(self, outcomes):
        outcomes.record(project("0000000001", reasons=("bad_total",)))
        [got] = outcomes.latest("c", "s")
        assert (got.offset, got.status, got.reasons) == (
            "0000000001",
            "failed",
            ("bad_total",),
        )

    def test_outcomes_are_scoped_by_consumer_and_stream(self, outcomes):
        """Distinct offsets per scope, deliberately.

        Mutation watched: dropping the consumer/stream filter from `latest`. With
        all three rows sharing one offset the answer collapses to a single entry
        either way and the mutation survives — the same shape of false green as
        asserting a count where the contents are what matter.
        """
        outcomes.record(project("0000000001", reasons=("mine",)))
        outcomes.record(project("0000000002", consumer="other", reasons=("theirs",)))
        outcomes.record(project("0000000003", path="other", reasons=("elsewhere",)))

        assert [(o.offset, o.reasons) for o in outcomes.latest("c", "s")] == [
            ("0000000001", ("mine",))
        ]
        assert [(o.offset, o.reasons) for o in outcomes.latest("other", "s")] == [
            ("0000000002", ("theirs",))
        ]
        assert [(o.offset, o.reasons) for o in outcomes.latest("c", "other")] == [
            ("0000000003", ("elsewhere",))
        ]

    def test_params_round_trip(self, outcomes):
        outcomes.record(project("0000000001", params={"row": "3", "budget": "500"}))
        [got] = outcomes.latest("c", "s")
        assert got.params == {"row": "3", "budget": "500"}

    def test_an_append_stage_outcome_has_no_offset(self, outcomes):
        outcomes.record(
            Outcome(
                consumer="c",
                stream_path="s",
                offset=None,
                sequence_key="seq",
                stage="append",
                status="refused",
                reasons=("declined_upstream",),
            )
        )
        [got] = outcomes.latest("c", "s")
        assert got.offset is None and got.stage == "append"

    # --- immutability and attempts ------------------------------------------

    def test_a_second_attempt_does_not_overwrite_the_first(self, outcomes):
        """The record is append-only: history accumulates rather than being edited.

        `latest` collapses it for reading, so this is asserted on the store's own
        terms — a second attempt must not *replace* the first row.
        """
        outcomes.record(project("0000000001", reasons=("first",), attempt=1))
        outcomes.record(project("0000000001", reasons=("second",), attempt=2))
        [got] = outcomes.latest("c", "s")
        assert got.reasons == ("second",) and got.attempt == 2

    def test_latest_takes_the_highest_attempt_not_the_last_written(self, outcomes):
        """Out-of-order writes must not decide the answer.

        Mutation watched: `latest` returning the most recently recorded row. A
        re-drive that records attempt 2 before a slow attempt 1 lands would then
        report the older verdict as current.
        """
        outcomes.record(project("0000000001", reasons=("second",), attempt=2))
        outcomes.record(project("0000000001", reasons=("first",), attempt=1))
        [got] = outcomes.latest("c", "s")
        assert got.reasons == ("second",)

    def test_latest_returns_one_row_per_offset(self, outcomes):
        for offset in ("0000000001", "0000000002", "0000000003"):
            outcomes.record(project(offset, attempt=1))
            outcomes.record(project(offset, attempt=2))
        assert [o.offset for o in outcomes.latest("c", "s")] == [
            "0000000001",
            "0000000002",
            "0000000003",
        ]

    def test_latest_orders_by_offset_with_append_stage_last(self, outcomes):
        outcomes.record(project("0000000002"))
        outcomes.record(
            Outcome(
                consumer="c",
                stream_path="s",
                offset=None,
                sequence_key="seq",
                stage="append",
                status="refused",
            )
        )
        outcomes.record(project("0000000001"))
        assert [o.offset for o in outcomes.latest("c", "s")] == [
            "0000000001",
            "0000000002",
            None,
        ]

    # --- what a clean run looks like ---------------------------------------

    def test_a_clean_run_records_nothing(self, outcomes):
        """The property the exceptions-only design rests on.

        Nothing failed, so nothing was written. Stated as a test because it is
        the load-bearing half of "the cursor is the success record": if a store
        ever synthesised a row per applied event, every count built on this table
        would change meaning without anything failing.
        """
        assert outcomes.latest("c", "s") == []
