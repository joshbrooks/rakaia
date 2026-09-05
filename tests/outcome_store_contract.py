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
    kw.setdefault("subject", offset)
    return Outcome(
        consumer=consumer, stream_path=path, offset=offset, stage="project", **kw
    )


def refused(subject: str, *, consumer: str = "c", path: str = "s", **kw) -> Outcome:
    """An append-stage refusal — no offset, because it never reached the log."""
    kw.setdefault("sequence_key", subject)
    kw.setdefault("status", "refused")
    return Outcome(
        consumer=consumer,
        stream_path=path,
        offset=None,
        subject=subject,
        stage="append",
        **kw,
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
        outcomes.record(refused("row-1", reasons=("declined_upstream",)))
        [got] = outcomes.latest("c", "s")
        assert got.offset is None and got.stage == "append"

    # --- immutability and attempts ------------------------------------------

    def test_a_later_attempt_is_what_latest_reports(self, outcomes):
        """Renamed from a claim it could not keep.

        It used to say it proved the store never *replaces* a row, while reading
        only through `latest()` — which collapses attempts, so an in-place
        overwrite is invisible to it. Mutation that proved it vacuous: make
        `record` delete the matching row before appending; this test stayed green.
        Append-only storage is not observable through this interface, so the name
        now claims only what the assertion can see.
        """
        outcomes.record(project("0000000001", reasons=("first",), attempt=1))
        outcomes.record(project("0000000001", reasons=("second",), attempt=2))
        [got] = outcomes.latest("c", "s")
        assert got.reasons == ("second",) and got.attempt == 2

    def test_every_refused_subject_is_reported_separately(self, outcomes):
        """The case the design exists for, and the one it originally lost.

        Refusals never reach the log, so they have no offset. Keyed on offset they
        all collapsed to a single row and the first was reported as though it spoke
        for the rest — one bad row making a whole form look accounted for, which is
        the defect this record is meant to expose.
        """
        for i in (1, 2, 3):
            outcomes.record(refused(f"row-{i}", reasons=(f"rule_{i}",)))

        got = outcomes.latest("c", "s")
        assert [(o.subject, o.reasons) for o in got] == [
            ("row-1", ("rule_1",)),
            ("row-2", ("rule_2",)),
            ("row-3", ("rule_3",)),
        ]

    def test_a_refusal_and_a_projection_failure_coexist(self, outcomes):
        """One with an offset, one without, both about different subjects."""
        outcomes.record(project("0000000001"))
        outcomes.record(refused("row-9"))
        assert [(o.subject, o.offset) for o in outcomes.latest("c", "s")] == [
            ("0000000001", "0000000001"),
            ("row-9", None),
        ]

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
        outcomes.record(refused("row-1"))
        outcomes.record(project("0000000001"))
        assert [o.offset for o in outcomes.latest("c", "s")] == [
            "0000000001",
            "0000000002",
            None,
        ]

    # --- what a clean run looks like ---------------------------------------

    def test_the_order_does_not_depend_on_when_things_were_recorded(self, outcomes):
        """Refusals share the "no offset" sort position, so without a further term
        the order is whatever the storage happened to do. Mutation: drop `subject`
        from the sort key — dicts preserve insertion order and a stable sort keeps
        it, so this comes back reversed."""
        for subject in ("row-3", "row-1", "row-2"):
            outcomes.record(refused(subject))
        assert [o.subject for o in outcomes.latest("c", "s")] == [
            "row-1",
            "row-2",
            "row-3",
        ]

    def test_re_recording_an_attempt_replaces_it(self, outcomes):
        """Two outcomes for one subject at the same attempt is a caller error either
        way; last-write-wins is the less surprising of the two answers, and `>` versus
        `>=` is otherwise invisible."""
        outcomes.record(project("0000000001", reasons=("first",), attempt=2))
        outcomes.record(project("0000000001", reasons=("corrected",), attempt=2))
        [got] = outcomes.latest("c", "s")
        assert got.reasons == ("corrected",)

    def test_a_clean_run_records_nothing(self, outcomes):
        """The property the exceptions-only design rests on.

        Nothing failed, so nothing was written. Stated as a test because it is
        the load-bearing half of "the cursor is the success record": if a store
        ever synthesised a row per applied event, every count built on this table
        would change meaning without anything failing.
        """
        assert outcomes.latest("c", "s") == []
