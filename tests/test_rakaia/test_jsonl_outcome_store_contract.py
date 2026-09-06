"""The file-backed outcome store against the shared contract.

The point of running the same suite twice. `InMemoryOutcomeStore` and this one
share no code, and this one has no database under it, so the pair passing is what
makes ADR 0007's claim — that an outcome is as store-agnostic as a cursor — a
measurement rather than an assertion.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rakaia.jsonl_outcomes import JsonlOutcomeStore
from tests.outcome_store_contract import OutcomeStoreContract


class TestJsonlOutcomeStoreContract(OutcomeStoreContract):
    @pytest.fixture
    def outcomes(self, tmp_path: Path) -> JsonlOutcomeStore:
        # fsync off: tmp_path has no durability question to answer and the
        # syscall is pure cost per recorded row.
        return JsonlOutcomeStore(tmp_path / "outcomes", fsync=False)


class TestJsonlOutcomeStoreDurability:
    """What the shared contract cannot ask for, because the in-memory store
    cannot satisfy it."""

    def test_outcomes_survive_a_new_store_over_the_same_root(self, tmp_path: Path):
        from rakaia.outcomes import Outcome

        root = tmp_path / "outcomes"
        JsonlOutcomeStore(root, fsync=False).record(
            Outcome(
                consumer="c",
                stream_path="submission/tf611",
                subject="row-1",
                offset="0000000001",
                sequence_key="seq",
                stage="project",
                status="failed",
                reasons=("bad_total",),
            )
        )
        [got] = JsonlOutcomeStore(root, fsync=False).latest("c", "submission/tf611")
        assert got.reasons == ("bad_total",)

    # The empty string used to be here. It is now refused at construction — an empty
    # name collides with whatever else escapes to the same path segment — so the case
    # this file covered moved to `test_the_names_cannot_be_empty`.
    @pytest.mark.parametrize("hostile", ["a/b", "../escape", "..", "a%2Fb"])
    @pytest.mark.parametrize("field", ["consumer", "stream_path"])
    def test_every_file_stays_directly_under_the_root(
        self, tmp_path: Path, field: str, hostile: str
    ):
        """A consumer id and a stream path are names, not paths.

        Asserted as *containment*, not as a round trip. Leaving either component
        unencoded still round-trips — `a/b` reads back from `root/a/b/…` exactly as
        it was written — so a read-what-you-wrote test passes while `../escape`
        writes outside the root entirely. The property worth holding is the layout:
        one directory per consumer, one file per stream, nothing deeper and nothing
        above.
        """
        from rakaia.outcomes import Outcome

        root = tmp_path / "outcomes"
        store = JsonlOutcomeStore(root, fsync=False)
        fixed = {"consumer": "c", "stream_path": "s"}
        store.record(
            Outcome(
                **{**fixed, field: hostile},
                subject="row-1",
                offset="0000000001",
                sequence_key="seq",
                stage="project",
                status="failed",
                reasons=(hostile,),
            )
        )

        written = list(root.rglob("*.jsonl"))
        assert len(written) == 1
        assert written[0].parent.parent == root, "a name became a directory tree"
        assert not list(tmp_path.glob("*.jsonl")), "a name escaped the root"

        scope = {**fixed, field: hostile}
        assert [
            o.reasons for o in store.latest(scope["consumer"], scope["stream_path"])
        ] == [(hostile,)]

    def test_a_torn_trailing_line_is_skipped_not_fatal(self, tmp_path: Path):
        """A crash mid-append leaves half a line. Skipping it loses one outcome;
        failing the read would lose the whole report."""
        from rakaia.outcomes import Outcome

        root = tmp_path / "outcomes"
        store = JsonlOutcomeStore(root, fsync=False)
        store.record(
            Outcome(
                consumer="c",
                stream_path="s",
                subject="row-1",
                offset="0000000001",
                sequence_key="seq",
                stage="project",
                status="failed",
            )
        )
        target = next(root.rglob("*.jsonl"))
        with target.open("a", encoding="utf-8") as fh:
            fh.write('{"consumer": "c", "stream_pa')

        assert [o.offset for o in store.latest("c", "s")] == ["0000000001"]

    def test_a_line_this_version_cannot_build_is_skipped_not_fatal(
        self, tmp_path: Path
    ):
        """A field added by a later version must not take the report down.

        `Outcome(**payload)` raises `TypeError` on an unknown key, and the read
        caught only `ValueError` — so one line written by a newer version lost
        every outcome in the file. The ADR forecasts exactly this key: "#232
        lands… an outcome should name its issuing store".
        """
        import json

        from rakaia.outcomes import Outcome

        root = tmp_path / "outcomes"
        store = JsonlOutcomeStore(root, fsync=False)
        store.record(
            Outcome(
                consumer="c",
                stream_path="s",
                subject="row-1",
                offset="0000000001",
                sequence_key="seq",
                stage="project",
                status="failed",
            )
        )
        target = next(root.rglob("*.jsonl"))
        with target.open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {
                        "consumer": "c",
                        "stream_path": "s",
                        "subject": "row-2",
                        "offset": "0000000002",
                        "sequence_key": "seq",
                        "stage": "project",
                        "status": "failed",
                        "reasons": [],
                        "params": {},
                        "attempt": 1,
                        "store": "from-a-later-version",
                    }
                )
                + "\n"
            )

        assert [o.subject for o in store.latest("c", "s")] == ["row-1", "row-2"]

    def test_a_line_missing_a_required_field_is_skipped_not_fatal(self, tmp_path: Path):
        """The other half of "a field added by a later version, or one removed".

        The unknown-key half is held by the field filter, so narrowing the `except`
        back to `ValueError` alone left the suite green and the `TypeError` arm
        looked dead. It is not: a line written before a field existed is missing a
        required argument, and building it raises `TypeError`, which without this
        arm takes the whole report down.
        """
        import json

        from rakaia.outcomes import Outcome

        root = tmp_path / "outcomes"
        store = JsonlOutcomeStore(root, fsync=False)
        store.record(
            Outcome(
                consumer="c",
                stream_path="s",
                subject="row-1",
                offset="0000000001",
                sequence_key="seq",
                stage="project",
                status="failed",
            )
        )
        target = next(root.rglob("*.jsonl"))
        with target.open("a", encoding="utf-8") as fh:
            # No `sequence_key` — written by a version predating the field.
            fh.write(
                json.dumps(
                    {
                        "consumer": "c",
                        "stream_path": "s",
                        "subject": "row-2",
                        "offset": "0000000002",
                        "stage": "project",
                        "status": "failed",
                        "reasons": [],
                        "params": {},
                        "attempt": 1,
                    }
                )
                + "\n"
            )

        assert [o.subject for o in store.latest("c", "s")] == ["row-1"]

    def test_the_record_after_a_torn_tail_is_not_lost_with_it(self, tmp_path: Path):
        """Skipping a torn line on read is half the recovery.

        An append that simply continued would glue its own record onto the
        fragment, and *both* are then dropped as one unparseable line — so a crash
        costs the outcome it interrupted and the next one too. The write side has
        to cut the fragment first. Mutation: drop the torn-tail cut from `record`;
        row-2 disappears.
        """
        from rakaia.outcomes import Outcome

        def refused(subject: str) -> Outcome:
            return Outcome(
                consumer="c",
                stream_path="s",
                subject=subject,
                offset=None,
                sequence_key=subject,
                stage="append",
                status="refused",
            )

        root = tmp_path / "outcomes"
        store = JsonlOutcomeStore(root, fsync=False)
        store.record(refused("row-1"))
        target = next(root.rglob("*.jsonl"))
        with target.open("a", encoding="utf-8") as fh:
            fh.write('{"consumer": "c", "stream_pa')
        store.record(refused("row-2"))
        store.record(refused("row-3"))

        assert [o.subject for o in store.latest("c", "s")] == [
            "row-1",
            "row-2",
            "row-3",
        ]

    @pytest.mark.parametrize("line", ["[]", "null", "42", '"text"'])
    def test_a_line_that_is_json_but_not_an_object_is_skipped_not_fatal(
        self, tmp_path: Path, line: str
    ):
        """Valid JSON is not the same as a record. Each of these parsed and then
        raised on `.items()`, outside the arm that catches a bad line — one such
        line took the whole report down."""
        from rakaia.outcomes import Outcome

        root = tmp_path / "outcomes"
        store = JsonlOutcomeStore(root, fsync=False)
        store.record(
            Outcome(
                consumer="c",
                stream_path="s",
                subject="row-1",
                offset="0000000001",
                sequence_key="seq",
                stage="project",
                status="failed",
            )
        )
        target = next(root.rglob("*.jsonl"))
        with target.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

        assert [o.subject for o in store.latest("c", "s")] == ["row-1"]

    def test_a_line_naming_another_scope_is_not_reported(self, tmp_path: Path):
        """The file name says which scope this is, and so does each line; the
        line wins. A case-folding filesystem gives consumers `A` and `a` one file,
        and trusting the name would show each the other's outcomes. Mutation:
        drop the scope filter from `latest`; `row-other` appears."""
        from rakaia.outcomes import Outcome, encode

        root = tmp_path / "outcomes"
        store = JsonlOutcomeStore(root, fsync=False)
        mine = Outcome(
            consumer="c",
            stream_path="s",
            subject="row-1",
            offset="0000000001",
            sequence_key="seq",
            stage="project",
            status="failed",
        )
        store.record(mine)
        target = next(root.rglob("*.jsonl"))
        with target.open("a", encoding="utf-8") as fh:
            for foreign in (
                Outcome(
                    consumer="C",
                    stream_path="s",
                    subject="row-other",
                    offset="0000000002",
                    sequence_key="seq",
                    stage="project",
                    status="failed",
                ),
                Outcome(
                    consumer="c",
                    stream_path="S",
                    subject="row-other",
                    offset="0000000003",
                    sequence_key="seq",
                    stage="project",
                    status="failed",
                ),
            ):
                fh.write(encode(foreign) + "\n")

        assert [o.subject for o in store.latest("c", "s")] == ["row-1"]


class TestJsonlOutcomeStoreLocking:
    """The one thing the append lock orders, pinned the only way it can be.

    With `O_APPEND` a short `write()` lands whole, so two unlocked writers never
    interleave and a test on the written bytes stays green with the lock removed.
    What the lock guards is the torn-tail check — read the tail, maybe truncate,
    then write — so the test holds one writer inside that window and checks the
    second is still waiting. Removing the `LOCK_EX` call makes it fail; the
    explicit unlock is not pinned, because closing the file releases it anyway.
    """

    def test_a_second_writer_waits_while_the_first_is_inside_the_torn_tail_check(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        import threading

        from rakaia import jsonl_outcomes
        from rakaia.outcomes import Outcome

        def refused(subject: str) -> Outcome:
            return Outcome(
                consumer="c",
                stream_path="s",
                subject=subject,
                offset=None,
                sequence_key=subject,
                stage="append",
                status="refused",
            )

        store = JsonlOutcomeStore(tmp_path / "outcomes", fsync=False)
        store.record(refused("row-0"))  # so the file and its directory exist
        real = jsonl_outcomes._discard_torn_tail
        second_still_waiting: list[bool] = []
        entered = threading.Event()

        def held_open(fh):
            # Only the first writer is held; the second must run the real thing.
            if not entered.is_set():
                entered.set()
                other = threading.Thread(target=store.record, args=(refused("row-2"),))
                other.start()
                other.join(timeout=0.5)
                second_still_waiting.append(other.is_alive())
            real(fh)

        monkeypatch.setattr(jsonl_outcomes, "_discard_torn_tail", held_open)
        store.record(refused("row-1"))

        assert second_still_waiting == [True]
        assert [o.subject for o in store.latest("c", "s")] == [
            "row-0",
            "row-1",
            "row-2",
        ]
