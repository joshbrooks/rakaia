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
