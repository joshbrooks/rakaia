"""The one apply loop, with recording writes in place of an executor (#325).

The executor contract checks that both executors write the right rows; this
checks what the loop hands them, and in what order.
"""

from __future__ import annotations

from typing import Any

import pytest

from rakaia.applying import apply_effects
from rakaia.effects import (
    Delete,
    EffectCollisionError,
    Ref,
    Retire,
    Transition,
    UnresolvedRefError,
    Update,
    Upsert,
)


class _Recorder:
    """The four writes, recording each call. An upsert's row is its lookup with a
    pk of 100 plus its position, so a sibling's `Ref` has something to bind to."""

    def __init__(self, outcomes: list[str] | None = None) -> None:
        self.calls: list[tuple[str, Any]] = []
        self._outcomes = list(outcomes or [])

    def upsert(self, eff: Upsert):
        self.calls.append(("upsert", eff))
        row = {"pk": 100 + len(self.calls), **eff.lookup}
        outcome = self._outcomes.pop(0) if self._outcomes else "created"
        return outcome, lambda field: row["pk"] if field in ("pk", "id") else row[field]

    def update_run(self, effs: list[Update]) -> None:
        self.calls.append(("updates", list(effs)))

    def delete(self, eff: Delete) -> None:
        self.calls.append(("delete", eff))

    def retire(self, eff: Retire) -> list[dict[str, Any]]:
        self.calls.append(("retire", eff))
        return [{"k": 1}]

    def apply(self, effects):
        return apply_effects(
            effects,
            upsert=self.upsert,
            update_run=self.update_run,
            delete=self.delete,
            retire=self.retire,
        )

    def kinds(self) -> list[str]:
        return [kind for kind, _ in self.calls]


def test_writes_then_deletes_then_retires_whatever_the_emitted_order():
    rec = _Recorder()
    rec.apply(
        [
            Retire("a.M", {"k": 1}, patch={"open": False}),
            Delete("a.M", {"k": 2}),
            Upsert("a.M", {"k": 3}, {"n": 1}),
        ]
    )
    assert rec.kinds() == ["upsert", "delete", "retire"]


def test_consecutive_updates_are_handed_over_as_one_run():
    rec = _Recorder()
    rec.apply([Update("a.M", {"k": i}, {"n": 1}) for i in range(3)])
    assert rec.kinds() == ["updates"]
    assert len(rec.calls[0][1]) == 3


def test_a_run_with_an_unresolvable_ref_is_never_handed_over():
    """The whole run is resolved before the executor sees any of it, so an
    executor that applies a run one update at a time cannot half-apply it."""
    rec = _Recorder()
    with pytest.raises(UnresolvedRefError):
        rec.apply(
            [
                Update("a.M", {"k": 1}, {"n": 1}),
                Update("a.M", {"k": 2}, {"n": Ref("nope")}),
            ]
        )
    assert rec.calls == []


def test_a_run_of_updates_ends_at_an_upsert():
    rec = _Recorder()
    rec.apply(
        [
            Update("a.M", {"k": 1}, {"n": 1}),
            Upsert("a.M", {"k": 2}, {"n": 1}),
            Update("a.M", {"k": 3}, {"n": 1}),
        ]
    )
    assert rec.kinds() == ["updates", "upsert", "updates"]


def test_a_ref_is_resolved_to_the_row_a_sibling_produced():
    rec = _Recorder()
    rec.apply(
        [
            Upsert("a.Parent", {"k": 1}, {}, produces="p"),
            Update("a.Child", {"parent_id": Ref("p")}, {"n": 1}),
            Delete("a.Child", {"parent_id": Ref("p"), "gone": True}),
            Retire("a.Child", {"parent_id": Ref("p")}, patch={"open": False}),
        ]
    )
    (_, updates), (_, deleted), (_, retired) = rec.calls[1], rec.calls[2], rec.calls[3]
    assert updates[0].lookup == {"parent_id": 101}
    assert deleted.lookup == {"parent_id": 101, "gone": True}
    assert retired.lookup == {"parent_id": 101}


def test_the_report_counts_what_each_upsert_did():
    rec = _Recorder(outcomes=["created", "written", "skipped", "created"])
    report = rec.apply([Upsert("a.M", {"k": i}, {"n": 1}) for i in range(4)])
    assert (report.upserts_created, report.upserts_written, report.upserts_skipped) == (
        2,
        3,
        1,
    )


def test_only_a_retire_that_asked_to_notify_is_reported():
    rec = _Recorder()
    notifying = Retire(
        "a.M",
        {"k": 1},
        patch={"open": False},
        transition=Transition(kind="closed", key_fields=("k",)),
    )
    report = rec.apply([notifying, Retire("a.M", {"k": 2}, patch={"open": False})])
    assert report.retire_flips == [(notifying, [{"k": 1}])]


def test_overlapping_defaults_are_refused_before_anything_is_written():
    rec = _Recorder()
    with pytest.raises(EffectCollisionError):
        rec.apply(
            [
                Upsert("a.M", {"k": 1}, {"n": 1}),
                Upsert("a.M", {"k": 1}, {"n": 2}),
            ]
        )
    assert rec.calls == []
