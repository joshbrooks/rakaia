"""Spike tests for rakaia.deltas — partial-update and part (array) events.

Written test-first. Each class pins one question the design has to answer, and
the failure modes are as load-bearing as the happy paths: a delta is only worth
having if a fold that lost its place *says so* instead of quietly producing a
state nobody ever saved.
"""

from __future__ import annotations

import json

import pytest

from rakaia.deltas import (
    PART_ID_KEY,
    PATCH_LABEL,
    AddPart,
    ClearField,
    DeltaConflictError,
    MovePart,
    NoBaseSnapshotError,
    RemovePart,
    SetField,
    apply_delta,
    decode_patch,
    encode_patch,
    fold_snapshot,
    is_patch,
    parts_of,
)
from rakaia.types import StreamMessage


def _msg(payload: dict, *, label: str = "update", offset: str = "1") -> StreamMessage:
    return StreamMessage(
        data=json.dumps(payload).encode("utf-8"),
        offset=offset,
        timestamp=1.0,
        label=label,
    )


def _patch_msg(*ops, offset: str = "1") -> StreamMessage:
    return _msg(encode_patch(ops), label=PATCH_LABEL, offset=offset)


# =============================================================================
# 1. Partial update — set and clear a location, without re-carrying the blob
# =============================================================================


class TestSetField:
    def test_sets_a_top_level_key(self):
        state = {"suku": "Aileu", "output": 3}
        assert apply_delta(state, SetField(("output",), 4)) == {
            "suku": "Aileu",
            "output": 4,
        }

    def test_does_not_mutate_the_input(self):
        state = {"output": 3}
        apply_delta(state, SetField(("output",), 4))
        assert state == {"output": 3}

    def test_sets_a_nested_key(self):
        state = {"fields": {"a": 1, "b": 2}}
        assert apply_delta(state, SetField(("fields", "b"), 9)) == {
            "fields": {"a": 1, "b": 9}
        }

    def test_sets_a_key_that_did_not_exist(self):
        assert apply_delta({"a": 1}, SetField(("b",), 2)) == {"a": 1, "b": 2}

    def test_sets_a_field_inside_a_part(self):
        state = {"rows": [{PART_ID_KEY: "p1", "n": 1}, {PART_ID_KEY: "p2", "n": 2}]}
        out = apply_delta(state, SetField(("rows", "p2", "n"), 5))
        assert out["rows"][1]["n"] == 5
        assert out["rows"][0]["n"] == 1

    def test_a_missing_parent_is_a_conflict_not_an_autovivify(self):
        # Autovivifying {"fields": {"x": 1}} here would let a patch written
        # against one schema silently invent a branch in another.
        with pytest.raises(DeltaConflictError):
            apply_delta({"a": 1}, SetField(("fields", "x"), 1))

    def test_an_unknown_part_id_is_a_conflict(self):
        state = {"rows": [{PART_ID_KEY: "p1", "n": 1}]}
        with pytest.raises(DeltaConflictError):
            apply_delta(state, SetField(("rows", "nope", "n"), 5))

    def test_empty_path_is_rejected_at_construction(self):
        with pytest.raises(ValueError):
            SetField((), 1)


class TestClearField:
    def test_removes_a_key(self):
        assert apply_delta({"a": 1, "b": 2}, ClearField(("b",))) == {"a": 1}

    def test_removes_a_nested_key(self):
        state = {"fields": {"a": 1, "b": 2}}
        assert apply_delta(state, ClearField(("fields", "b"))) == {"fields": {"a": 1}}

    def test_clearing_an_absent_key_is_a_conflict(self):
        # A clear of something that is not there means the fold's base is not
        # the base the producer patched against.
        with pytest.raises(DeltaConflictError):
            apply_delta({"a": 1}, ClearField(("b",)))

    def test_clearing_a_field_of_a_part(self):
        state = {"rows": [{PART_ID_KEY: "p1", "n": 1, "note": "x"}]}
        out = apply_delta(state, ClearField(("rows", "p1", "note")))
        assert out["rows"][0] == {PART_ID_KEY: "p1", "n": 1}


class TestAWholePartIsNotAField:
    # A path whose last segment is a part id addresses the row, not a field of
    # it. Both ops refuse it rather than approximating RemovePart — which would
    # skip the identity checks and, for ClearField, used to silently no-op.
    def test_set_on_a_whole_part_is_refused(self):
        state = {"rows": [{PART_ID_KEY: "p1", "n": 1}]}
        with pytest.raises(DeltaConflictError, match="whole part"):
            apply_delta(state, SetField(("rows", "p1"), {"n": 2}))

    def test_clear_on_a_whole_part_is_refused(self):
        state = {"rows": [{PART_ID_KEY: "p1", "n": 1}]}
        with pytest.raises(DeltaConflictError, match="whole part"):
            apply_delta(state, ClearField(("rows", "p1")))

    def test_the_row_survives_the_refusal(self):
        state = {"rows": [{PART_ID_KEY: "p1", "n": 1}]}
        with pytest.raises(DeltaConflictError):
            apply_delta(state, ClearField(("rows", "p1")))
        assert state == {"rows": [{PART_ID_KEY: "p1", "n": 1}]}


# =============================================================================
# 2. Part ops — add / remove / move a row of an array
# =============================================================================


class TestAddPart:
    def test_appends_at_the_index(self):
        state = {"rows": [{PART_ID_KEY: "p1"}]}
        out = apply_delta(state, AddPart("rows", 1, "p2", {"n": 7}))
        assert out["rows"] == [{PART_ID_KEY: "p1"}, {PART_ID_KEY: "p2", "n": 7}]

    def test_inserts_at_a_middle_index(self):
        state = {"rows": [{PART_ID_KEY: "a"}, {PART_ID_KEY: "b"}]}
        out = apply_delta(state, AddPart("rows", 1, "c", {}))
        assert [r[PART_ID_KEY] for r in out["rows"]] == ["a", "c", "b"]

    def test_stamps_the_part_id_into_the_row(self):
        # The whole point: the source data has no stable child id, so the add
        # event mints one and the folded row carries it (ADR 0001).
        out = apply_delta({"rows": []}, AddPart("rows", 0, "p1", {"n": 1}))
        assert out["rows"][0][PART_ID_KEY] == "p1"

    def test_creates_the_array_when_the_key_is_absent(self):
        out = apply_delta({}, AddPart("rows", 0, "p1", {"n": 1}))
        assert out["rows"] == [{PART_ID_KEY: "p1", "n": 1}]

    def test_a_duplicate_part_id_is_a_conflict(self):
        state = {"rows": [{PART_ID_KEY: "p1"}]}
        with pytest.raises(DeltaConflictError):
            apply_delta(state, AddPart("rows", 1, "p1", {}))

    def test_an_index_past_the_end_is_a_conflict(self):
        with pytest.raises(DeltaConflictError):
            apply_delta({"rows": []}, AddPart("rows", 3, "p1", {}))

    def test_a_negative_index_is_rejected_at_construction(self):
        with pytest.raises(ValueError):
            AddPart("rows", -1, "p1", {})

    def test_a_value_carrying_the_part_id_key_is_rejected(self):
        with pytest.raises(ValueError):
            AddPart("rows", 0, "p1", {PART_ID_KEY: "other"})


class TestRemovePart:
    def test_removes_by_id_not_by_position(self):
        state = {"rows": [{PART_ID_KEY: "a"}, {PART_ID_KEY: "b"}, {PART_ID_KEY: "c"}]}
        out = apply_delta(state, RemovePart("rows", "b"))
        assert [r[PART_ID_KEY] for r in out["rows"]] == ["a", "c"]

    def test_an_unknown_part_id_is_a_conflict(self):
        with pytest.raises(DeltaConflictError):
            apply_delta({"rows": [{PART_ID_KEY: "a"}]}, RemovePart("rows", "zz"))

    def test_an_unknown_array_is_a_conflict(self):
        with pytest.raises(DeltaConflictError):
            apply_delta({}, RemovePart("rows", "a"))


class TestMovePart:
    def test_moves_to_a_later_index(self):
        state = {"rows": [{PART_ID_KEY: c} for c in "abc"]}
        out = apply_delta(state, MovePart("rows", "a", 2))
        assert [r[PART_ID_KEY] for r in out["rows"]] == ["b", "c", "a"]

    def test_moves_to_an_earlier_index(self):
        state = {"rows": [{PART_ID_KEY: c} for c in "abc"]}
        out = apply_delta(state, MovePart("rows", "c", 0))
        assert [r[PART_ID_KEY] for r in out["rows"]] == ["c", "a", "b"]

    def test_the_index_is_the_destination_after_removal(self):
        # Pinned because "index" is ambiguous for a move: it is the position in
        # the list *without* the moved element, which is what a drag-and-drop
        # UI reports and what makes move(x, i) idempotent for i == current.
        state = {"rows": [{PART_ID_KEY: c} for c in "abcd"]}
        out = apply_delta(state, MovePart("rows", "b", 2))
        assert [r[PART_ID_KEY] for r in out["rows"]] == ["a", "c", "b", "d"]

    def test_moving_to_its_current_position_is_a_no_op(self):
        state = {"rows": [{PART_ID_KEY: c} for c in "abc"]}
        out = apply_delta(state, MovePart("rows", "b", 1))
        assert [r[PART_ID_KEY] for r in out["rows"]] == ["a", "b", "c"]

    def test_carries_the_row_content_with_it(self):
        state = {"rows": [{PART_ID_KEY: "a", "n": 1}, {PART_ID_KEY: "b", "n": 2}]}
        out = apply_delta(state, MovePart("rows", "a", 1))
        assert out["rows"][1] == {PART_ID_KEY: "a", "n": 1}

    def test_an_index_past_the_end_is_a_conflict(self):
        with pytest.raises(DeltaConflictError):
            apply_delta({"rows": [{PART_ID_KEY: "a"}]}, MovePart("rows", "a", 4))

    def test_an_unknown_part_id_is_a_conflict(self):
        with pytest.raises(DeltaConflictError):
            apply_delta({"rows": [{PART_ID_KEY: "a"}]}, MovePart("rows", "z", 0))


# =============================================================================
# 3. The wire form — a patch event's payload
# =============================================================================


class TestEncoding:
    @pytest.mark.parametrize(
        "delta",
        [
            SetField(("fields", "output"), 4),
            SetField(("rows", "p1", "n"), None),
            ClearField(("fields", "note")),
            AddPart("rows", 2, "p9", {"n": 1, "s": "x"}),
            RemovePart("rows", "p9"),
            MovePart("rows", "p9", 0),
        ],
    )
    def test_round_trips_through_json(self, delta):
        payload = json.loads(json.dumps(encode_patch([delta])))
        assert decode_patch(payload) == [delta]

    def test_a_patch_carries_several_ops_in_order(self):
        ops = [
            SetField(("a",), 1),
            AddPart("rows", 0, "p", {}),
            MovePart("rows", "p", 0),
        ]
        assert decode_patch(encode_patch(ops)) == ops

    def test_paths_are_json_pointers(self):
        assert encode_patch([SetField(("fields", "output"), 4)])["ops"][0]["path"] == (
            "/fields/output"
        )

    def test_a_path_segment_with_a_slash_survives_the_round_trip(self):
        delta = SetField(("fields", "a/b~c"), 1)
        assert decode_patch(encode_patch([delta])) == [delta]

    def test_is_patch_recognises_the_payload(self):
        assert is_patch(encode_patch([SetField(("a",), 1)]))

    def test_is_patch_rejects_an_ordinary_snapshot(self):
        assert not is_patch({"suku": "Aileu", "ops": "not a patch"})

    def test_an_unknown_op_is_rejected_loudly(self):
        with pytest.raises(ValueError):
            decode_patch({"ops": [{"op": "frobnicate", "path": "/a"}]})


# =============================================================================
# 4. Folding a stream — snapshots and patches together
# =============================================================================


class TestFoldSnapshot:
    def test_a_lone_snapshot_folds_to_itself(self):
        assert fold_snapshot([_msg({"a": 1})]) == {"a": 1}

    def test_a_patch_applies_to_the_preceding_snapshot(self):
        msgs = [_msg({"a": 1, "b": 2}), _patch_msg(SetField(("b",), 9))]
        assert fold_snapshot(msgs) == {"a": 1, "b": 9}

    def test_a_later_snapshot_replaces_the_state_entirely(self):
        msgs = [
            _msg({"a": 1}),
            _patch_msg(SetField(("a",), 2)),
            _msg({"z": 26}),
        ]
        assert fold_snapshot(msgs) == {"z": 26}

    def test_patches_compose_in_order(self):
        msgs = [
            _msg({"rows": []}),
            _patch_msg(AddPart("rows", 0, "a", {"n": 1})),
            _patch_msg(AddPart("rows", 1, "b", {"n": 2})),
            _patch_msg(MovePart("rows", "b", 0)),
            _patch_msg(SetField(("rows", "a", "n"), 10)),
        ]
        state = fold_snapshot(msgs)
        assert [(r[PART_ID_KEY], r["n"]) for r in state["rows"]] == [
            ("b", 2),
            ("a", 10),
        ]

    def test_a_patch_with_no_base_is_refused(self):
        # The single most important failure: reading from a mid-stream offset
        # lands on a patch, and folding it against {} would produce a state that
        # was never saved. Deltas cost random access; this is where you pay.
        with pytest.raises(NoBaseSnapshotError):
            fold_snapshot([_patch_msg(SetField(("b",), 9))])

    def test_an_explicit_base_lets_an_incremental_tail_fold(self):
        base = {"a": 1, "b": 2}
        msgs = [_patch_msg(SetField(("b",), 9))]
        assert fold_snapshot(msgs, base=base) == {"a": 1, "b": 9}

    def test_the_base_is_not_mutated(self):
        base = {"a": 1}
        fold_snapshot([_patch_msg(SetField(("a",), 2))], base=base)
        assert base == {"a": 1}

    def test_a_tombstone_folds_to_none(self):
        msgs = [_msg({"a": 1}), _msg({}, label="delete")]
        assert fold_snapshot(msgs) is None

    def test_a_patch_after_a_tombstone_is_refused(self):
        msgs = [
            _msg({"a": 1}),
            _msg({}, label="delete"),
            _patch_msg(SetField(("a",), 2)),
        ]
        with pytest.raises(NoBaseSnapshotError):
            fold_snapshot(msgs)

    def test_a_conflicting_patch_names_the_offset_it_failed_at(self):
        msgs = [
            _msg({"a": 1}, offset="7"),
            _patch_msg(ClearField(("nope",)), offset="8"),
        ]
        with pytest.raises(DeltaConflictError) as exc:
            fold_snapshot(msgs)
        assert "8" in str(exc.value)

    def test_an_empty_message_list_with_no_base_folds_to_none(self):
        assert fold_snapshot([]) is None

    def test_folding_is_deterministic_across_repeats(self):
        msgs = [
            _msg({"rows": []}),
            _patch_msg(AddPart("rows", 0, "a", {"n": 1})),
            _patch_msg(AddPart("rows", 1, "b", {"n": 2})),
        ]
        assert fold_snapshot(msgs) == fold_snapshot(msgs)


# =============================================================================
# 5. The payoff — a folded array is reconcile_tree-ready
# =============================================================================


class TestPartsAreProjectable:
    def test_parts_of_yields_id_and_position(self):
        state = {"rows": [{PART_ID_KEY: "a", "n": 1}, {PART_ID_KEY: "b", "n": 2}]}
        assert parts_of(state, "rows") == [
            ("a", 0, {PART_ID_KEY: "a", "n": 1}),
            ("b", 1, {PART_ID_KEY: "b", "n": 2}),
        ]

    def test_a_reorder_changes_position_but_not_identity(self):
        # ADR 0001's caveat, closed: under full snapshots [a,b,c] -> [c,a,b] is
        # indistinguishable from "every slot's content changed". With a move
        # event the identities are unchanged and only the positions move, so a
        # reconcile_tree keyed on the part id rewrites order, not rows.
        msgs = [
            _msg({"rows": []}),
            _patch_msg(AddPart("rows", 0, "a", {"n": 1})),
            _patch_msg(AddPart("rows", 1, "b", {"n": 2})),
            _patch_msg(AddPart("rows", 2, "c", {"n": 3})),
        ]
        before = parts_of(fold_snapshot(msgs), "rows")
        after = parts_of(
            fold_snapshot([*msgs, _patch_msg(MovePart("rows", "c", 0))]), "rows"
        )
        assert {i for i, _, _ in before} == {i for i, _, _ in after}
        assert [i for i, _, _ in after] == ["c", "a", "b"]
        assert {i: r for i, _, r in before} == {i: r for i, _, r in after}

    def test_reconcile_tree_over_folded_parts_keys_by_part_id(self):
        from rakaia.projections import reconcile_tree

        state = fold_snapshot(
            [
                _msg({"rows": []}),
                _patch_msg(AddPart("rows", 0, "a", {"n": 1})),
                _patch_msg(AddPart("rows", 1, "b", {"n": 2})),
            ]
        )
        effects = reconcile_tree(
            "app.Row",
            {"submission_id": 5},
            "part_id",
            [row for _, _, row in parts_of(state, "rows")],
            id_fn=lambda r: r[PART_ID_KEY],
            defaults_fn=lambda r: {"n": r["n"]},
        )
        assert effects[0].lookup == {"submission_id": 5, "part_id": "a"}
        assert effects[1].lookup == {"submission_id": 5, "part_id": "b"}


# =============================================================================
# 6. Size — the reason this exists at all
# =============================================================================


class TestPayloadSize:
    def test_a_one_field_edit_of_a_large_repeater_is_orders_of_magnitude_smaller(self):
        rows = [
            {
                PART_ID_KEY: f"p{i}",
                "project": f"proj-{i}",
                "output": i,
                "note": "x" * 40,
            }
            for i in range(60)
        ]
        snapshot = {"suku": "Aileu", "rows": rows}
        full = len(json.dumps(snapshot).encode())
        patch = len(
            json.dumps(encode_patch([SetField(("rows", "p30", "output"), 99)])).encode()
        )
        assert patch * 20 < full
