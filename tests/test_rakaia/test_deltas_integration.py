"""What a delta event costs the rest of rakaia.

The `test_deltas.py` cases pin what deltas *do*. These pin what they *break* —
four places that quietly assume every event is a full snapshot. Each is written
as an executable statement of the hazard rather than a paragraph in a design doc,
so the cost of adopting deltas is measured rather than asserted, and so a future
change that removes a hazard fails a test here instead of going unnoticed.

None of these are bugs in the code under test. They are the price of the payload
shape, and the point of writing them down is that the price is payable — each has
a named remedy, exercised alongside the hazard.
"""

from __future__ import annotations

import json

import pytest

from rakaia.deltas import (
    PATCH_LABEL,
    AddPart,
    SetField,
    apply_delta,
    encode_patch,
    fold_snapshot,
    is_patch,
)
from rakaia.history import history_effects, label_marker
from rakaia.projections import project_latest
from rakaia.registry import HandlerRegistry, UpcasterRegistry
from rakaia.types import StreamMessage


def _msg(payload: dict, *, label: str = "update", offset: str = "1") -> StreamMessage:
    return StreamMessage(
        data=json.dumps(payload).encode("utf-8"),
        offset=offset,
        timestamp=1.0,
        label=label,
    )


def _patch_msg(*ops, offset: str = "1", subject: str | None = None) -> StreamMessage:
    payload = encode_patch(ops)
    if subject is not None:
        payload = {"subject": subject, **payload}
    return _msg(payload, label=PATCH_LABEL, offset=offset)


# =============================================================================
# Hazard 1 — content routing reads a field the patch does not carry
# =============================================================================


class TestContentRoutingMissesAPatch:
    """`match_field` routing tests a glob against ``str(event[match_field])``.

    A snapshot carries ``form_type`` because it carries everything. A patch
    carries only what changed, so the routing field is in the *base*, not the
    event — and the handler that would have projected it simply does not fire.
    Silently: an unmatched content-routed event is normal (a different form_type
    on the same stream), so nothing raises.
    """

    def _registry(self) -> HandlerRegistry:
        reg = HandlerRegistry()
        reg.register(
            name="tf611",
            event_match="TF_6_1_1",
            fn=lambda event: [],  # noqa: ARG005
            effective_from=0,
            match_field="form_type",
        )
        return reg

    def test_a_snapshot_routes(self):
        reg = self._registry()
        assert reg.resolve("ignored", 0, {"form_type": "TF_6_1_1", "fields": {}})

    def test_a_bare_patch_does_not_route(self):
        reg = self._registry()
        patch = encode_patch([SetField(("fields", "output"), 4)])
        assert not reg.resolve("ignored", 0, patch)

    def test_the_remedy_is_to_carry_the_routing_fields_on_the_patch(self):
        # A patch is "only what changed" plus whatever the *transport* needs to
        # deliver it. Routing fields are the latter, so they ride along — the
        # payload stays small because the repeater does not, not because the
        # discriminator was dropped.
        reg = self._registry()
        patch = {
            "form_type": "TF_6_1_1",
            **encode_patch([SetField(("fields", "output"), 4)]),
        }
        assert reg.resolve("ignored", 0, patch)
        assert is_patch(patch)


# =============================================================================
# Hazard 2 — an upcaster rewrites a snapshot's fields, not a patch's paths
# =============================================================================


class TestUpcastersDoNotSeeInsideAPath:
    """A rename upcaster is ``event -> event``. Against a snapshot it moves a
    key. Against a patch the key it is looking for is not a key at all — it is a
    *segment of a path string* — so the upcaster passes the patch through
    untouched and the fold writes the pre-rename name.

    This is the hazard with the longest fuse: it only bites on the day a schema
    version lands, by which time the un-upcast patches are already durable.
    """

    def _registry(self) -> UpcasterRegistry:
        reg = UpcasterRegistry()

        def rename(event: dict) -> dict:
            out = dict(event)
            if "beneficiaries" in out:
                out["beneficiary_count"] = out.pop("beneficiaries")
            return out

        reg.register(event_match="subs/*", from_version=1, fn=rename)
        return reg

    def test_a_snapshot_is_renamed(self):
        upcast = self._registry().upcast_to_current({"beneficiaries": 120}, "subs/x")
        assert upcast == {"beneficiary_count": 120, "schema_version": 2}

    def test_a_patch_path_is_left_at_the_old_name(self):
        patch = encode_patch([SetField(("beneficiaries",), 120)])
        upcast = self._registry().upcast_to_current(patch, "subs/x")
        assert upcast["ops"][0]["path"] == "/beneficiaries"

    def test_so_the_fold_writes_the_pre_rename_key(self):
        upcast = self._registry().upcast_to_current(
            encode_patch([SetField(("beneficiaries",), 120)]), "subs/x"
        )
        folded = fold_snapshot(
            [_msg({"beneficiary_count": 1}), _msg(upcast, label=PATCH_LABEL)]
        )
        assert folded is not None
        assert "beneficiaries" in folded  # the old name, resurrected
        assert folded["beneficiary_count"] == 1  # the rename did not reach it

    def test_the_remedy_is_a_path_aware_upcaster(self):
        # Writable, but it is a *second* upcaster body per rename, and nothing in
        # the registry makes forgetting it visible. That asymmetry is the finding.
        def rename_paths(event: dict) -> dict:
            if not is_patch(event):
                return event
            ops = [
                {
                    **op,
                    "path": op["path"].replace("/beneficiaries", "/beneficiary_count"),
                }
                if "path" in op
                else op
                for op in event["ops"]
            ]
            return {**event, "ops": ops}

        patched = rename_paths(encode_patch([SetField(("beneficiaries",), 120)]))
        assert patched["ops"][0]["path"] == "/beneficiary_count"


# =============================================================================
# Hazard 3 — the latest-state projection folds a patch as if it were a snapshot
# =============================================================================


class TestProjectLatestTreatsAPatchAsAWholeState:
    """`project_latest`'s contract is "every event is a full snapshot, so latest
    needs no reducer". Handed a patch it does exactly what it says: the newest
    event wins and becomes the row. The row is then the *diff*, not the state,
    and no exception is raised on the way there.
    """

    def _messages(self):
        return [
            _msg({"subject": "s1", "suku": "Aileu", "output": 3}, offset="1"),
            _patch_msg(SetField(("output",), 4), offset="2", subject="s1"),
        ]

    def test_the_row_becomes_the_diff(self):
        effects = project_latest(
            self._messages(),
            "app.Sub",
            subject_of=lambda e: e["subject"],
            defaults_of=lambda _m, e: {k: v for k, v in e.items() if k != "subject"},
        )
        assert len(effects) == 1
        assert "ops" in (effects[0].defaults or {})
        assert "suku" not in (effects[0].defaults or {})

    def test_the_remedy_is_to_fold_before_projecting(self):
        state = fold_snapshot(self._messages())
        assert state == {"subject": "s1", "suku": "Aileu", "output": 4}


# =============================================================================
# Hazard 4 — the audit row stops being a self-contained snapshot
# =============================================================================


class TestHistoryRowsAreNoLongerSelfContained:
    """`history_effects` writes one row per event carrying the payload snapshot.
    For a patch that payload is the ops list, so "what did this look like at
    version N" stops being answerable by reading row N — you have to re-fold from
    the last snapshot row. The peak-snapshot recovery (most fields wins) is worse
    off still: a patch always has fewer fields than a snapshot, so it can never
    be the peak, which is accidentally the right answer for the wrong reason.
    """

    def _effects(self):
        msgs = [
            _msg({"subject": "s1", "a": 1, "b": 2}, offset="1"),
            _patch_msg(SetField(("b",), 9), offset="2", subject="s1"),
        ]
        return msgs, history_effects(
            msgs,
            "app.Audit",
            subject_of=lambda e: e["subject"],
            defaults_of=lambda m, e: {"label": m.label, "snapshot": e},
            version_of=lambda m: m.offset,
        )

    def test_one_row_per_event_still(self):
        _msgs, effects = self._effects()
        assert [e.lookup["version"] for e in effects] == ["1", "2"]

    def test_the_second_row_holds_ops_not_a_state(self):
        _msgs, effects = self._effects()
        assert "ops" in (effects[1].defaults or {})["snapshot"]

    def test_so_reading_row_n_alone_no_longer_answers_what_it_looked_like(self):
        msgs, effects = self._effects()
        row = (effects[1].defaults or {})["snapshot"]
        assert row.get("a") is None  # the value at version 2 is not in the row
        assert fold_snapshot(msgs) == {
            "subject": "s1",
            "a": 1,
            "b": 9,
        }  # only a re-fold has it

    def test_the_patch_label_needs_a_marker_decision(self):
        # `label_marker` maps anything unrecognised to `~`, which happens to be
        # right for a partial update and wrong for a patch whose only op adds or
        # removes a part. Not broken — undecided, and the decision belongs with
        # whoever owns the /history rendering.
        assert label_marker(PATCH_LABEL) == "~"


# =============================================================================
# Hazard 5 — random access: a tail read has to find its base
# =============================================================================


class TestRandomAccessCosts:
    """Under full snapshots any single message is a complete state, so a reader
    can resume from any offset. Under deltas it cannot: the window must reach
    back to the last snapshot, or be handed the state it is resuming from.

    Both routes are exercised here because the choice between them *is* the
    adoption decision — periodic snapshots bound the read, an explicit base
    couples the reader to a projection row.
    """

    def _stream(self):
        return [
            _msg({"a": 1, "rows": []}, offset="1"),
            _patch_msg(AddPart("rows", 0, "p1", {"n": 1}), offset="2"),
            _patch_msg(AddPart("rows", 1, "p2", {"n": 2}), offset="3"),
            _patch_msg(SetField(("a",), 5), offset="4"),
        ]

    def test_a_full_window_folds(self):
        state = fold_snapshot(self._stream())
        assert state is not None
        assert state["a"] == 5
        assert len(state["rows"]) == 2

    def test_a_tail_window_refuses_rather_than_guessing(self):
        from rakaia.deltas import NoBaseSnapshotError

        with pytest.raises(NoBaseSnapshotError):
            fold_snapshot(self._stream()[2:])

    def test_route_a_periodic_snapshot_bounds_the_read(self):
        stream = self._stream()
        compacted = [_msg(fold_snapshot(stream) or {}, offset="5")]
        assert fold_snapshot(compacted) == fold_snapshot(stream)

    def test_route_b_an_explicit_base_from_the_projection_row(self):
        stream = self._stream()
        base = fold_snapshot(stream[:2])
        assert fold_snapshot(stream[2:], base=base) == fold_snapshot(stream)


# =============================================================================
# Hazard 6 — a delta is not idempotent, so it cannot be an Effect
# =============================================================================


class TestDeltasAreNotEffects:
    """Every rakaia Effect converges on re-application: that is what makes replay
    safe. A delta does not — adding a part twice is two parts — which is why this
    module produces *state*, and the state produces Effects, rather than deltas
    being a sixth Effect applied straight to a row.
    """

    def test_re_applying_a_snapshot_upsert_converges(self):
        state = {"rows": []}
        once = apply_delta(state, AddPart("rows", 0, "p1", {"n": 1}))
        assert fold_snapshot([_msg(once), _msg(once)]) == once

    def test_re_applying_an_add_does_not(self):
        from rakaia.deltas import DeltaConflictError

        state = apply_delta({"rows": []}, AddPart("rows", 0, "p1", {"n": 1}))
        # It raises rather than duplicating — the loud version of non-idempotent,
        # which is the whole reason the conflict errors exist.
        with pytest.raises(DeltaConflictError):
            apply_delta(state, AddPart("rows", 0, "p1", {"n": 1}))

    def test_but_re_folding_the_same_window_converges(self):
        # Idempotency lives at the *fold*, not the op: replaying the window is
        # safe, replaying one op is not. So a projection is rebuilt by re-folding
        # and re-upserting, never by re-applying a delta to a row.
        stream = [
            _msg({"rows": []}, offset="1"),
            _patch_msg(AddPart("rows", 0, "p1", {"n": 1}), offset="2"),
        ]
        assert fold_snapshot(stream) == fold_snapshot(stream)
