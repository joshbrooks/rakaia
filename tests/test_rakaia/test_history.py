"""Tests for rakaia.history: the audit-log materializer + recovery."""

from __future__ import annotations

import json

from rakaia.history import (
    envelope_actor,
    history_effects,
    label_marker,
    recover_peak_snapshot,
)
from rakaia.types import StreamMessage


def _msg(
    payload: dict, *, label: str = "", metadata: dict | None = None, offset: str = "0"
):
    return StreamMessage(
        data=json.dumps(payload).encode("utf-8"),
        offset=offset,
        timestamp=1.0,
        label=label,
        metadata=metadata,
    )


class TestLabelMarker:
    def test_mapping(self):
        assert label_marker("insert") == "+"
        assert label_marker("create") == "+"
        assert label_marker("delete") == "-"
        assert label_marker("update") == "~"
        assert label_marker("") == "~"  # raw append → update marker


class TestEnvelopeActor:
    def test_prefers_context_editor(self):
        msg = _msg({"user_id": 7}, metadata={"user": 42})
        assert envelope_actor(msg, {"user_id": 7}) == 42  # editor wins

    def test_falls_back_to_payload_owner(self):
        msg = _msg({"user_id": 7})  # no metadata → no context actor
        assert envelope_actor(msg, {"user_id": 7}) == 7  # owner FK

    def test_none_when_neither_present(self):
        msg = _msg({})
        assert envelope_actor(msg, {}) is None

    def test_explicit_anonymous_stamp_falls_back_to_owner(self):
        # An anonymous request stamps metadata={"user": None}; that must fall
        # back to the payload owner, not report None — matching pghistory's
        # `if uid is None: uid = h.user_id`.
        msg = _msg({"user_id": 7}, metadata={"user": None, "url": "/x"})
        assert envelope_actor(msg, {"user_id": 7}) == 7


class TestHistoryEffects:
    def _effects(self, messages):
        return history_effects(
            messages,
            "app.SubmissionHistory",
            subject_of=lambda ev: ev["key"],
            defaults_of=lambda msg, ev: {
                "marker": label_marker(msg.label),
                "actor": envelope_actor(msg, ev),
                "ts": msg.timestamp,
                "snapshot": ev,
            },
            subject_field="submission_id",
            version_field="version",
        )

    def test_one_row_per_event_keyed_by_subject_and_version(self):
        messages = [
            _msg({"key": "s1", "n": 1}, label="insert", metadata={"user": 3}),
            _msg({"key": "s1", "n": 2}, label="update", metadata={"user": 3}),
        ]
        effects = self._effects(messages)
        assert [e.op for e in effects] == ["update_or_create", "update_or_create"]
        assert effects[0].lookup == {"submission_id": "s1", "version": 0}
        assert effects[1].lookup == {"submission_id": "s1", "version": 1}
        assert effects[0].defaults["marker"] == "+"
        assert effects[1].defaults["marker"] == "~"
        assert effects[0].defaults["actor"] == 3
        assert effects[1].defaults["snapshot"] == {"key": "s1", "n": 2}

    def test_actor_fallback_when_no_context(self):
        messages = [_msg({"key": "s1", "user_id": 9})]  # no metadata
        effects = self._effects(messages)
        assert effects[0].defaults["actor"] == 9  # payload owner

    def test_empty_stream_yields_no_effects(self):
        assert self._effects([]) == []

    def test_version_of_derives_stable_version(self):
        # With version_of, the version is a stable per-event id (the offset), so
        # materializing a tail alone doesn't restart at 0 and collide.
        messages = [
            _msg({"key": "s1", "n": 1}, offset="10"),
            _msg({"key": "s1", "n": 2}, offset="20"),
        ]
        # The stable per-event key is the opaque offset token itself — not
        # int(m.offset), which the offset contract forbids (formats differ by
        # store).
        effects = history_effects(
            messages,
            "app.H",
            subject_of=lambda ev: ev["key"],
            defaults_of=lambda _m, ev: {"snapshot": ev},
            version_of=lambda m: m.offset,
        )
        assert [e.lookup["version"] for e in effects] == ["10", "20"]

        # A later tail read of just event #3 keeps its own stable version.
        tail = [_msg({"key": "s1", "n": 3}, offset="30")]
        tail_effects = history_effects(
            tail,
            "app.H",
            subject_of=lambda ev: ev["key"],
            defaults_of=lambda _m, ev: {"snapshot": ev},
            version_of=lambda m: m.offset,
        )
        assert tail_effects[0].lookup["version"] == "30"  # not 0 → no collision


class TestRecoverPeakSnapshot:
    def test_recovers_peak_over_a_truncating_save(self):
        messages = [
            _msg({"key": "s1", "a": 1, "b": 2, "c": 3}),  # 4 keys (peak)
            _msg({"key": "s1"}),  # blank save → 1 key
            _msg({"key": "s2", "x": 1}),  # a different subject
        ]
        peak = recover_peak_snapshot(messages, "s1", subject_of=lambda ev: ev["key"])
        assert peak == {"key": "s1", "a": 1, "b": 2, "c": 3}

    def test_missing_subject_returns_empty(self):
        assert recover_peak_snapshot([], "s1", subject_of=lambda ev: ev["key"]) == {}

    def test_newest_wins_on_a_tie(self):
        # Two equal-size snapshots (3 keys each) — recovery must restore the
        # NEWER one, matching pghistory's ORDER BY created_at DESC.
        messages = [
            _msg({"key": "s1", "a": 1, "b": 2}),  # older, 3 keys
            _msg({"key": "s1", "a": 9, "b": 9}),  # newer, 3 keys
        ]
        peak = recover_peak_snapshot(messages, "s1", subject_of=lambda ev: ev["key"])
        assert peak == {"key": "s1", "a": 9, "b": 9}  # newest
