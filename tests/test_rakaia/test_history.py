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


def _msg(payload: dict, *, label: str = "", metadata: dict | None = None):
    return StreamMessage(
        data=json.dumps(payload).encode("utf-8"),
        offset="0",
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
