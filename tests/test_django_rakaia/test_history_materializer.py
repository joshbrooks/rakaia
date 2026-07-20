"""Tests for django_rakaia.materialize_history — the read+build+apply convenience.

The same read -> history_effects -> DjangoExecutor().apply dance as
test_history_materialization.py, but through the one-call helper.
"""

from __future__ import annotations

import pytest

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.history import materialize_history
from rakaia.history import envelope_actor, label_marker
from rakaia.types import AppendOptions

from .models import History


def _defaults(msg, event):
    return {
        "marker": label_marker(msg.label),
        "actor": envelope_actor(msg, event),
        "ts": msg.timestamp,
        "snapshot": event,
    }


def _seed() -> DjangoStreamStore:
    store = DjangoStreamStore()
    store.create("submissions")
    store.append(
        "submissions",
        b'{"key": "s1", "user_id": 9, "a": 1}',
        AppendOptions(label="insert", metadata={"user": 42}),
    )
    store.append(
        "submissions",
        b'{"key": "s1", "user_id": 9, "a": 2}',
        AppendOptions(label="update"),  # no context -> actor falls back to owner
    )
    return store


def _materialize(store, **kw):
    return materialize_history(
        store,
        "submissions",
        "test_django_rakaia.History",
        subject_field="submission_id",
        subject_of=lambda ev: ev["key"],
        defaults_of=_defaults,
        **kw,
    )


@pytest.mark.django_db
class TestMaterializeHistory:
    def test_reads_builds_and_applies_in_one_call(self):
        effects = _materialize(_seed())
        assert len(effects) == 2
        rows = list(History.objects.order_by("version"))
        assert [r.marker for r in rows] == ["+", "~"]
        assert [r.actor for r in rows] == [42, 9]  # context editor, then owner
        assert rows[1].snapshot == {"key": "s1", "user_id": 9, "a": 2}

    def test_idempotent(self):
        store = _seed()
        _materialize(store)
        _materialize(store)  # keyed by (subject, version) -> no duplicates
        assert History.objects.count() == 2

    def test_version_of_keys_on_offset(self):
        _materialize(_seed(), version_of=lambda m: int(m.offset))
        versions = sorted(History.objects.values_list("version", flat=True))
        assert versions == [1, 2]  # the durable offsets, not the 0/1 list index

    def test_custom_executor_is_used(self):
        applied: list = []

        class Recorder:
            def apply(self, effects):
                applied.extend(effects)

        _materialize(_seed(), executor=Recorder())
        assert len(applied) == 2
        assert History.objects.count() == 0  # recorder didn't write to the ORM
