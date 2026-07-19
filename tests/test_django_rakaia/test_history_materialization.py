"""End-to-end: enveloped durable stream -> history read-model over the ORM."""

from __future__ import annotations

import pytest

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.effect_executor import DjangoExecutor
from rakaia.history import envelope_actor, history_effects, label_marker
from rakaia.types import AppendOptions

from .models import History


def _defaults(msg, event):
    return {
        "marker": label_marker(msg.label),
        "actor": envelope_actor(msg, event),
        "ts": msg.timestamp,
        "snapshot": event,
    }


@pytest.mark.django_db
class TestHistoryMaterialization:
    def test_materializes_from_durable_enveloped_stream(self):
        store = DjangoStreamStore()
        store.create("submissions")
        # insert by editor 42 (request-context actor)
        store.append(
            "submissions",
            b'{"key": "s1", "user_id": 9, "a": 1}',
            AppendOptions(label="insert", metadata={"user": 42}),
        )
        # update with NO context -> actor falls back to the payload owner (9)
        store.append(
            "submissions",
            b'{"key": "s1", "user_id": 9, "a": 2}',
            AppendOptions(label="update"),
        )

        messages, _ = store.read("submissions")
        effects = history_effects(
            messages,
            "test_django_rakaia.History",
            subject_of=lambda ev: ev["key"],
            defaults_of=_defaults,
            subject_field="submission_id",
            version_field="version",
        )
        DjangoExecutor().apply(effects)

        rows = list(History.objects.order_by("version"))
        assert len(rows) == 2
        assert rows[0].marker == "+" and rows[0].actor == 42  # context editor
        assert rows[1].marker == "~" and rows[1].actor == 9  # fallback to owner
        assert rows[1].snapshot == {"key": "s1", "user_id": 9, "a": 2}

    def test_rematerialize_is_idempotent(self):
        store = DjangoStreamStore()
        store.create("submissions")
        store.append(
            "submissions",
            b'{"key": "s1", "a": 1}',
            AppendOptions(label="insert", metadata={"user": 1}),
        )
        messages, _ = store.read("submissions")
        effects = history_effects(
            messages,
            "test_django_rakaia.History",
            subject_of=lambda ev: ev["key"],
            defaults_of=_defaults,
            subject_field="submission_id",
            version_field="version",
        )
        DjangoExecutor().apply(effects)
        DjangoExecutor().apply(effects)  # again — keyed by (subject, version)
        assert History.objects.count() == 1
