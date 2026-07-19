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

    def test_incremental_tail_materialization_via_version_of(self):
        # Materialize the full stream, then append + materialize only the TAIL.
        # With version_of=int(offset) the tail keeps its stable version instead
        # of restarting at 0 and overwriting an earlier row.
        store = DjangoStreamStore()
        store.create("submissions")
        store.append(
            "submissions", b'{"key": "s1", "n": 1}', AppendOptions(label="insert")
        )
        store.append(
            "submissions", b'{"key": "s1", "n": 2}', AppendOptions(label="update")
        )

        def materialize(messages):
            DjangoExecutor().apply(
                history_effects(
                    messages,
                    "test_django_rakaia.History",
                    subject_of=lambda ev: ev["key"],
                    defaults_of=_defaults,
                    subject_field="submission_id",
                    version_field="version",
                    version_of=lambda m: int(m.offset),
                )
            )

        full, _ = store.read("submissions")
        materialize(full)
        cursor = store.get_current_offset("submissions")

        store.append(
            "submissions", b'{"key": "s1", "n": 3}', AppendOptions(label="update")
        )
        tail, _ = store.read("submissions", offset=cursor)
        assert len(tail) == 1  # only the new event
        materialize(tail)

        versions = sorted(
            History.objects.filter(submission_id="s1").values_list("version", flat=True)
        )
        assert versions == [1, 2, 3]  # tail didn't collide/overwrite version 1
        assert History.objects.get(submission_id="s1", version=3).snapshot["n"] == 3

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
