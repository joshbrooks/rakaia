"""Tests for the converged-design spike (RFC #22, Decision #13).

Run: `uv run --extra django python manage.py test submission_stream`.

These cover the properties the demo command asserts, plus the ones a demo can't
show cleanly: transactional atomicity (Decision #11), delete/tombstone
projection (Decision #2), and idempotency. `TransactionTestCase` is used because
the write path opens its own `transaction.atomic()` and one test forces a
rollback.

Out of scope on sqlite: the Postgres coverage guard (Decision #10). Its tests
(raw write raises, `SET CONSTRAINTS ALL IMMEDIATE`, bulk + index, sanctioned
bypass) need a Postgres backend and belong with the guard itself.
"""

from __future__ import annotations

import contextlib
import json
from unittest import mock

from django.test import TransactionTestCase

from submission_stream import stream
from submission_stream.models import Submission, SubmissionHistory

A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


class SubmissionStreamTests(TransactionTestCase):
    def setUp(self) -> None:
        self.store = stream.get_store()
        with contextlib.suppress(KeyError):
            self.store.delete(stream.STREAM)
        self.store.create(stream.STREAM)

    def _record(self, key: str, **kw: object) -> None:
        stream.record_submission(self.store, key, **kw)  # type: ignore[arg-type]

    def _projection(self) -> dict[str, tuple]:
        return {
            s.key: (json.dumps(s.fields, sort_keys=True), s.status, s.user, s.version)
            for s in Submission.objects.all()
        }

    def test_latest_event_wins_across_interleaving(self) -> None:
        self._record(A, fields={"v": 1}, status=0, actor="x")
        self._record(B, fields={"v": 1}, status=0, actor="y")
        self._record(A, fields={"v": 2}, status=1, actor="z")
        a = Submission.objects.get(key=A)
        self.assertEqual(a.fields["v"], 2)
        self.assertEqual(a.status, 1)
        self.assertEqual(a.user, "z")
        # B is untouched by A's later events.
        self.assertEqual(Submission.objects.get(key=B).fields["v"], 1)

    def test_append_and_project_are_atomic(self) -> None:
        # If projection fails, the append must roll back with it — the event and
        # the change commit together or not at all (Decision #11).
        with (
            mock.patch.object(stream, "reproject_all", side_effect=RuntimeError()),
            self.assertRaises(RuntimeError),
        ):
            self._record(A, fields={"v": 1}, status=0, actor="x")
        messages, _ = self.store.read(stream.STREAM)
        self.assertEqual(len(messages), 0)  # no orphaned event
        self.assertFalse(Submission.objects.filter(key=A).exists())

    def test_delete_label_tombstones_the_projection(self) -> None:
        self._record(A, fields={"v": 1}, status=0, actor="x", label="create")
        self.assertTrue(Submission.objects.filter(key=A).exists())
        self._record(A, fields={"v": 1}, status=0, actor="x", label="delete")
        # The row is gone from the projection...
        self.assertFalse(Submission.objects.filter(key=A).exists())
        # ...but the log/history still records the create and the delete.
        stream.materialize_history(self.store)
        markers = list(
            SubmissionHistory.objects.filter(key=A)
            .order_by("version")
            .values_list("marker", flat=True)
        )
        self.assertEqual(markers, ["+", "-"])

    def test_reproject_is_idempotent_and_content_correct(self) -> None:
        # Multiple keys + a tombstone, then compare the FULL projection (fields,
        # status, user, version) across a second reproject — not just a row
        # count, which the unique key makes 0/1 by construction. This catches a
        # reproject that yields a *wrong* row or forgets the tombstone.
        self._record(A, fields={"v": 1}, status=0, actor="x")
        self._record(B, fields={"v": 1}, status=0, actor="y")
        self._record(A, fields={"v": 2}, status=1, actor="z")
        self._record(B, fields={"v": 1}, status=0, actor="y", label="delete")
        first = self._projection()
        stream.reproject_all(self.store)
        self.assertEqual(self._projection(), first)  # content stable, not just count
        # ...and correct: A at its latest event, B tombstoned away.
        self.assertEqual(first[A][:3], ('{"v": 2}', 1, "z"))
        self.assertNotIn(B, first)

    def test_history_materialize_is_idempotent(self) -> None:
        self._record(A, fields={"v": 1}, status=0, actor="x")
        self._record(A, fields={"v": 2}, status=1, actor="z")
        stream.materialize_history(self.store)
        first = SubmissionHistory.objects.count()
        stream.materialize_history(self.store)
        self.assertEqual(SubmissionHistory.objects.count(), first)

    def test_context_less_write_logs_full_event_with_null_actor(self) -> None:
        # Mode B: no request context -> a full event still, only actor/url null.
        self._record(A, fields={"v": 1}, status=2, label="import")
        stream.materialize_history(self.store)
        h = SubmissionHistory.objects.get(key=A)
        self.assertIsNone(h.actor)
        self.assertIsNone(h.url)
        self.assertEqual(h.snapshot["status"], 2)  # snapshot intact
        self.assertIsNone(Submission.objects.get(key=A).user)

    def test_provenance_captured_on_append_not_signal(self) -> None:
        self._record(A, fields={"v": 1}, status=1, actor="amaral", url="/submit/A")
        stream.materialize_history(self.store)
        h = SubmissionHistory.objects.get(key=A)
        self.assertEqual(h.actor, "amaral")
        self.assertEqual(h.url, "/submit/A")
