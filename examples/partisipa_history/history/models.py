"""Projections for the pghistory-retirement spike.

Three tables, two of them derived from the stream and one modelling the status
quo:

* ``SubmissionRecord`` — the current-state projection. This is the rakaia
  replacement for Partisipa's ``Submission`` table: one live row per submission,
  rebuilt by replaying the stream.
* ``SubmissionHistoryEntry`` — the audit read-model, also derived from the
  stream. One row per event, carrying the actor / label / timestamp the
  ``/history`` API serves. This is what makes ``django-pghistory`` redundant.
* ``PghEventGolden`` — a faithful stand-in for django-pghistory's ``pgh_event``
  table (populated by the "today" path). The spike asserts the stream-derived
  ``SubmissionHistoryEntry`` reproduces it byte-for-byte.
"""

from django.db import models


class SubmissionRecord(models.Model):
    """Current-state projection — the rakaia replacement for `Submission`."""

    submission_id = models.CharField(max_length=64, unique=True)
    fields = models.JSONField(default=dict)
    actor = models.CharField(max_length=128, default="")
    updated_at = models.CharField(max_length=32, default="")

    class Meta:
        ordering = ["submission_id"]

    def __str__(self) -> str:
        return f"{self.submission_id} @ {self.updated_at}"


class SubmissionHistoryEntry(models.Model):
    """Stream-derived audit log — reproduces django-pghistory's `pgh_event`.

    ``seq`` is the event's position in the stream, so re-replay is idempotent
    (the (submission_id, seq) key is stable). ``label`` is the `/history`
    diff marker: ``+`` create, ``~`` update, ``-`` delete.
    """

    submission_id = models.CharField(max_length=64)
    seq = models.IntegerField()
    label = models.CharField(max_length=1)
    actor = models.CharField(max_length=128, default="")
    ts = models.CharField(max_length=32, default="")
    fields = models.JSONField(default=dict)

    class Meta:
        ordering = ["submission_id", "seq"]
        unique_together = ["submission_id", "seq"]

    def __str__(self) -> str:
        return f"{self.submission_id}#{self.seq} {self.label} by {self.actor}"


class PghEventGolden(models.Model):
    """A faithful model of django-pghistory's `pgh_event` audit table.

    Populated by the "today" path (`pghistory_today.simulate`) — one row per
    ``Submission.save()``, exactly as ``@pghistory.track()`` + the
    ``HistoryMiddleware`` actor context would write. Not a live pghistory
    instance; a golden reference the stream must reproduce.
    """

    pgh_id = models.AutoField(primary_key=True)
    submission_id = models.CharField(max_length=64)
    pgh_label = models.CharField(max_length=16)  # insert | update | delete
    pgh_context_user = models.CharField(max_length=128, default="")
    pgh_created_at = models.CharField(max_length=32, default="")
    fields = models.JSONField(default=dict)

    class Meta:
        ordering = ["pgh_id"]

    def __str__(self) -> str:
        return f"pgh#{self.pgh_id} {self.submission_id} {self.pgh_label}"
