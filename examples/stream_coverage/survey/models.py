"""One table, and the stream written from it.

Every ``save()`` of a `Submission` appends one event to the ``submissions``
stream, carrying the row's id under ``submission``. That is the producer the
coverage check holds the table against. Anything that writes rows without calling
``save()`` — ``bulk_create``, ``QuerySet.update``, raw SQL — writes no event, and
the demo uses exactly those to open a gap.
"""

from __future__ import annotations

from dataclasses import dataclass

from django.db import models

from django_rakaia import stream_model

STREAM = "submissions"


@dataclass
class SubmissionData:
    submission: int
    name: str


@stream_model(
    stream_paths=STREAM,
    to_dataclass=lambda obj: SubmissionData(submission=obj.pk, name=obj.name),
)
class Submission(models.Model):
    name = models.CharField(max_length=100)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["pk"]

    def __str__(self) -> str:
        return self.name
