"""The projection the two workers write, and the duplicate it cannot make.

One row per event a worker applied. ``offset`` is unique on purpose: if two
workers under one consumer name ever apply the same event, the second write
fails loudly instead of quietly double-counting — the projection is what the
demo asserts on, so it has to be the thing that would notice.
"""

from django.db import models


class AppliedEvent(models.Model):
    consumer = models.CharField(max_length=64)
    offset = models.CharField(max_length=64, unique=True)
    worker = models.CharField(max_length=16)
    key = models.CharField(max_length=64)

    class Meta:
        ordering = ["offset"]

    def __str__(self) -> str:
        return f"{self.key} by {self.worker} at {self.offset}"
