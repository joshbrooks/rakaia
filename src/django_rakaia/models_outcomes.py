"""SPIKE — the outcome table. Field limits deliberately mirror `ConsumerCursor`."""

from __future__ import annotations

from django.db import models


class EventOutcome(models.Model):
    consumer = models.CharField(max_length=128)
    stream_path = models.CharField(max_length=255)
    subject = models.CharField(max_length=255)
    offset = models.CharField(max_length=64, null=True)
    sequence_key = models.CharField(max_length=255)
    stage = models.CharField(max_length=16)
    status = models.CharField(max_length=16)
    reasons = models.JSONField(default=list)
    params = models.JSONField(default=dict)
    attempt = models.PositiveSmallIntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "rakaia_eventoutcome"
        unique_together = [["consumer", "stream_path", "subject", "attempt"]]

    def __str__(self) -> str:
        return f"{self.consumer}@{self.stream_path}/{self.subject}={self.status}"
