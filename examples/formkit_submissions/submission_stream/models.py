"""The two tables of the converged design (RFC #22, Decision #13).

`SubmissionEvent` — the append-only source of truth — is rakaia's durable
`StreamEvent`/`StreamEntry` log (written via `DjangoStreamStore`), so it isn't
redefined here. This module holds the two *projections* derived from it:

* `Submission` — the **latest version** of each submission. A pure function of
  the event log; never written directly, always rebuilt by `reproject_all`.
* `SubmissionHistory` — the `/history` audit view: one row per event,
  materialised from the same log by `rakaia.history_effects`.

The arrow points `SubmissionEvent ──project──▶ Submission`, the reverse of
pghistory (`Submission ──trigger──▶ SubmissionEvent` shadow).
"""

from django.db import models


class Submission(models.Model):
    """Projection holding the LATEST snapshot of each submission.

    `version` is the stream position of the event this row was projected from.
    Because every event is a full snapshot (Decision #5), projecting is just
    "take the newest event for this key" — no reducer.
    """

    key = models.CharField(max_length=64, unique=True)
    fields = models.JSONField(default=dict)
    status = models.IntegerField(default=0)
    # Actor of the latest event (envelope metadata['user']); may be null for a
    # context-less write (import) — the graceful "mode B" case.
    user = models.CharField(max_length=64, null=True, blank=True)
    version = models.IntegerField(default=0)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["key"]

    def __str__(self) -> str:
        return f"{self.key[:8]} v{self.version} status={self.status} user={self.user}"


class SubmissionHistory(models.Model):
    """`/history` audit row — one per SubmissionEvent, keyed by (key, version).

    `version` is the global stream position, never renumbered (Decision #7), so
    re-materialising is idempotent.
    """

    key = models.CharField(max_length=64)
    version = models.IntegerField()
    marker = models.CharField(max_length=1, default="~")
    actor = models.CharField(max_length=64, null=True, blank=True)
    url = models.CharField(max_length=200, null=True, blank=True)
    status = models.IntegerField(default=0)
    snapshot = models.JSONField(default=dict)
    ts = models.FloatField(default=0.0)

    class Meta:
        ordering = ["key", "version"]
        unique_together = ["key", "version"]

    def __str__(self) -> str:
        return f"{self.key[:8]} v{self.version} {self.marker} by {self.actor}"
