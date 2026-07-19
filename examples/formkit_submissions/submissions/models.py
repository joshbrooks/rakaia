"""Materialized projections written by replaying the `submissions` stream.

These two models are the rakaia analogue of a FormKit-Ninja *typed* form model
(the concrete table `SeparatedSubmission.to_model()` populates). They are
*derived* state — never edited directly. The submission event stream is the
source of truth; replay re-derives every row via idempotent `update_or_create`,
keyed on the stable submission UUID (mirroring FormKit-Ninja's `submission`
OneToOneField anchor).

* `MonitoringVisit` — one row per submission (the flattened *root* node), plus
  fields *derived* from its activities by the versioned `visit_summary`
  handler.
* `ActivityProgress` — one row per repeater child (FormKit-Ninja's
  `repeater_parent` rows), written by the `activity_rows` fan-out handler and
  keyed by `(submission_id, activity_index)`.
"""

from django.db import models


class MonitoringVisit(models.Model):
    """Root projection row — one per submission UUID."""

    # The stable submission UUID. In FormKit-Ninja this is the PK of
    # SeparatedSubmission and the anchor for to_model()'s update_or_create.
    submission_id = models.CharField(max_length=64, unique=True)
    form_type = models.CharField(max_length=64, default="")

    # Copied verbatim from the submission's root `fields` (Stage 3 hydration).
    project_code = models.CharField(max_length=32, default="")
    suku = models.CharField(max_length=64, default="")
    monitor = models.CharField(max_length=64, default="")
    visit_date = models.CharField(max_length=32, default="")

    # Derived by the `visit_summary` handler from the activity repeater.
    total_budget = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    overall_progress = models.DecimalField(max_digits=5, decimal_places=2, default=0)

    # `status` is where the v1 -> v2 policy split shows up: v1 marks COMPLETE at
    # >= 100% progress; v2 (a later tolerance policy) at >= 90%. Old visits keep
    # the strict rule — the time-correctness guarantee.
    status = models.CharField(max_length=16, default="")

    class Meta:
        ordering = ["submission_id"]

    def __str__(self) -> str:
        return f"{self.project_code} @ {self.suku} ({self.overall_progress}% {self.status})"


class ActivityProgress(models.Model):
    """One repeater child per activity, keyed by (submission_id, index)."""

    submission_id = models.CharField(max_length=64)
    activity_index = models.IntegerField()

    name = models.CharField(max_length=128, default="")
    budget = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    progress_pct = models.IntegerField(default=0)

    class Meta:
        ordering = ["submission_id", "activity_index"]
        unique_together = ["submission_id", "activity_index"]

    def __str__(self) -> str:
        return f"{self.name} ({self.progress_pct}%)"


class SubmissionHistory(models.Model):
    """Streams-native `/history` audit row — the pghistory replacement.

    Unlike `MonitoringVisit`/`ActivityProgress` (which hold only the *latest*
    derived state), this is one row per recorded change, materialised from the
    event **envelope** carried on each stream message. It is the same shape a
    `django-pghistory` `/history` endpoint returns: a per-version marker
    (``+``/``~``/``-``), the acting user captured at write time, a timestamp, and
    the full payload snapshot. Written by `rakaia.history_effects` in the
    `demo_submissions` command, keyed by the stable submission UUID + the
    stream version so re-materialising is idempotent.
    """

    submission_id = models.CharField(max_length=64)
    version = models.IntegerField()

    # Envelope-derived audit columns.
    marker = models.CharField(max_length=1, default="~")
    actor = models.CharField(max_length=64, null=True, blank=True)
    label = models.CharField(max_length=32, default="")
    ts = models.FloatField(default=0.0)
    snapshot = models.JSONField(default=dict)

    class Meta:
        ordering = ["submission_id", "version"]
        unique_together = ["submission_id", "version"]

    def __str__(self) -> str:
        return f"{self.submission_id[:8]} v{self.version} {self.marker} by {self.actor}"
