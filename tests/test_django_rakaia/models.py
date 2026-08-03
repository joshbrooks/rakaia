"""
Test models for django_rakaia integration tests.

This module contains the canonical example models: Area, Project,
and demonstrates the normalized Stream/StreamEvent/StreamEntry model.

User events are handled via Django's built-in User model (django.contrib.auth)
using the create_stream_event helper function.

This demonstrates multi-stream events:
- User events appear in "user:{id}:projects" stream
- Area events appear in "area:{id}:projects" stream
- Project events appear in BOTH "user:{created_by_id}:projects" AND "area:{area_id}:projects" streams

NOTE: `Area` and `Project` are `@stream_model` — saving one fires a `post_save`
that appends a StreamEvent to the `default` database. For isolation / query-count
/ hermeticity tests (anything asserting "no writes/reads to `default`" or counting
UPDATEs), use a PLAIN model instead: `FinanceLine`, `Measure`, `Balance`,
`SukuProjection`, or `History`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from django.contrib.auth.models import User
from django.db import models
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from django_rakaia.admin import register_stream_event_admin
from django_rakaia.decorators import create_stream_event, stream_model
from django_rakaia.models import StreamEvent


@dataclass
class UserData:
    """Dataclass for streaming User model changes."""

    id: int
    username: str
    email: str
    first_name: str
    last_name: str


@dataclass
class AreaData:
    """Dataclass for streaming Area model changes."""

    id: int
    name: str


@dataclass
class ProjectData:
    """Dataclass for streaming Project model changes."""

    id: int
    name: str
    area_id: int
    created_by_id: int


class AppStreamEvent(StreamEvent):
    """
    Concrete stream event model for the test application.

    This extends StreamEvent for the test app. Events can appear
    in multiple streams via the StreamEntry junction table.
    """

    class Meta:
        app_label = "test_django_rakaia"
        db_table = "test_app_streamevent"


def to_user_data(instance: User) -> UserData:
    """Convert User model to UserData dataclass."""
    return UserData(
        id=instance.id,
        username=instance.get_username(),
        email=instance.email,
        first_name=instance.first_name,
        last_name=instance.last_name,
    )  # type: ignore[attr-defined]


# Signal handlers for Django's built-in User model
@receiver(post_save, sender="auth.User", weak=False)
def handle_user_post_save(
    sender: type[models.Model],  # noqa: ARG001
    instance: models.Model,
    created: bool,
    signal: Any,  # noqa: ARG001
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """Handle User post_save signal - single stream."""
    action = "create" if created else "update"
    create_stream_event(
        stream_paths=f"user:{instance.id}:projects",
        to_dataclass=to_user_data,
        instance=instance,
        action=action,
    )


@receiver(post_delete, sender="auth.User", weak=False)
def handle_user_post_delete(
    sender: type[models.Model],  # noqa: ARG001
    instance: models.Model,
    signal: Any,  # noqa: ARG001
    **kwargs: Any,  # noqa: ARG001
) -> None:
    """Handle User post_delete signal - single stream."""
    create_stream_event(
        stream_paths=f"user:{instance.id}:projects",
        to_dataclass=to_user_data,
        instance=instance,
        action="delete",
    )


@stream_model(
    stream_paths=lambda instance: f"area:{instance.id}:projects",
    to_dataclass=lambda instance: AreaData(id=instance.id, name=instance.name),
)
class Area(models.Model):
    """
    Area model for testing.

    Areas contain projects and are used to organize stream subscriptions.
    Area events appear in the "area:{id}:projects" stream.
    """

    name = models.CharField(max_length=100)

    class Meta:
        app_label = "test_django_rakaia"

    def __str__(self) -> str:
        return str(self.name or "")


def get_project_stream_paths(instance: Project) -> list[str]:
    """
    Get stream paths for a project.

    Projects appear in TWO streams:
    1. The creator's user stream: "user:{created_by_id}:projects"
    2. The area's stream: "area:{area_id}:projects"

    This demonstrates multi-stream events.
    """
    return [
        f"user:{instance.created_by_id}:projects",
        f"area:{instance.area_id}:projects",
    ]


@stream_model(
    stream_paths=get_project_stream_paths,
    to_dataclass=lambda instance: ProjectData(
        id=instance.id,
        name=instance.name,
        area_id=instance.area_id,
        created_by_id=instance.created_by_id,
    ),
)
class Project(models.Model):
    """
    Project model for testing.

    Projects belong to an area and are created by a user.
    Project events appear in BOTH the user's stream AND the area's stream,
    demonstrating the multi-stream capability of the normalized model.
    """

    name = models.CharField(max_length=100)
    area = models.ForeignKey(Area, on_delete=models.CASCADE, related_name="projects")
    created_by = models.ForeignKey(
        "auth.User",
        on_delete=models.CASCADE,
        related_name="created_projects",
    )

    class Meta:
        app_label = "test_django_rakaia"

    def __str__(self) -> str:
        return str(self.name or "")

    @property
    def area_id(self) -> int:
        """Get the area ID."""
        return self.area_id

    @property
    def created_by_id(self) -> int:
        """Get the creator's user ID."""
        return self.created_by_id


class Alert(models.Model):
    """Quality-flag / alert projection row — the rakaia analogue of
    FormKit-Ninja's ``Flag`` on a ``SeparatedSubmission``.

    **Natural key** = ``(stream_key, alert_type, field_key)`` — never an
    ordinal. ``field_key`` defaults to ``""`` (not null) so the composite key
    stays a stable unique constraint for whole-entity alerts.

    **Soft-delete**: open ⟺ ``resolved_at IS NULL``. A resolved row is retained,
    not deleted, so the audit trail survives. ``resolved_at`` holds the
    *triggering event's* timestamp (an ISO string), never ``timezone.now()`` —
    that is what keeps replay deterministic.

    One model serves both alert layers:

    * **authored** (``alert_raised`` / ``alert_dismissed`` events) — plain
      natural-key ``update_or_create`` upserts; never reconciled.
    * **machine-reconciled** — written by ``reconcile_by_key`` (upsert current
      violations, ``retire`` the absent ones), scoped to machine ``alert_type``s.
    """

    stream_key = models.CharField(max_length=64)
    """The entity the alert is about (e.g. a submission UUID)."""

    alert_type = models.CharField(max_length=64)
    """Rule / marker id, e.g. ``ff4_operational_exceeds_ksp`` or ``alert``."""

    field_key = models.CharField(max_length=64, default="")
    """Which field the alert points at; ``""`` = whole-entity."""

    severity = models.CharField(max_length=16, default="info")
    """``error`` | ``warning`` | ``info``."""

    message = models.TextField(default="")

    resolved_at = models.CharField(max_length=32, null=True, default=None)
    """Event-timestamp string when retired; ``NULL`` ⟺ open."""

    resolved_by = models.CharField(max_length=64, null=True, default=None)

    dismissed_version = models.IntegerField(null=True, default=None)
    """Data version an authored dismissal was made against (Phase 3). A standing
    dismissal suppresses a user-resolvable violation while
    ``dismissed_version >= the current violation's version``; a newer edit
    supersedes it. ``NULL`` = no standing dismissal."""

    created_at = models.CharField(max_length=32, default="")

    class Meta:
        app_label = "test_django_rakaia"
        ordering = ["stream_key", "alert_type", "field_key"]
        unique_together = ["stream_key", "alert_type", "field_key"]

    @property
    def is_open(self) -> bool:
        return self.resolved_at is None

    def __str__(self) -> str:
        state = "open" if self.resolved_at is None else "resolved"
        return f"{self.alert_type}[{self.field_key or '*'}] ({self.severity}, {state})"


class FinanceLine(models.Model):
    """Plain contributing-rows model for the reducer/aggregate tests."""

    submission_id = models.CharField(max_length=64, unique=True)
    suku = models.CharField(max_length=64)
    delta = models.IntegerField(default=0)

    class Meta:
        app_label = "test_django_rakaia"


class Balance(models.Model):
    """Plain aggregate model for the reducer/aggregate tests."""

    suku = models.CharField(max_length=64, unique=True)
    total = models.IntegerField(default=0)

    class Meta:
        app_label = "test_django_rakaia"


class SukuProjection(models.Model):
    """Multi-owned aggregate row for the `reconcile_aggregate(owns=...)` tests.

    One row per ``suku`` whose columns are written by two independent reducers:
    a status reducer owns ``status``, a finance reducer owns ``ksp_total``. When
    one owner's group vanishes, its columns must be null-cleared without
    disturbing the other owner's — the multi-owner reconcile invariant (#67)."""

    suku = models.CharField(max_length=64, unique=True)
    status = models.CharField(max_length=16, null=True, default=None)
    ksp_total = models.IntegerField(null=True, default=None)

    class Meta:
        app_label = "test_django_rakaia"


class Measure(models.Model):
    """Projection row with a UUID natural key and a Decimal column — the
    fixture for the ``diff_effects_against_rows`` verification helper, whose
    default normalizers cover exactly these two "log carries a different
    representation than the column" cases (UUID<->str, over-precise Decimal)."""

    ref = models.UUIDField(unique=True)
    amount = models.DecimalField(max_digits=10, decimal_places=2, default=0)

    class Meta:
        app_label = "test_django_rakaia"


class History(models.Model):
    """Audit-log read-model for the history materializer tests."""

    submission_id = models.CharField(max_length=64)
    version = models.IntegerField()
    marker = models.CharField(max_length=1)
    actor = models.IntegerField(null=True, blank=True)
    ts = models.FloatField(default=0)
    snapshot = models.JSONField(default=dict)

    class Meta:
        app_label = "test_django_rakaia"
        unique_together = ["submission_id", "version"]


# Register the admin interface for AppStreamEvent
register_stream_event_admin(AppStreamEvent)
