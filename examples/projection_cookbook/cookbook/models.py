"""Projection read-models for the cookbook: a two-form staged projection.

A ``Project`` is the reference entity (created by a ``PROJECT`` form); a ``Task``
is the dependent entity (created by a ``TASK`` form) that links to its Project by
the ``code`` natural key. The link is resolved during a *later* replay stage, so
a task that arrives before its project still binds — no reactive re-save, no
backfill task.
"""

from __future__ import annotations

from django.db import models


class Project(models.Model):
    """Reference entity. Natural key: ``code``."""

    code = models.CharField(max_length=32, unique=True)
    name = models.CharField(max_length=200, default="")

    class Meta:
        ordering = ["code"]

    def __str__(self) -> str:
        return f"{self.code} ({self.name})"


class Task(models.Model):
    """Dependent entity. Natural key: ``task_id``; links to a Project by code."""

    task_id = models.CharField(max_length=32, unique=True)
    title = models.CharField(max_length=200, default="")
    project = models.ForeignKey(
        Project,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="tasks",
    )

    class Meta:
        ordering = ["task_id"]

    def __str__(self) -> str:
        return f"{self.task_id}: {self.title}"
