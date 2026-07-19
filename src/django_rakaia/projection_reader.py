"""
Django implementation of rakaia's `ProjectionReader`.

`replay()` passes a reader to every stage > 0 handler as its second argument
during staged replay, so the handler can resolve facts that earlier stages
committed. This reader is a thin, read-only accessor over the Django ORM:

    from rakaia.replay import replay
    from django_rakaia.effect_executor import DjangoExecutor
    from django_rakaia.projection_reader import DjangoProjectionReader

    replay(store, path, DjangoExecutor(), reader=DjangoProjectionReader())

Because it only ever reads committed projections (themselves a pure function of
the log), a handler using it stays deterministic across replays.
"""

from __future__ import annotations

from typing import Any

from django.apps import apps
from django.db.models import QuerySet


class DjangoProjectionReader:
    """Read-only projection accessor over `apps.get_model(...).objects`."""

    def get(self, model_label: str, /, **lookup: Any) -> Any | None:
        """The single row matching `lookup`, or None (never raises on absence)."""
        return apps.get_model(model_label).objects.filter(**lookup).first()

    def filter(self, model_label: str, /, **lookup: Any) -> QuerySet:
        """A queryset of the rows matching `lookup`."""
        return apps.get_model(model_label).objects.filter(**lookup)

    def query(self, model_label: str, /) -> QuerySet:
        """A queryset of every row of the model."""
        return apps.get_model(model_label).objects.all()
