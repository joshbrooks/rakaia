"""
Django implementation of the rakaia.effects.Executor protocol.

Applies update_or_create Effects to the Django ORM in a single atomic
transaction. External effects passed to apply() are silently dropped —
the replay orchestrator decides whether external effects reach the
executor based on its include_external setting.
"""

from __future__ import annotations

from collections.abc import Iterable

from django.apps import apps
from django.db import transaction

from rakaia.effects import Effect, check_disjoint_defaults


class DjangoExecutor:
    """Apply Effects via Django's ORM."""

    def apply(self, effects: Iterable[Effect]) -> None:
        effects_list = list(effects)
        check_disjoint_defaults(effects_list)
        with transaction.atomic():
            for eff in effects_list:
                if eff.op != "update_or_create":
                    continue
                if eff.model_label is None or eff.lookup is None:
                    raise ValueError(
                        "update_or_create effect requires model_label and lookup"
                    )
                model = apps.get_model(eff.model_label)
                model.objects.update_or_create(
                    **eff.lookup,
                    defaults=eff.defaults or {},
                )
