"""
Django-style autodiscovery for rakaia versioned handlers and upcasters.

On `DjangoRakaiaConfig.ready()`, this module walks INSTALLED_APPS and
imports any `<app>/handlers.py` and `<app>/upcasters.py` modules. Importing
those modules triggers their `@register_handler` / `@register_upcaster`
decorators, populating the default rakaia registries.
"""

from __future__ import annotations

from django.utils.module_loading import autodiscover_modules


def autodiscover() -> None:
    """Import every installed app's `handlers.py` and `upcasters.py`."""
    autodiscover_modules("handlers")
    autodiscover_modules("upcasters")
