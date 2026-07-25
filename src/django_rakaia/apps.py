from django.apps import AppConfig
from django.conf import settings


class DjangoRakaiaConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_rakaia"
    verbose_name = "Rakaia Streams"

    def ready(self) -> None:
        # Framework tier: always wire handler/upcaster autodiscovery. This path
        # has no `channels` dependency, so it must load for a projections-only
        # consumer that never touches the protocol server (ADR-0002 / #41).
        from django_rakaia.handlers_autodiscover import autodiscover

        autodiscover()

        # Protocol tier: SSE broadcasting via Django Channels. Wired only when
        # wanted, so `channels`/`daphne` stay an optional extra for framework
        # consumers (they are in `[project.optional-dependencies]`).
        self._wire_sse_signals()

    def _wire_sse_signals(self) -> None:
        """Import the Channels SSE signal handlers unless opted out / absent.

        Gate (issue #41 — keep `channels` optional for framework-tier use):

        * ``RAKAIA_ENABLE_SSE = False`` — never wire.
        * ``RAKAIA_ENABLE_SSE = True`` — wire; if `channels` is missing, let the
          ``ImportError`` propagate (the consumer asked for SSE but didn't
          install the extra — a real misconfiguration, surfaced loudly).
        * unset — auto-detect: wire if `channels` imports, skip silently if not
          (a framework-only consumer that never installed the extra).
        """
        enabled = getattr(settings, "RAKAIA_ENABLE_SSE", None)
        if enabled is False:
            return
        try:
            import django_rakaia.channels_signals  # noqa: F401
        except ImportError:
            if enabled:
                raise
