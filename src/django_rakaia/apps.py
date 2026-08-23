from django.apps import AppConfig


class DjangoRakaiaConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_rakaia"
    verbose_name = "Rakaia Streams"

    def ready(self) -> None:
        # Registering the checks is what runs them: importing the module binds
        # them to Django's registry via @register(). Kept first and dependency
        # -free so `manage.py check` reports a bad RAKAIA_STORE even when the
        # rest of `ready()` would fail for the same reason.
        from django_rakaia import checks  # noqa: F401

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

        The gate itself (issue #41 — keep `channels` optional for framework-tier
        use) lives in `sse_gate.sse_import`, which is also what `urls.py` asks
        before adding the SSE route. It was written out here and nowhere else,
        which is how the URL file came to import the SSE view unconditionally
        and make `channels` a hard dependency of the polling API (#230).
        """
        from .sse_gate import sse_import

        sse_import("django_rakaia.channels_signals")
