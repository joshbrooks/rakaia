from django.apps import AppConfig


class PolyglotConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "polyglot"

    def ready(self) -> None:
        from . import signals  # noqa: F401
