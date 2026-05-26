from django.apps import AppConfig


class DjangoRakaiaConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_rakaia"
    verbose_name = "Rakaia Streams"

    def ready(self) -> None:
        import django_rakaia.channels_signals  # noqa: F401
        from django_rakaia.handlers_autodiscover import autodiscover

        autodiscover()
