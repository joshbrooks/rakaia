from django.apps import AppConfig


class DjangoRakaiaConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "django_rakaia"
    verbose_name = "Rakaia Streams"

    def ready(self) -> None:
        pass
