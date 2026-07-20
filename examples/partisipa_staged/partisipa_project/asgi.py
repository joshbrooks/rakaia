"""ASGI entry point for the staged-replay spike."""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "partisipa_project.settings")

application = get_asgi_application()
