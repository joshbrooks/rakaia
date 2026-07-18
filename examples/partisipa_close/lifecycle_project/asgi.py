"""ASGI entry point for the close-precondition spike."""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "lifecycle_project.settings")

application = get_asgi_application()
