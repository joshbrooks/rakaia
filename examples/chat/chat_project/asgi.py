"""ASGI entry point for the chat sample."""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "chat_project.settings")

application = get_asgi_application()
