"""ASGI entry point (present for parity with the other samples; unused by the
command-line demo)."""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "cookbook_project.settings")

application = get_asgi_application()
