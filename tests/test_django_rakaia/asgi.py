import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.test_django_rakaia.settings")

application = get_asgi_application()
