"""Production-style settings for the Rakaia polyglot sample.

Run with:

    just polyglot-serve              # multi-worker hypercorn + Redis
    just polyglot-serve workers=8    # override worker count
"""

import os

from .settings import *  # noqa: F403

DEBUG = False

SECRET_KEY = os.environ.get(
    "DJANGO_SECRET_KEY",
    "insecure-default-please-set-DJANGO_SECRET_KEY-in-production",
)

ALLOWED_HOSTS = [
    h.strip()
    for h in os.environ.get("DJANGO_ALLOWED_HOSTS", "localhost,127.0.0.1").split(",")
    if h.strip()
]

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [REDIS_URL],
        },
    },
}

STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
    },
}
