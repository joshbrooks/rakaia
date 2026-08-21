"""Production-style settings for the Rakaia chat sample.

Differences from `settings.py`:

* `DEBUG = False`
* `SECRET_KEY`, `ALLOWED_HOSTS`, and `REDIS_URL` come from environment variables.
* `CHANNEL_LAYERS` uses `channels-redis` so SSE broadcasts work across multiple
  worker processes.

Run with:

    just serve              # multi-worker hypercorn + Redis
    just serve workers=8    # override worker count

Environment variables:

* `DJANGO_SECRET_KEY` (required)
* `DJANGO_ALLOWED_HOSTS` (comma-separated, default: "localhost,127.0.0.1")
* `REDIS_URL` (default: "redis://localhost:6379/0")
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

# Static files: WhiteNoise serves /static/ from STATIC_ROOT (set in
# settings.py) and the Compressed-Manifest backend gives every file a
# content-hashed name plus a gzip/brotli sibling for far-future caching.
# Run `python manage.py collectstatic --noinput` before booting hypercorn
# (the `just serve` recipe does this for you).
STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
    },
}

# SQLite is fine for the chat *demo*, but for any real workload swap this
# out for Postgres / MySQL. SQLite serializes writes, which becomes a
# bottleneck under multi-worker write contention.
