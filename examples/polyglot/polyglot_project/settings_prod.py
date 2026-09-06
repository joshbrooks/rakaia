"""Production-style settings for the Rakaia polyglot sample.

Run with:

    just polyglot-serve              # multi-worker hypercorn, no broker
    just polyglot-serve workers=8    # override worker count

There is no channel layer here on purpose. This demo's log is a folder of files
and its SSE comes from the protocol server mounted in `asgi.py`, so the workers
share state through the filesystem rather than through a broker. `settings.py`
leaves an in-memory channel layer configured for `channels` itself; nothing in
this app sends on it.
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

STORAGES = {
    "default": {
        "BACKEND": "django.core.files.storage.FileSystemStorage",
    },
    "staticfiles": {
        "BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage",
    },
}
