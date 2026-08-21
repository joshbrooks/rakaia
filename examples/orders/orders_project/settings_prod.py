"""Production-style settings for the Rakaia orders sample.

Differences from `settings.py`:

* `DEBUG = False`
* `SECRET_KEY` and `ALLOWED_HOSTS` come from environment variables.

Environment variables:

* `DJANGO_SECRET_KEY` (required)
* `DJANGO_ALLOWED_HOSTS` (comma-separated, default: "localhost,127.0.0.1")
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
