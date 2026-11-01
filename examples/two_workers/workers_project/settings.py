"""Minimal Django settings for the two-workers example.

``RAKAIA_STORE = "durable"`` because the whole point is a cursor row two
*processes* can contend for: a log in memory would give each process its own.

``RAKAIA_TEST_DB=postgres`` points the demo at a real Postgres, where
``select_for_update()`` emits ``FOR UPDATE``. Left alone it uses a file-backed
SQLite, where that call compiles to a plain SELECT and the cursor row lock
therefore does not exist — which is exactly what the demo prints and skips.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "insecure-sample-key-do-not-use-in-production"
DEBUG = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "daphne",
    "channels",
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_rakaia",
    "workers",
]

ROOT_URLCONF = "workers_project.urls"

if os.environ.get("RAKAIA_TEST_DB") == "postgres":
    # The same libpq variable names the test suite's settings use, so the demo
    # runs against a CI service container, `just test-pg`'s podman container, or
    # a developer's own server without an edit.
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.environ.get("PGDATABASE", "postgres"),
            "USER": os.environ.get("PGUSER", "postgres"),
            "PASSWORD": os.environ.get("PGPASSWORD", "postgres"),
            "HOST": os.environ.get("PGHOST", "127.0.0.1"),
            "PORT": os.environ.get("PGPORT", "5432"),
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
        },
    }

# The log lives in the database, beside the cursor and the applied rows.
RAKAIA_STORE = "durable"

# django_rakaia's AppConfig.ready() imports channels_signals, so channels must
# be installed. The in-memory layer is plenty for a demo with no server.
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}

USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
