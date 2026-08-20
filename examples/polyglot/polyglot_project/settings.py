"""Minimal Django settings for the Rakaia polyglot sample app."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "insecure-sample-key-do-not-use-in-production"
DEBUG = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "daphne",
    "channels",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_rakaia",
    "polyglot",
]

import importlib.util  # noqa: E402

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    # WhiteNoise ships in the `prod` extra and serves /static/ under
    # `just serve`/hypercorn. It is inserted only when installed so that
    # `just dev` (dev extras only) works — runserver serves static itself.
    *(
        ["whitenoise.middleware.WhiteNoiseMiddleware"]
        if importlib.util.find_spec("whitenoise")
        else []
    ),
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "polyglot_project.urls"
ASGI_APPLICATION = "polyglot_project.asgi.application"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

# SQLite is the default so the demo runs from a bare checkout. Set
# POLYGLOT_DB=postgres (with the same PG* variables `just test-pg` uses) to
# point it at Postgres instead — the interesting difference is under
# `stress_translations --concurrency`, where SQLite's single-writer lock is a
# hard ceiling and Postgres's is not.
if os.environ.get("POLYGLOT_DB") == "postgres":
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.environ.get("PGDATABASE", "postgres"),
            "USER": os.environ.get("PGUSER", "postgres"),
            "PASSWORD": os.environ.get("PGPASSWORD", "postgres"),
            "HOST": os.environ.get("PGHOST", "127.0.0.1"),
            "PORT": os.environ.get("PGPORT", "55432"),
        },
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
            # SQLite's defaults (`journal_mode=delete`, `synchronous=FULL`)
            # fsync twice per save here — once for the row UPDATE, once for the
            # event append — which capped this demo at ~40 events/s regardless
            # of how fast anything else was. The same work measured ~370/s with
            # the settings below.
            "OPTIONS": {
                "init_command": ("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"),
                # Without this, a second concurrent writer fails *immediately*
                # with "database is locked" rather than waiting out `timeout`: a
                # deferred transaction that starts by reading (which every
                # append does — it locks the offset high-water) cannot upgrade
                # to a write lock while another writer holds one, and SQLite
                # does not retry an upgrade. Taking the write lock up front
                # makes the wait honour `timeout` instead of raising.
                "transaction_mode": "IMMEDIATE",
                "timeout": 15,
            },
        },
    }

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
