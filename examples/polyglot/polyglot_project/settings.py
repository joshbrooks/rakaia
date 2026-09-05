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
            # fsync on every commit, which capped this demo at ~40 saves/s
            # regardless of how fast anything else was. The same work measured
            # ~370/s with the settings below. The event append has since moved
            # out of the database entirely (RAKAIA_STORE below), so only the
            # `Translatable` row `UPDATE` is left here — but that is still one
            # commit per save, and these settings are still what makes it cheap.
            "OPTIONS": {
                "init_command": ("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"),
                # Without this, a second concurrent writer fails *immediately*
                # with "database is locked" rather than waiting out `timeout`: a
                # deferred transaction that starts by reading cannot upgrade to
                # a write lock while another writer holds one, and SQLite does
                # not retry an upgrade. Taking the write lock up front makes the
                # wait honour `timeout` instead of raising. The read that used
                # to trigger this was the append locking the offset high-water;
                # `stress_translations` still reads before it writes.
                "transaction_mode": "IMMEDIATE",
                "timeout": 15,
            },
        },
    }

# The event log lives in a folder of ordinary JSON-lines files rather than in the
# database. Nothing here needs a migration, and the whole log can be read with
# `less` and diffed in git.
#
# The consequence is that live delivery no longer goes through the channel
# layer: only the durable store broadcasts an append over channels, and the
# file-backed one cannot — it lives in the framework-independent package and has
# no way to reach Django. So SSE is served instead by rakaia's own protocol
# server, mounted on /protocol/ in asgi.py over this same directory, which tails
# the log through the filesystem. That works between processes on one machine,
# which `InMemoryChannelLayer` never did.
#
# `rakaia.W002` is the check that warns about exactly this, and it is silenced
# because the demo has answered it.
RAKAIA_STORE = "jsonl"
RAKAIA_JSONL_ROOT = os.environ.get("POLYGLOT_STREAM_ROOT", BASE_DIR / "streams")

# Every append waits for the disk by default, which is what makes a returned
# save mean the event survives a power cut. Set POLYGLOT_STREAM_FSYNC=0 to turn
# that off — worth pairing with a memory-backed POLYGLOT_STREAM_ROOT such as
# /dev/shm/polyglot, where there is no disk to wait for and the events still
# outlive the process that wrote them.
RAKAIA_JSONL_FSYNC = os.environ.get("POLYGLOT_STREAM_FSYNC", "1") != "0"
SILENCED_SYSTEM_CHECKS = ["rakaia.W002"]

# Where asgi.py mounts the protocol server, and what landing.html builds its
# EventSource URL from. A setting rather than a constant in asgi.py, because
# importing asgi.py from a view would run `get_asgi_application()` a second time
# as a side effect of rendering a page.
POLYGLOT_PROTOCOL_PREFIX = "/protocol"

# Still an in-memory layer, and still only used by anything the demo wires to
# channels itself — the translation stream no longer rides on it.
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
