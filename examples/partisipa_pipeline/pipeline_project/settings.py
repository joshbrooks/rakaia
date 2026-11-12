"""Django settings for the pipeline example.

Two databases. ``default`` is live: the log and the tables a save keeps up to
date. ``scratch`` is where a rebuild replays the log so it can be compared with
live, and it is in memory on purpose — a rebuild needs an empty database, and one
that disappears when the process ends is empty every time.
"""

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
    "pipeline",
]

ROOT_URLCONF = "pipeline_project.urls"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    },
    "scratch": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    },
}

# The log lives in the live database, which is what a rebuild reads it from.
RAKAIA_STORE = "durable"

# django_rakaia's AppConfig.ready() imports channels_signals, so channels must
# be installed. The in-memory layer is plenty for this single-process demo.
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}

USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
