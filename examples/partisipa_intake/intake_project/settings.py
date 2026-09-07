"""Minimal Django settings for the Rakaia consuming-loop example.

``RAKAIA_STORE = "durable"`` on purpose: the point of this example is what
survives a restart — the reading position and the failure records — and a log
kept in memory would take the events with it when the process ends.
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
    "intake",
]

ROOT_URLCONF = "intake_project.urls"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    },
}

# The log lives in the database, beside the cursor and the outcomes.
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
