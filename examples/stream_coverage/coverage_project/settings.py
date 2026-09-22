"""Minimal Django settings for the Rakaia stream coverage example.

``RAKAIA_COVERAGE_CHECKS`` is the point of it: one entry saying that the
``survey.Submission`` table feeds the ``submissions`` stream, that each event
names its row under the ``submission`` key, and that ``updated_at`` is when a row
last changed.
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
    "survey",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    },
}

# The events live in the database, beside the table they are written from,
# which is what the coverage check reads.
RAKAIA_STORE = "durable"

RAKAIA_COVERAGE_CHECKS = [
    {
        "model": "survey.Submission",
        "stream_path": "submissions",
        "subject_key": "submission",  # each event carries {"submission": <row id>}
        "changed_field": "updated_at",
    },
]

# django_rakaia's AppConfig.ready() imports channels_signals, so channels must
# be installed. The in-memory layer is plenty for this single-process demo.
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}

USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
