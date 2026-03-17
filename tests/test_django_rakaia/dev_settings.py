"""
Development settings for django_rakaia.

Uses a file-based SQLite database so data persists between commands.
"""

from pathlib import Path

# Build paths inside the project
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Import test settings as base FIRST
from tests.test_django_rakaia.settings import *  # noqa: F401, F403, E402

# Override for development
DEBUG = True
ALLOWED_HOSTS = ["localhost", "127.0.0.1", "0.0.0.0"]

# Use a file-based database
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}
