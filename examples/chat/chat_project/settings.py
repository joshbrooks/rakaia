"""Minimal Django settings for the Rakaia chat sample app."""

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
    "chat",
]

import importlib.util  # noqa: E402

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    # WhiteNoise must come right after SecurityMiddleware so static-file
    # requests bypass the rest of the stack. With DEBUG=True the runserver
    # staticfiles handler still takes precedence; with DEBUG=False (e.g.
    # under `just serve`/hypercorn) WhiteNoise is what serves /static/.
    #
    # It ships in the `prod` extra, so it is inserted only when installed:
    # `just dev` (dev extras only) serves static via the runserver handler and
    # does not need it, while `just install` / `just serve` pull it in.
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

ROOT_URLCONF = "chat_project.urls"
ASGI_APPLICATION = "chat_project.asgi.application"

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

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    },
}

# In-memory channel layer is enough for the single-process sample.
# For production, swap to channels_redis (see docs/deployment.md).
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
