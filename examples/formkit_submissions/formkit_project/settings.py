"""Minimal Django settings for the Rakaia FormKit-submissions prototype."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Stream store backend: "memory" (default, process-local) or "durable"
# (DB-backed via DjangoStreamStore — survives across processes). Set with
# RAKAIA_STORE=durable to emit in one process and replay in another.
RAKAIA_STORE = os.environ.get("RAKAIA_STORE", "memory")

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
    "submissions",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "formkit_project.urls"
ASGI_APPLICATION = "formkit_project.asgi.application"

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

# django_rakaia's AppConfig.ready() imports channels_signals, so the channels
# app must be installed. The in-memory layer is plenty for this single-process
# demo — this prototype never actually broadcasts.
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
