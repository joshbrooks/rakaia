import os

# Which database the suite runs against. Unset (or anything other than
# "postgres") keeps the historical in-memory SQLite behaviour, so a plain
# `pytest` is unchanged. `RAKAIA_TEST_DB=postgres` points the same suite at a
# real Postgres, which is the only way the `select_for_update()` calls in
# `models.py`, `django_store.py` and `effect_executor.py` do anything at all:
# Django's SQL compiler emits `FOR UPDATE` only when
# `connection.features.has_select_for_update` is true, and the SQLite backend
# leaves it false, so on SQLite every one of those calls is a silent no-op and
# the "must be inside a transaction" TransactionManagementError never fires.
#
# Django settings reference for DATABASES / TEST:
#   https://docs.djangoproject.com/en/6.0/ref/settings/#databases
#   https://docs.djangoproject.com/en/6.0/ref/settings/#test
# pytest-django creates and (with --reuse-db) reuses the test databases named
# by DATABASES[alias]["TEST"]["NAME"]:
#   https://pytest-django.readthedocs.io/en/latest/database.html
RAKAIA_TEST_DB = os.environ.get("RAKAIA_TEST_DB", "sqlite")


def _postgres(test_name: str) -> dict[str, object]:
    """One Postgres alias, configured entirely from the environment.

    The connection parameters use the standard libpq environment variable
    names so the same settings module works against a GitHub Actions
    `services:` container, a local podman container, or a developer's own
    server without editing anything.
    """
    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("PGDATABASE", "postgres"),
        "USER": os.environ.get("PGUSER", "postgres"),
        "PASSWORD": os.environ.get("PGPASSWORD", "postgres"),
        "HOST": os.environ.get("PGHOST", "127.0.0.1"),
        "PORT": os.environ.get("PGPORT", "5432"),
        # Each alias needs its own test database. Without an explicit TEST
        # NAME both aliases would derive `test_postgres` from the same NAME
        # and the second would clobber the first.
        "TEST": {"NAME": test_name},
    }


if RAKAIA_TEST_DB == "postgres":
    DATABASES = {
        "default": _postgres("test_rakaia"),
        # See the SQLite branch below for what `overlay` is for.
        "overlay": _postgres("test_rakaia_overlay"),
    }
else:
    DATABASES = {
        "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
        # A second in-memory alias used by the `using=` seam tests — a disposable
        # database a from-scratch rebuild can be replayed into without touching
        # `default` (see test_using_seam.py; #68 item 2).
        "overlay": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
    }
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
    "tests.test_django_rakaia.apps.TestDjangoRakaiaConfig",
    "django_extensions",
]
USE_TZ = True
SECRET_KEY = "dummy"
ROOT_URLCONF = "tests.test_django_rakaia.urls"
ASGI_APPLICATION = "tests.test_django_rakaia.asgi.application"
STATIC_URL = "/static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]
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
