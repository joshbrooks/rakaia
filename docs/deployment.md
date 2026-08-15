# Deployment

This guide covers running Rakaia in development and in a production-style
setup with Redis and multiple worker processes.

## TL;DR — `just`

If you have [`just`](https://github.com/casey/just) and `podman` installed,
the entire workflow is:

```bash
just install            # uv sync --extra dev --extra django --extra docs --extra prod
just dev                # single-worker dev server (in-memory channel layer)

# Production-style local run:
just serve              # 4 hypercorn workers + Redis (podman) on 0.0.0.0:8000
just serve 8            # 8 workers (parameters are positional)
just serve 2 127.0.0.1 9000   # 2 workers, custom host/port
just smoke              # curl the running server
just redis-down         # stop the Redis container
just nuke               # stop Redis + clean caches and DB
```

`just --list` shows everything available. The rest of this page explains
what those recipes actually do, in case you want to run the commands by
hand or wire them into your own deployment.

## Standalone Rakaia protocol server

The core `rakaia` package is a plain ASGI application with zero runtime
dependencies. Run it under any ASGI server:

```bash
uv run uvicorn rakaia:app --host 0.0.0.0 --port 4437
# or
uv run hypercorn --bind 0.0.0.0:4437 --workers 4 rakaia:app
```

There's a shortcut recipe too:

```bash
just rakaia              # uvicorn rakaia:app on port 4437
just rakaia port=9999    # custom port
```

By default Rakaia uses an in-memory `StreamStore`. To plug in a custom
store, build the app yourself:

```python
# wsgi_or_asgi.py
from rakaia import StreamStore, create_app

store = StreamStore()           # or your own implementation
app = create_app(store=store)
```

Note that an in-memory store is **not** safe across worker processes — if
you run with `--workers > 1` you need a shared store implementation.

## Django dev server

Single worker, autoreload, in-memory channel layer. Good for development,
**not** for any kind of production load:

```bash
just install
just dev
```

Or by hand:

```bash
uv sync --extra dev --extra django
cd examples/chat
uv run python manage.py migrate
uv run python manage.py runserver 0.0.0.0:8000
```

URLs (chat sample):

| Page         | URL                                                  |
|--------------|------------------------------------------------------|
| Rooms list   | http://localhost:8000/                               |
| Streams      | http://localhost:8000/streams/                       |
| Admin        | http://localhost:8000/admin/                         |

## Production-style local run

The `just serve` recipe spins up a real Redis container under `podman`,
applies migrations against the chat sample using the `settings_prod`
module, and runs **multiple `hypercorn` workers** in front of the Django
ASGI app. Multiple workers makes SSE broadcasts go through Redis (the
channel layer) rather than in-process memory.

```bash
just install
just serve              # 4 workers, 0.0.0.0:8000
just serve 8            # 8 workers
```

What it actually runs:

```bash
# 1. Bring up Redis (idempotent)
podman start rakaia-redis 2>/dev/null \
  || podman run -d --name rakaia-redis -p 6379:6379 docker.io/redis:7-alpine

# 2. Migrate using prod settings
cd examples/chat
DJANGO_SETTINGS_MODULE=chat_project.settings_prod \
  uv run python manage.py migrate --noinput

# 3. Collect static files into STATIC_ROOT (Django admin CSS, etc.)
DJANGO_SETTINGS_MODULE=chat_project.settings_prod \
  uv run python manage.py collectstatic --noinput

# 4. Multi-worker hypercorn against the Django ASGI app
DJANGO_SETTINGS_MODULE=chat_project.settings_prod \
  uv run hypercorn \
    --bind 0.0.0.0:8000 \
    --workers 4 \
    --access-logfile - \
    chat_project.asgi:application
```

### Static files (WhiteNoise)

`runserver` serves `/static/` automatically when `DEBUG=True`; no other
ASGI server does. Without something handling static files, the Django
admin loads but its CSS/JS 404s and the page looks broken.

The chat sample uses [WhiteNoise](https://whitenoise.readthedocs.io/) —
in-process middleware that serves `STATIC_ROOT` directly from the Python
worker, with content-hashed filenames and gzip/brotli pre-compression for
far-future caching:

* `whitenoise` is in the `[prod]` extra.
* `WhiteNoiseMiddleware` is in `MIDDLEWARE` right after
  `SecurityMiddleware` (so static-file URLs bypass the rest of the stack).
* `STATIC_ROOT = BASE_DIR / "staticfiles"` in `settings.py`.
* `settings_prod.py` sets `STORAGES["staticfiles"]` to
  `whitenoise.storage.CompressedManifestStaticFilesStorage`.
* `just serve` runs `collectstatic --noinput` before launching hypercorn.

In dev (`just dev`, `DEBUG=True`), Django's `runserver` static handler
takes precedence and WhiteNoise stays out of the way — no extra commands
needed.

If you put a reverse proxy (Caddy, nginx) in front of hypercorn for
real-prod, leave WhiteNoise enabled. The proxy terminates TLS and forwards
to hypercorn; WhiteNoise still serves the bytes and the proxy forwards
the cache headers through. You only need the proxy to *also* serve static
files directly if you want to take WhiteNoise out of the request path
entirely (slight perf win, more config).

### Why hypercorn (and not Daphne)?

| Server     | Multi-worker?            | Notes                                 |
|------------|--------------------------|---------------------------------------|
| Daphne     | No (single process)      | Fine for dev, can't scale CPU         |
| Uvicorn    | Only via gunicorn workers| Adds gunicorn as a dep                |
| **Hypercorn** | **Yes (`--workers N`)** | Pure Python, native ASGI, simple CLI |
| Granian    | Yes                      | Faster Rust impl, less mainstream     |

We use Daphne for the *dev* server (it's already a transitive dep via the
`runserver` ASGI integration in `[django]`) and hypercorn for the *prod*
recipe via the `[prod]` extra.

### Required environment variables

`chat_project/settings_prod.py` reads:

| Variable               | Default                       | Purpose                       |
|------------------------|-------------------------------|-------------------------------|
| `DJANGO_SECRET_KEY`    | *(insecure default)*          | Django secret. **Set this.**  |
| `DJANGO_ALLOWED_HOSTS` | `localhost,127.0.0.1`         | Comma-separated host allow-list|
| `REDIS_URL`            | `redis://localhost:6379/0`    | Channel layer Redis URL       |

The justfile picks these up automatically (it sets `set dotenv-load`), so
dropping a `.env` file in the repo root works:

```bash
# .env
DJANGO_SECRET_KEY=please-change-me
DJANGO_ALLOWED_HOSTS=streams.example.com,localhost
REDIS_URL=redis://localhost:6379/0
```

### Production hardening (real prod, not local)

For an actual deployment, you'd want at least:

1. **A real database** (Postgres). The chat sample uses SQLite which
   serializes writes — fine for a demo, a bottleneck under contention.
2. **Redis with persistence + auth.** Replace the simple `podman run` with:
   ```bash
   podman run -d --name rakaia-redis -p 6379:6379 \
       -v redis-data:/data \
       docker.io/redis:7-alpine \
       redis-server --appendonly yes --requirepass "$REDIS_PASSWORD"
   ```
   Then `REDIS_URL=redis://:$REDIS_PASSWORD@host:6379/0`.
3. **A reverse proxy** (nginx/Caddy/Traefik) terminating TLS and *not
   buffering* SSE responses. Nginx:
   ```nginx
   location /streams/ {
       proxy_pass http://hypercorn:8000;
       proxy_http_version 1.1;
       proxy_set_header Connection "";
       proxy_buffering off;          # critical for SSE
       proxy_read_timeout 24h;
   }
   ```
   The `X-Accel-Buffering: no` header is already set on the SSE responses.
4. **Process supervision.** systemd, s6, or your container orchestrator.
5. **Static files.** The chat sample has none, but if you add any, run
   `collectstatic` and serve them via the reverse proxy or WhiteNoise.

## Troubleshooting

**`just: command not found`** — install just from your package manager
(`brew install just`, `dnf install just`, `pacman -S just`, or
`cargo install just`).

**`podman: command not found`** — install podman or substitute `docker`
(`alias podman=docker` works for the recipes in this repo).

**Port already in use** — `just serve port=9000`.

**`channels_redis.core.RedisChannelLayer` not found** — you forgot
`--extra prod`. Run `just install` (or `uv sync --extra prod`) and try
again.

**SSE connection drops after 60s** — increase your reverse-proxy read
timeout (the nginx snippet above uses `24h`). The Django views themselves
have no timeout.

**Redis container won't start: `port already allocated`** — something
else is bound to `:6379`. Either stop it or change the port:
```bash
just redis-destroy
podman run -d --name rakaia-redis -p 16379:6379 docker.io/redis:7-alpine
REDIS_URL=redis://localhost:16379/0 just serve
```

**Workers see stale data after restart** — channels-redis caches
subscriptions in Redis. `just redis-destroy && just serve` resets it.
