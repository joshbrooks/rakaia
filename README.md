# Rakaia

**Rakaia** is a Python implementation of the [Durable Streams protocol](docs/protocol.md) —
an HTTP-based protocol for append-only, ordered, durable byte streams.

It ships two installable packages:

- **`rakaia`** — A zero-dependency ASGI app implementing the protocol. Run it
  standalone (`uvicorn`/`daphne`/`granian`) or mount it inside Django/FastAPI/Starlette.
- **`django_rakaia`** — A Django app with normalized stream models, a
  `@stream_model` decorator, Channels-based SSE broadcasting, and an admin
  interface.

## Install

```bash
pip install rakaia                 # core protocol server
pip install "rakaia[django]"       # + Django integration
pip install "rakaia[django,prod]"  # + channels-redis + hypercorn for prod
```

## Quick start (standalone)

```bash
pip install rakaia uvicorn
uvicorn rakaia:app --port 4437
```

```python
from rakaia import create_app, StreamStore

app = create_app()                  # in-memory store
app = create_app(store=StreamStore())
```

## Quick start (Django)

```python
# models.py
from dataclasses import dataclass
from django.db import models
from django_rakaia.decorators import stream_model

@dataclass
class RoomData:
    id: int
    name: str

@stream_model(
    stream_paths=lambda obj: f"room:{obj.id}:messages",
    to_dataclass=lambda obj: RoomData(id=obj.id, name=obj.name),
)
class ChatRoom(models.Model):
    name = models.CharField(max_length=100)
```

Every save/delete now emits a stream event you can subscribe to over SSE.

## Documentation

The documentation site is built with [Zensical](https://zensical.org/).

```bash
uv sync --extra docs
uv run zensical serve   # live preview at http://localhost:8000
uv run zensical build   # static build into ./site
```

Pages live in [`docs/`](docs/) and the site config is in
[`zensical.toml`](zensical.toml):

- [Overview & quick start](docs/index.md)
- [Django integration](docs/django-integration.md)
- [Translations](docs/translations.md)
- [Deployment](docs/deployment.md)
- [Protocol specification](docs/protocol.md)

## Sample application

A standalone Django chat app demonstrating the library lives in
[`examples/chat/`](examples/chat/).

## Running it

If you have [`just`](https://github.com/casey/just) and `podman`:

```bash
just install
just dev      # single-worker dev server
just serve    # production-style: 4 hypercorn workers + Redis (podman)
```

`just --list` shows everything. The full guide is in
[`docs/deployment.md`](docs/deployment.md).

## Development

```bash
just install
just check   # lint + format + types + tests + docs build
```

Or by hand:

```bash
uv sync --extra dev --extra django
uv run pytest
uv run ruff check src/
uv run pyright src/
```

## License

MIT.
