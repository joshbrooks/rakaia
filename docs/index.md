---
icon: lucide/waves
---

# Rakaia Documentation

**Rakaia** is a Python implementation of the [Durable Streams protocol](protocol.md) — an
HTTP-based protocol for append-only, ordered, durable byte streams.

The project ships two installable packages:

- **`rakaia`** — A zero-dependency ASGI application implementing the protocol. Run
  it standalone with `uvicorn`/`daphne`/`granian`, or mount it inside Django,
  FastAPI, or Starlette.
- **`django_rakaia`** — A Django app that stores stream events in your database
  via normalized `Stream` / `StreamEvent` / `StreamEntry` models, broadcasts
  changes over Django Channels, and provides a `@stream_model` decorator for
  emitting events from your own Django models.

## Installation

```bash
# Core protocol server only (zero runtime dependencies)
pip install rakaia

# With the Django integration
pip install "rakaia[django]"

# With Redis-backed channel layer (for multi-process SSE)
pip install "rakaia[django,redis]"
```

## Quick Start (standalone server)

```bash
pip install rakaia uvicorn
uvicorn rakaia:app --port 4437
```

That gives you a fully functional Durable Streams server backed by an
in-memory store.

```python
from rakaia import create_app, StreamStore

# Default in-memory store
app = create_app()

# Or supply your own store
app = create_app(store=StreamStore())
```

## Quick Start (Django integration)

See [`django-integration.md`](django-integration.md) for the full guide.

```python
# settings.py
INSTALLED_APPS = [
    "daphne",
    "channels",
    "django_rakaia",
    # ...
]
ASGI_APPLICATION = "myproject.asgi.application"
CHANNEL_LAYERS = {
    "default": {"BACKEND": "channels.layers.InMemoryChannelLayer"},
}
```

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

Every save/delete now emits a `StreamEvent` and a `StreamEntry` against the
relevant stream(s), automatically broadcast to any connected SSE consumers via
the channel layer.

## Documentation index

- [Protocol specification](protocol.md) — Wire format, headers, semantics.
- [Django integration](django-integration.md) — Models, decorator, admin, SSE.
- [Translations](translations.md) — Optional `Translatable` model and UI.
- [Deployment](deployment.md) — Production setup, ASGI servers, Redis channel
  layer, scaling.

## Sample application

A minimal standalone Django chat app demonstrating the library lives in
[`examples/chat/`](../examples/chat/). It shows multi-stream events, SSE
consumption, and the `@stream_model` decorator end-to-end.
