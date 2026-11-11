# Rakaia

**Rakaia** has two halves, and you can use either without the other.

- **A server for the [Durable Streams protocol](docs/protocol.md)** — an
  HTTP-based protocol for append-only, ordered, durable byte streams. Choose this
  if you want to publish or follow ordered streams over HTTP, including live updates
  over SSE.
- **A way to build tables from a record of events** — keep an append-only log,
  replay it through versioned handlers to fill tables, and rebuild them after you
  fix a bug in a handler. Choose this if you want tables you can reproduce, rehearse
  and repair rather than patch.

It ships two installable packages:

- **`rakaia`** — Zero dependencies. The ASGI protocol server (run it standalone
  with `uvicorn`/`daphne`/`granian`, or mount it inside Django/FastAPI/Starlette),
  plus the replay engine, handlers and in-memory projections.
- **`django_rakaia`** — A Django app with normalized stream models, a
  `@stream_model` decorator, Channels-based SSE broadcasting, an admin
  interface, and Django-backed replay.

## Install

```bash
pip install rakaia-streams                 # core: protocol server + replay
pip install "rakaia-streams[django]"       # + Django integration
pip install "rakaia-streams[django,prod]"  # + channels-redis + hypercorn for prod
```

The distribution is named `rakaia-streams` on PyPI (plain `rakaia` was already
taken); the import names are unchanged — `import rakaia`, `import django_rakaia`.

Already using rakaia from a pinned git revision? See
[`UPGRADING.md`](UPGRADING.md) before bumping the pin — the distribution rename
above is itself a breaking change for a `[tool.uv.sources]` entry spelled
`rakaia`.

## Quick start (standalone)

```bash
pip install rakaia-streams uvicorn
uvicorn rakaia:app --port 4437
```

```python
from rakaia import create_app, StreamStore

app = create_app()  # in-memory store
app = create_app(store=StreamStore())
```

## Quick start (replay, no Django)

```python
from rakaia import (
    HandlerRegistry,
    InMemoryProjections,
    StreamStore,
    Upsert,
    register_simple,
    replay,
    seed_stream,
)

registry = HandlerRegistry()  # private, so the snippet can be re-run


@register_simple("rooms", "room:*", registry=registry)
def project_room(event):
    return Upsert("Room", lookup={"id": event["id"]}, defaults={"name": event["name"]})


events = [{"id": 1, "name": "lobby"}, {"id": 1, "name": "Lobby"}]
store = seed_stream("room:1", events, store=StreamStore())
tables = InMemoryProjections()
replay(store, "room:1", tables, handler_registry=registry)
print(tables.rows("Room"))  # [{'pk': 1, 'id': 1, 'name': 'Lobby'}]
```

Fix the handler, replay again into fresh tables, and the rows follow.

## Quick start (Django)

```python
# models.py
from dataclasses import dataclass
from django.db import models
from django_rakaia import stream_model


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

Built with [Zensical](https://zensical.org/): `uv sync --extra docs`, then
`uv run zensical serve` (preview at http://localhost:8000) or
`uv run zensical build`. Pages live in [`docs/`](docs/), config in
[`zensical.toml`](zensical.toml):

- [Overview & quick start](docs/index.md)
- [**What's new — a guided tour**](docs/whats-new.md) — start here to see the recent features, each with a one-command demo
- [Glossary](docs/glossary.md) — plain-language definitions of the event-sourcing terms
- [Django integration](docs/django-integration.md) · [Deployment](docs/deployment.md)
- [Versioned handlers](docs/versioned-handlers.md) · [Projections & fan-out](docs/projections-and-fan-out.md) · [Dry-run & executors](docs/dry-run-and-executors.md)
- [Translations](docs/translations.md) (example)
- [Protocol specification](docs/protocol.md) · [Backend storage](docs/streams-backend-storage.md)

## Versioned handlers (event replay with history)

Replay runs a stream through handlers whose *current* and *historical* versions
are both kept in source. Handlers are pure — they return `Effect` descriptions
that an executor applies idempotently, so replay can be re-run safely.

```python
from rakaia import Upsert, register_handler


@register_handler(
    name="mogrify", event_match="room:*:messages", effective_from=0, effective_to=10_000
)
def mogrify_v1(event):
    return Upsert(
        model_label="myapp.Room",
        lookup={"id": event["room_id"]},
        defaults={"name": event["name"]},
    )


@register_handler(name="mogrify", event_match="room:*:messages", effective_from=10_000)
def mogrify_v2(event):  # bugfix only for events from seq 10_000 onward
    ...
```

`python manage.py replay room:5:messages --from 0` runs every event through its
time-correct handler version, with drift detection (`--strict-drift`) and
dry-run (`--dry-run`) modes. See [`docs/versioned-handlers.md`](docs/versioned-handlers.md)
for upcasters and a worked example.

## Sample applications

Each example demonstrates one feature area end-to-end. Most are standalone Django
projects; two are zero-dependency scripts (no Django). Run them all with
`just demo`, or individually:

| Example | Demonstrates | Run |
|---|---|---|
| [`examples/orders/`](examples/orders/) | Versioned handlers, upcasters, replay, dry-run | `just orders-demo` |
| [`examples/formkit_submissions/`](examples/formkit_submissions/) | Projections/fan-out, `reconcile_children`, migration parity | `just formkit-demo` |
| [`examples/protocol_streams/`](examples/protocol_streams/) | Protocol layer (no Django): producer fencing, close, `poll` cursors | `just protocol-demo` |
| [`examples/multi_owner/`](examples/multi_owner/) | Effect primitives (no Django): `Ref`, `reconcile_aggregate(owns=)` | `just multi-owner-demo` |
| [`examples/chat/`](examples/chat/) | `@stream_model`, multi-stream events, live SSE | `just dev` |
| [`examples/polyglot/`](examples/polyglot/) | Language-scoped streams, live-editable translations | `just polyglot-dev` |

The full catalog with a concept-coverage matrix is in
[`docs/examples.md`](docs/examples.md), and the machine-readable
[Open Knowledge Format](https://github.com/GoogleCloudPlatform/knowledge-catalog)
bundle is in [`okf/`](okf/).

## Running it

With [`just`](https://github.com/casey/just) and `podman`: `just install`, then
`just dev` (single-worker dev server) or `just serve` (4 hypercorn workers +
Redis). `just --list` shows everything; see [`docs/deployment.md`](docs/deployment.md).

## Development

`just install`, then `just check` (lint + format + types + tests + docs build).
Or by hand: `uv sync --extra dev --extra django`, then `uv run pytest`,
`uv run ruff check`, `uv run pyright src/`.

### Protocol conformance

Beyond the pytest suite, rakaia is checked against the upstream, language-agnostic
[`@durable-streams/server-conformance-tests`](https://github.com/durable-streams/durable-streams/tree/main/packages/server-conformance-tests)
compliance suite:

```bash
just conformance   # starts rakaia, runs the suite against it, tears it down (needs node/npm)
```

This runs in CI as a non-blocking check (`.github/workflows/conformance.yml`).
rakaia passes the full protocol surface today except the stream **forking**
family, which is not yet implemented. See [`conformance/README.md`](conformance/README.md).

## License

MIT.
