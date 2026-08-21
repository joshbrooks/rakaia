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
pip install rakaia-streams                 # core protocol server
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
- [**What's new — a guided tour**](docs/whats-new.md) — start here to see the recent features, each with a one-command demo
- [Glossary](docs/glossary.md) — plain-language definitions of the event-sourcing terms
- [Django integration](docs/django-integration.md)
- [Versioned handlers](docs/versioned-handlers.md)
- [Projections & fan-out](docs/projections-and-fan-out.md)
- [Dry-run & executors](docs/dry-run-and-executors.md)
- [Translations](docs/translations.md) (example)
- [Deployment](docs/deployment.md)
- [Protocol specification](docs/protocol.md) · [Backend storage](docs/streams-backend-storage.md)

## Versioned handlers (event replay with history)

Rakaia ships a subsystem for replaying a stream through handlers whose
*current* and *historical* versions are both kept in source. Handlers are
pure — they return `Effect` descriptions that an executor applies via
idempotent `update_or_create`, so replay can be re-run safely.

```python
from rakaia import Upsert, register_handler, register_upcaster


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


@register_upcaster(event_match="room:*:messages", from_version=1)
def upcast_v1_to_v2(event):  # schema-shape change handled separately
    return {**event, "currency": "USD"}
```

`python manage.py replay room:5:messages --from 0` then runs every event
through its time-correct handler version, with drift detection (`--strict-drift`)
and dry-run (`--dry-run`) modes.

See [`docs/versioned-handlers.md`](docs/versioned-handlers.md) for the
full story, including a worked example based on Partisipa's submissions
pipeline.

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

For the full catalog with a concept-coverage matrix, see
[`docs/examples.md`](docs/examples.md) (human) or the machine-readable
[Open Knowledge Format](https://github.com/GoogleCloudPlatform/knowledge-catalog)
bundle in [`okf/`](okf/) (agents/tools). For a narrated walkthrough, see
[`docs/whats-new.md`](docs/whats-new.md).

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
