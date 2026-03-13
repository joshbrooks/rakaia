# Rakaia

**Rakaia** is a high-performance, rock-solid Python server implementation of the [Durable Streams protocol](PROTOCOL.md).

It is a zero-dependency ASGI application that can run standalone (via `uvicorn`, `daphne`, or `granian`) or be integrated seamlessly into existing web frameworks.

## Quick Start

```bash
pip install rakaia
pip install uvicorn  # or daphne

# Run standalone
uvicorn rakaia:app --port 4437
```

## Usage

```python
from rakaia import create_app, StreamStore

# Default (in-memory store)
app = create_app()

# Custom store
store = StreamStore()
app = create_app(store=store)
```

### Mount in Django

```python
from django.urls import path
from rakaia import create_app

urlpatterns = [
    path("streams/", create_app()),
]
```

### Mount in FastAPI

```python
from fastapi import FastAPI
from rakaia import create_app

app = FastAPI()
app.mount("/streams", create_app())
```
