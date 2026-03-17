# Django Rakaia Protocol HTTP API

Protocol-compliant HTTP API for the Durable Streams protocol, implemented on top of Django's normalized stream models.

## Overview

This module provides a RESTful HTTP API that implements the [Durable Streams Protocol](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md), allowing frontend clients to interact with streams using standard HTTP methods.

## Installation

The protocol views are included in the `django_rakaia` package. To enable them, add the URLs to your project:

```python
# urls.py
from django.urls import path, include

urlpatterns = [
    # ... other URLs
    path("protocol/", include("django_rakaia.protocol_views")),
]
```

## Architecture

The protocol API is built on top of the normalized stream model:

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Stream    │────<│ StreamEntry  │>────│ StreamEvent │
│ (stream_id) │     │   (offset)   │     │   (data)    │
└─────────────┘     └──────────────┘     └─────────────┘
```

This allows:
- **Single event, multiple streams**: One `StreamEvent` can appear in multiple streams
- **Independent offsets**: Each stream maintains its own monotonic offset sequence
- **Efficient queries**: Fast lookups by stream_id and offset range

## API Endpoints

### Create Stream

```
PUT /protocol/streams/{stream_path}/create
```

Creates a new stream or returns success if it already exists (idempotent).

**Request Headers:**
- `Content-Type`: MIME type (default: `application/json`)

**Response:**
- `201 Created`: Stream created
- `200 OK`: Stream already exists

**Response Headers:**
- `Stream-Next-Offset`: Initial offset (`0_0000000000000000`)
- `Content-Type`: The stream's content type

**Example:**
```bash
curl -X PUT http://localhost:8000/protocol/streams/area:5:projects/create \
  -H "Content-Type: application/json"
```

### Append to Stream

```
POST /protocol/streams/{stream_path}/append
```

Appends data to an existing stream.

**Request Headers:**
- `Content-Type`: Must be `application/json` (or match stream's type)

**Request Body:**
- JSON object or any data

**Response:**
- `204 No Content`: Append successful
- `404 Not Found`: Stream does not exist

**Response Headers:**
- `Stream-Next-Offset`: New tail offset

**Example:**
```bash
curl -X POST http://localhost:8000/protocol/streams/area:5:projects/append \
  -H "Content-Type: application/json" \
  -d '{"event": "project_created", "project_id": 123}'
```

### Read Stream (Catch-up)

```
GET /protocol/streams/{stream_path}/read?offset={offset}
```

Reads events from a stream starting at the given offset.

**Query Parameters:**
- `offset`: Start reading after this offset (format: `read_seq_byte_offset`)
  - Default: `-1` (read from beginning)
  - Example: `0_0000000000000005`

**Response:**
- `200 OK`: Returns newline-delimited JSON events
- `404 Not Found`: Stream does not exist
- `400 Bad Request`: Invalid offset format

**Response Headers:**
- `Content-Type`: `application/json`
- `Stream-Next-Offset`: Current tail offset
- `Stream-Up-To-Date`: `true` if no more events

**Example:**
```bash
# Read all events
curl http://localhost:8000/protocol/streams/area:5:projects/read

# Read from offset 5
curl http://localhost:8000/protocol/streams/area:5:projects/read?offset=0_0000000000000005
```

**Response Format:**
```
{"event": "project_created", "project_id": 123}
{"event": "project_updated", "project_id": 123, "name": "New Name"}
{"event": "project_deleted", "project_id": 123}
```

### Read Stream (SSE Live)

```
GET /protocol/streams/{stream_path}/sse?cursor={cursor}
```

Streams new events in real-time using Server-Sent Events.

**Query Parameters:**
- `cursor`: Start streaming events after this cursor (format: `read_seq_byte_offset`)
  - Default: `0_0` (stream from beginning)

**Response:**
- `200 OK`: Streaming response with `text/event-stream` content type

**Response Headers:**
- `Content-Type`: `text/event-stream`
- `Cache-Control`: `no-cache`

**Example:**
```bash
curl http://localhost:8000/protocol/streams/area:5:projects/sse?cursor=0_0000000000000000
```

**SSE Event Format:**
```
event: message
id: 0_0000000000000006
data: {"event": "project_created", "project_id": 456}

event: message
id: 0_0000000000000007
data: {"event": "project_updated", "project_id": 456}
```

### Stream Metadata

```
HEAD /protocol/streams/{stream_path}/metadata
```

Gets metadata for a stream without transferring data.

**Response:**
- `200 OK`: Stream exists
- `404 Not Found`: Stream does not exist

**Response Headers:**
- `Content-Type`: The stream's content type
- `Stream-Next-Offset`: Current tail offset

**Example:**
```bash
curl -I http://localhost:8000/protocol/streams/area:5:projects/metadata
```

## Frontend Client Usage

### JavaScript (Vanilla)

```javascript
// Catch-up read
async function getStreamEvents(streamPath, offset = '0_0') {
  const response = await fetch(`/protocol/streams/${streamPath}/read?offset=${offset}`);
  const text = await response.text();
  
  if (!text) return [];
  
  return text.split('\n').map(line => JSON.parse(line));
}

// Live updates via SSE
function subscribeToStream(streamPath, cursor = '0_0', onEvent) {
  const eventSource = new EventSource(
    `/protocol/streams/${streamPath}/sse?cursor=${cursor}`
  );
  
  eventSource.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);
    const offset = event.lastEventId;
    onEvent(data, offset);
  });
  
  eventSource.onerror = (error) => {
    console.error('SSE connection error:', error);
    eventSource.close();
    
    // Reconnect after 3 seconds
    setTimeout(() => subscribeToStream(streamPath, cursor, onEvent), 3000);
  };
  
  return eventSource;
}

// Usage
const events = await getStreamEvents('area:5:projects');
console.log('Historical events:', events);

const sse = subscribeToStream(
  'area:5:projects',
  '0_0000000000000005',
  (data, offset) => {
    console.log('New event:', data, 'at offset:', offset);
  }
);
```

### TypeScript Example

```typescript
interface StreamEvent {
  event: string;
  [key: string]: any;
}

interface StreamResponse {
  events: StreamEvent[];
  nextOffset: string;
}

async function fetchStreamEvents(
  streamPath: string,
  offset: string = '0_0'
): Promise<StreamResponse> {
  const response = await fetch(
    `/protocol/streams/${streamPath}/read?offset=${offset}`
  );
  
  if (!response.ok) {
    throw new Error(`Stream read failed: ${response.status}`);
  }
  
  const text = await response.text();
  const events = text
    ? text.split('\n').map(line => JSON.parse(line))
    : [];
  
  return {
    events,
    nextOffset: response.headers.get('Stream-Next-Offset') || '0_0',
  };
}
```

## Integration with Django Models

The protocol API automatically captures events from your Django models via signal handlers:

```python
# models.py
from django.db import models
from django_rakaia.decorators import stream_model

@stream_model(
    stream_paths=lambda obj: [
        f"user:{obj.created_by_id}:projects",  # User's stream
        f"area:{obj.area_id}:projects",        # Area's stream
    ],
    to_dataclass=lambda obj: ProjectData(
        id=obj.id,
        name=obj.name,
        area_id=obj.area_id,
    ),
)
class Project(models.Model):
    name = models.CharField(max_length=100)
    area = models.ForeignKey(Area, on_delete=models.CASCADE)
    created_by = models.ForeignKey("auth.User", on_delete=models.CASCADE)
```

When a `Project` is saved, it automatically appears in both streams via the protocol API.

## Offset Format

Offsets follow the Durable Streams protocol format: `{read_seq}_{byte_offset}`

- `read_seq`: Read sequence number (usually 0 for simple implementations)
- `byte_offset`: Byte position in the stream (zero-padded to 16 digits)

Examples:
- `0_0000000000000000`: Initial offset
- `0_0000000000000001`: First event
- `0_0000000000000005`: Fifth event

## Error Handling

| Status Code | Meaning | When |
|-------------|---------|------|
| `200 OK` | Success | Read/metadata operations |
| `201 Created` | Stream created | PUT to new stream |
| `204 No Content` | Append successful | POST append |
| `400 Bad Request` | Invalid request | Invalid offset format |
| `404 Not Found` | Stream not found | Stream doesn't exist |
| `409 Conflict` | Conflict | Content-type mismatch (TODO) |

## Testing

Run the protocol compliance tests:

```bash
uv run pytest tests/test_django_rakaia/test_protocol.py -v
```

Expected output:
```
================= 23 passed, 1 skipped =================
```

The skipped test is for content-type validation (TODO item).

## Limitations & TODOs

- [ ] Content-type validation on append (currently accepts any type)
- [ ] Stream-TTL and Stream-Expires-At headers
- [ ] Stream-Closed functionality
- [ ] Idempotent producer headers (Producer-Id, Producer-Epoch, Producer-Seq)
- [ ] Stream-Seq coordination
- [ ] SSE reconnection with automatic cursor resumption

## Protocol Compliance

This implementation passes 23 of 24 protocol compliance tests. See `tests/test_django_rakaia/test_protocol.py` for the full test suite.

For full protocol specification, see: [Durable Streams PROTOCOL.md](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md)
