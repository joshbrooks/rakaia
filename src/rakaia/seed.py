"""Putting events into a stream — the first thing anyone does, in one call.

Getting a handful of events into a stream took four lines and rakaia never
shipped a way to do it, so everyone wrote the four lines: create the stream,
loop, `json.dumps`, `.encode()`. Our own suite had six hand-rolled copies in
three different shapes and our examples had twelve more, four of them wrapped in
a locally-defined `_append`. The cost lands hardest on tests, where setup ran to
sixty-eight lines before the first assertion and the reader could not see what
the test was about.

The shape here is the union of what those call sites actually needed, and
nothing else:

* **The store is optional.** Omit it and you get a fresh in-memory `StreamStore`;
  pass one and it is used. Either way it is returned, so both the
  ``seed_stream(...)`` and ``store = seed_stream(...)`` styles read naturally.
* **Any `WritableStore` works** — `DjangoStreamStore` and a process-wide
  singleton are as valid as an in-memory store.
* **The envelope is per event, not per batch.** A batch-level ``label=`` would
  have been shorter and wrong: real callers want a different label on each event
  with metadata on only the first, or a different ``event_ts`` on each. An event
  may therefore be given as ``(payload, AppendOptions(...))``, reusing the
  vocabulary `append` already speaks rather than inventing a parallel one.
* **Payloads may be dicts or already-encoded bytes**, because callers pinning an
  exact on-the-wire shape pass bytes.
* **Creation is unconditional.** `create()` is idempotent by store contract and,
  as ``tests/store_contract.py::test_create_on_an_existing_stream_preserves_its_messages``
  pins, cannot truncate a populated stream — so seeding the same path twice
  appends, and no caller needs a ``has()`` guard.

The ``encoder`` hook is the load-bearing part. Django payloads need
`DjangoJSONEncoder` so a `UUID`, `datetime` or `Decimal` survives, and a
dependency-free core package cannot import it. Taking the encoder as a parameter
keeps **one** `json.dumps` call in the codebase: `django_rakaia.envelope.append_event`
passes its encoder in rather than repeating the rule. `envelope.py` warns that a
drifting second copy "produces events that replay differently from every other
event in the same stream, and no test anywhere is looking at the difference" —
a hook is how that stays a warning about the past.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any, TypeVar, Union, overload

from .protocols import WritableStore
from .store import StreamStore
from .types import AppendOptions

#: One event to seed: a JSON-encodable payload, pre-encoded bytes, or either of
#: those paired with the `AppendOptions` envelope to record on it.
SeedEvent = Union[
    "dict[str, Any]",
    bytes,
    "tuple[dict[str, Any] | bytes, AppendOptions | None]",
]

_S = TypeVar("_S", bound=WritableStore)


@overload
def seed_stream(
    path: str,
    events: Iterable[SeedEvent] = ...,
    *,
    store: _S,
    encoder: type[json.JSONEncoder] | None = ...,
) -> _S: ...


@overload
def seed_stream(
    path: str,
    events: Iterable[SeedEvent] = ...,
    *,
    store: None = ...,
    encoder: type[json.JSONEncoder] | None = ...,
) -> StreamStore: ...


def seed_stream(
    path: str,
    events: Iterable[SeedEvent] = (),
    *,
    store: _S | None = None,
    encoder: type[json.JSONEncoder] | None = None,
) -> _S | StreamStore:
    """Create ``path`` and append ``events`` to it, in list order.

    ``events`` are payloads — dicts, which are JSON-encoded, or ``bytes``, which
    are appended untouched. Pair one with `AppendOptions` to give it an envelope::

        store = seed_stream("submissions", [
            ({"key": "s1", "a": 1}, AppendOptions(label="insert",
                                                  metadata={"user": 42})),
            ({"key": "s1", "a": 2}, AppendOptions(label="update")),
        ])

    ``store`` defaults to a fresh in-memory `StreamStore` and is returned either
    way. ``encoder`` is a `json.JSONEncoder` subclass passed straight to
    `json.dumps`; it never touches a ``bytes`` payload.

    Seeding is additive: the stream is created unconditionally, which is
    idempotent and non-destructive, so seeding an existing path appends to it.
    """
    target: Any = StreamStore() if store is None else store
    target.create(path)
    for event in events:
        payload, options = event if isinstance(event, tuple) else (event, None)
        data = (
            payload
            if isinstance(payload, bytes)
            else json.dumps(payload, cls=encoder).encode("utf-8")
        )
        target.append(path, data, options)
    return target
