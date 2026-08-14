"""The append envelope and the live fold — the two rituals every consumer retypes.

Two shapes show up at nearly every durable call site in a real Django consumer,
and neither is hard enough to be interesting or short enough to be safe:

* **Append an enveloped event** — JSON-encode the payload with Django's encoder,
  create the stream if it isn't there, wrap the label/actor/timestamp in an
  `AppendOptions`.
* **Fold a batch live** — seed a scratch in-memory `StreamStore`, then `replay()`
  it through a handler registry with a reader bound, so a projection can be
  materialised at write time using the same handlers a rebuild will use.

The first production consumer wrote the append ~37 times across 18 files and the
fold 11 times, and left a warning in its own module that names the risk exactly:
*"a second write path which re-implements the envelope is a path no gate
covers."* A copy that drifts — a missing `event_ts`, a `metadata` that is `None`
instead of `{}`, a `json.dumps` without the Django encoder — produces events that
replay differently from every other event in the same stream, and no test
anywhere is looking at the difference.

These live in `django_rakaia` rather than `rakaia` because both are Django-shaped:
the encoder is `DjangoJSONEncoder`, and the fold's default executor writes Django
models. The core package stays dependency-free.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from rakaia import AppendOptions, StreamStore
from rakaia.protocols import ProjectionReader, WritableStore
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay

#: The in-memory stream path a live fold replays through. A fold is not a
#: durable append — it exists to run the *same handlers* a rebuild will run, so
#: the write-time projection and the replayed one cannot diverge. The path is a
#: constant so a registry's ``event_match`` can name it.
SCRATCH_PATH = "produce/submission"


def append_event(
    store: WritableStore,
    stream_path: str,
    payload: dict[str, Any],
    *,
    label: str,
    actor: Any = None,
    event_ts: float | None = None,
) -> None:
    """Append one enveloped event to ``stream_path``, creating the stream if absent.

    The envelope, fixed so every call site produces the same shape:

    * the payload is JSON-encoded with `DjangoJSONEncoder`, so a `UUID`,
      `datetime` or `Decimal` survives the trip rather than raising `TypeError`
      at insert time;
    * ``metadata`` is always a dict carrying ``user`` — the key
      `rakaia.history.envelope_actor` reads. Ambient `provenance()` still merges
      underneath, so a request-scoped `url`/`causation` is not shut out;
    * ``event_ts`` is passed through. ``None`` means "order by append time",
      which is the pre-existing default, not a silent loss of ordering.

    ``create()`` is called unconditionally rather than guarded by ``has()``:
    creation is idempotent by contract and — as
    ``tests/store_contract.py::test_create_on_an_existing_stream_preserves_its_messages``
    pins — a redundant create cannot truncate a populated stream or rewind its
    offsets. One round trip instead of two.
    """
    store.create(stream_path)
    store.append(
        stream_path,
        json.dumps(payload, cls=DjangoJSONEncoder).encode(),
        AppendOptions(label=label, metadata={"user": actor}, event_ts=event_ts),
    )


def fold_events(
    events: Sequence[dict[str, Any]],
    registry: HandlerRegistry,
    *,
    reader: ProjectionReader | None = None,
    executor: Any = None,
    label: str = "import",
    scratch_path: str = SCRATCH_PATH,
) -> None:
    """Project ``events`` now, through the handlers a replay would use.

    Seeds a throwaway in-memory `StreamStore` with ``events`` in list order and
    replays it through ``registry`` — staged, and reader-bound when ``reader`` is
    given so stage-1 handlers resolve against the rows stage 0 just wrote.

    The point is that write-time projection and rebuild-time projection run the
    *same* handler code. A consumer that instead materialises rows directly at
    write time has two implementations of one projection, and a rebuild that
    disagrees with live is then indistinguishable from a rebuild that is correct.

    The scratch store is in-memory and fresh per call: a fold leaves nothing in
    the durable log (append separately, with `append_event`, if the event should
    also be recorded), and successive folds cannot replay each other's events.

    ``executor`` defaults to a `DjangoExecutor`. Pass a `CollectingExecutor` to
    see what a fold *would* write without writing it.
    """
    if not events:
        return

    from .effect_executor import DjangoExecutor

    scratch = StreamStore()
    scratch.create(scratch_path)
    for event in events:
        scratch.append(
            scratch_path,
            json.dumps(event, cls=DjangoJSONEncoder).encode(),
            AppendOptions(label=label),
        )

    replay(
        scratch,
        scratch_path,
        executor or DjangoExecutor(),
        handler_registry=registry,
        reader=reader,
    )
