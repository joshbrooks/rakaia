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

from collections.abc import Sequence
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder

from rakaia import AppendOptions, seed_stream
from rakaia.protocols import ProjectionReader, WritableStore
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay

#: The in-memory stream path a live fold replays through.
#:
#: **Arbitrary, but load-bearing.** The store is in-memory and thrown away per
#: call, so the value means nothing on its own — but a registry's ``event_match``
#: has to name it for the handlers to fire, which makes it part of the calling
#: contract rather than an implementation detail. It is a constant so both sides
#: can refer to one name instead of repeating a string.
#:
#: The leading underscore marks it as rakaia's own namespace, so it cannot
#: collide with a consumer's stream paths. It previously read
#: ``"produce/submission"`` — domain language borrowed from the first consumer,
#: which implied a convention that does not exist and made every other consumer
#: register handlers against another project's vocabulary (#100).
SCRATCH_PATH = "_scratch/fold"


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
    * an ``actor`` is recorded under ``user`` — the key
      `rakaia.history.envelope_actor` reads. Ambient `provenance()` still merges
      underneath, so a request-scoped `url`/`causation` is not shut out. A
      ``None`` actor is *omitted* rather than written: `merge_provenance` layers
      ambient under explicit, so an explicit ``{"user": None}`` would beat the
      actor `ProvenanceMiddleware` had already stamped on the block — turning
      the caller's silence into a positive assertion that nobody did this;
    * ``event_ts`` is passed through. ``None`` means "order by append time",
      which is the pre-existing default, not a silent loss of ordering.

    The create-and-append itself is `rakaia.seed_stream`, handed the Django
    encoder: this module's whole warning is about a second `json.dumps` rule
    drifting from the first, so there is one and it lives in the core package.
    What stays here is the Django-shaped part — which encoder, and where an
    actor goes.

    ``create()`` is called unconditionally rather than guarded by ``has()``:
    creation is idempotent by contract and — as
    ``tests/store_contract.py::test_create_on_an_existing_stream_preserves_its_messages``
    pins — a redundant create cannot truncate a populated stream or rewind its
    offsets. One round trip instead of two.
    """
    seed_stream(
        stream_path,
        [
            (
                payload,
                AppendOptions(
                    label=label,
                    metadata={"user": actor} if actor is not None else None,
                    event_ts=event_ts,
                ),
            )
        ],
        store=store,
        encoder=DjangoJSONEncoder,
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

    ``scratch_path`` must match the ``event_match`` the registry's handlers were
    registered against, or nothing fires. It defaults to `SCRATCH_PATH`; register
    against that same constant rather than repeating the string.

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

    scratch = seed_stream(
        scratch_path,
        [(event, AppendOptions(label=label)) for event in events],
        encoder=DjangoJSONEncoder,
    )

    replay(
        scratch,
        scratch_path,
        executor or DjangoExecutor(),
        handler_registry=registry,
        reader=reader,
    )
