"""
Django-side ``replay`` convenience.

``rakaia.replay.replay()`` is framework-agnostic: it takes an executor and, for
staged replays, a reader. From Django both of those are always the same pair —
``DjangoExecutor`` writes the projections and ``DjangoProjectionReader`` reads
them back for stage > 0 handlers and reducers. ``replay_stream`` fills those in
so a staged replay does not fail with "stage > 0 but no reader" just because the
caller didn't wire the Django reader by hand (see issue #68).

    from django_rakaia.replay import replay_stream

    replay_stream("submissions:42")                 # applies via the ORM
    replay_stream("submissions:42", executor=CollectingExecutor())  # dry run

Everything else is forwarded to ``rakaia.replay.replay`` unchanged.
"""

from __future__ import annotations

from rakaia.effects import Executor
from rakaia.protocols import ProjectionReader, ReadableStore
from rakaia.registry import HandlerRegistry, UpcasterRegistry
from rakaia.replay import OnDriftPolicy, ReplayResult, replay

from .effect_executor import DjangoExecutor
from .projection_reader import DjangoProjectionReader
from .store import get_store


def replay_stream(
    stream_path: str,
    *,
    executor: Executor | None = None,
    reader: ProjectionReader | None = None,
    handler_registry: HandlerRegistry | None = None,
    upcaster_registry: UpcasterRegistry | None = None,
    start_seq: int = 0,
    end_seq: int | None = None,
    event_match: str | None = None,
    on_drift: OnDriftPolicy = "warn",
    store: ReadableStore | None = None,
) -> ReplayResult:
    """Replay ``stream_path`` through the Django executor + reader by default.

    ``executor`` defaults to ``DjangoExecutor()`` (pass a ``CollectingExecutor``
    for a dry run) and ``reader`` to ``DjangoProjectionReader()``, so a staged
    replay works without the caller wiring the reader. ``store`` defaults to the
    Django global store. All other arguments match ``rakaia.replay.replay``.
    """
    return replay(
        store=store if store is not None else get_store(),
        stream_path=stream_path,
        executor=executor if executor is not None else DjangoExecutor(),
        handler_registry=handler_registry,
        upcaster_registry=upcaster_registry,
        start_seq=start_seq,
        end_seq=end_seq,
        event_match=event_match,
        on_drift=on_drift,
        reader=reader if reader is not None else DjangoProjectionReader(),
    )
