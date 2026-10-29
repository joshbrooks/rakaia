"""
Django decorators for streaming model changes to stream events.

Supports multi-stream events where a single event can appear in multiple
streams with independent, monotonic offsets per stream.
"""

import dataclasses
import json
from collections.abc import Callable
from typing import Any, Literal

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import StreamEvent
from django_rakaia.store import get_store
from rakaia.types import AppendOptions, StreamClosed, StreamNotFound

StreamPathResolver = (
    str
    | Callable[[models.Model], str]
    | list[str]
    | Callable[[models.Model], list[str]]
)
DataclassTransformer = Callable[[models.Model], Any]
DeleteEventType = Literal["delete", "update"] | None

#: Every model decorated with `@stream_model`, for the startup check that warns
#: when their events would go to a store that forgets them on restart.
streamed_models: list[type[models.Model]] = []


def create_stream_event(
    stream_paths: str | list[str] | Callable[[models.Model], str | list[str]],
    to_dataclass: DataclassTransformer,
    instance: models.Model,
    action: str,
    using: str | None = None,
) -> StreamEvent | None:
    """
    Create a stream event for the given model instance.

    Supports single or multiple streams. The event data is stored once,
    with separate StreamEntry records for each stream.

    Args:
        stream_paths: Single stream ID, list of stream IDs, or callable
            that returns either. The callable receives the model instance.
        to_dataclass: Callable converting model instance to dataclass.
        instance: The model instance that changed.
        action: Event type ("create", "update", or "delete").
        using: Database alias to write to. Defaults to the instance's own
            (`instance._state.db`), so an event lands on the same database as
            the save that produced it — the two are one write and must not be
            split (#159). Pass explicitly to override.

    The event is appended through the store `RAKAIA_STORE` names, by the same
    rules as any other append: a closed stream raises `StreamClosed`, and the
    save that fired it fails with it. With the durable store the event is
    written in the save's transaction; with the memory or file store it is
    written immediately and cannot be rolled back with the row.

    Returns:
        The created StreamEvent, or None when the configured store is not the
        database one (it has no `StreamEvent` row to hand back).

    Example (single stream):
        create_stream_event(
            stream_paths=f"user:{instance.id}:activity",
            to_dataclass=to_user_data,
            instance=instance,
            action="create",
        )

    Example (multiple streams):
        create_stream_event(
            stream_paths=[
                f"user:{instance.created_by_id}:projects",
                f"area:{instance.area_id}:projects",
            ],
            to_dataclass=to_project_data,
            instance=instance,
            action="create",
        )
    """
    # Resolve stream paths
    if callable(stream_paths):
        resolved = stream_paths(instance)
    else:
        resolved = stream_paths

    # Normalize to list
    if isinstance(resolved, str):
        stream_ids = [resolved]
    else:
        stream_ids = list(resolved)

    # Convert to dataclass and then dict
    dc_instance = to_dataclass(instance)

    # `is_dataclass` alone admits a dataclass *class* as well as an instance of
    # one, and `asdict` accepts only the instance. Rejecting the class here
    # turns what was an obscure `asdict` failure into the same clear message
    # every other wrong return from `to_dataclass` already gets.
    if not dataclasses.is_dataclass(dc_instance) or isinstance(dc_instance, type):
        raise TypeError(
            f"to_dataclass must return a dataclass instance, got {type(dc_instance)}"
        )

    # Encode here, not just on the way to the column. `StreamEvent.data` uses
    # DjangoJSONEncoder, but that only covers the INSERT — the encoded form is
    # never written back onto the in-memory instance. The SSE fan-out
    # (`channels_signals.handle_stream_entry_created`) broadcasts
    # `instance.event.data` from that same in-memory object, and the production
    # channel layer (`channels_redis`, msgpack) and the SSE view (`json.dumps`)
    # both choke on a raw UUID/datetime/Decimal — synchronously, inside the
    # consumer's own `post_save`. Round-tripping through the encoder here makes
    # the instance carry exactly the primitives the DB row does (issue #80).
    payload = json.loads(
        json.dumps(dataclasses.asdict(dc_instance), cls=DjangoJSONEncoder)
    )

    # The event goes through the configured store, like every other append, so a
    # closed stream refuses it (`StreamClosed`) and `RAKAIA_STORE` decides where
    # it lands (#341). The save being audited decides the database, unless a
    # caller says otherwise. `getattr` rather than `instance._state.db`: this
    # door accepts anything with the attributes `to_dataclass` reads, and the
    # envelope-writer tests pass a plain stand-in with no ORM state; no state
    # means the default database.
    state = getattr(instance, "_state", None)
    alias = using if using is not None else getattr(state, "db", None)
    configured = get_store()

    if isinstance(configured, DjangoStreamStore):
        # Same database as the save, and one transaction (a savepoint inside the
        # caller's own) so the event stays with the row that produced it. The
        # store stamps `event_ts` and merges `provenance()` under its locks.
        return DjangoStreamStore(
            using=alias, permanent=configured.permanent
        ).append_to_many(stream_ids, payload, label=action)

    # A memory or file store cannot share a transaction with the row, so the
    # event is written now rather than on commit: a refusal (a closed stream)
    # must reach the code saving the model, and `on_commit` would raise it after
    # the row had already committed. The price is that a save that later rolls
    # back leaves its event behind. A fan-out is one append per stream here, so
    # every stream is checked first: one closed stream refuses the whole save,
    # as it does on the database store, instead of leaving the earlier ones
    # written (barring a close that lands between the check and the writes).
    # An expired stream reads as missing and is started again.
    options = AppendOptions(content_type="application/json", label=action)
    body = json.dumps(payload).encode()
    for stream_id in sorted(set(stream_ids)):
        existing = configured.get(stream_id)
        if existing is not None and existing.closed:
            raise StreamClosed(f"Stream is closed: {stream_id}")
    for stream_id in sorted(set(stream_ids)):
        try:
            result = configured.append(stream_id, body, options)
        except StreamNotFound:
            configured.create(stream_id, content_type="application/json")
            result = configured.append(stream_id, body, options)
        if result.stream_closed and result.message is None:
            raise StreamClosed(f"Stream is closed: {stream_id}")
    return None


def stream_model(
    stream_paths: StreamPathResolver,
    to_dataclass: DataclassTransformer,
    on_delete: DeleteEventType = "delete",
    delete_to_dataclass: DataclassTransformer | None = None,
) -> Callable[[type[models.Model]], type[models.Model]]:
    """
    Decorator to automatically stream Django model changes to stream events.

    Supports single or multiple streams per event.

    Args:
        stream_paths: Stream ID(s) or callable(s) that return stream ID(s).
            Can be:
            - A single stream ID string
            - A list of stream ID strings
            - A callable that takes the instance and returns a string or list
        to_dataclass: Callable converting model instance to dataclass.
        on_delete: What to append when Django fires ``post_delete``:
            - ``"delete"`` (default): a ``delete`` event — the row is gone.
            - ``"update"``: an ``update`` event — for soft-delete models where
              the DELETE was rewritten into an UPDATE and the row survives.
            - ``None``: nothing; no ``post_delete`` receiver is registered. Use
              this only when the delete genuinely has nothing to record — see
              the warning below, it is *not* the answer for a DB-level soft
              delete.
        delete_to_dataclass: Optional transformer used *instead of*
            ``to_dataclass`` for the delete signal. Django hands ``post_delete``
            the pre-delete in-memory instance, so a soft-delete model needs this
            to describe the state the row actually ended up in. Requires
            ``on_delete`` to be set.

    Soft-delete models (issue #80):
        With ``pgtrigger.SoftDelete`` a ``DELETE`` becomes ``UPDATE
        is_active=false`` and the row survives, but Django still fires
        ``post_delete``. The default would record a hard delete that never
        happened, snapshotting the stale ``is_active=True`` state.

        Use ``on_delete="update"`` with a ``delete_to_dataclass`` that reports
        the state the row ended up in. **Do not use** ``on_delete=None`` here:
        the flip is performed by a ``BEFORE DELETE`` trigger inside the
        database, so Django only ever issued a ``DELETE`` and ``post_save``
        never fires. Suppressing ``post_delete`` drops the soft delete from the
        stream entirely, and a projection replayed from it shows the row active
        forever — strictly worse than the phantom hard delete.

        ``on_delete=None`` is for models that soft-delete in *Python* (a
        ``delete()`` override that calls ``save()``, so the flip already
        arrives as an ordinary ``post_save`` update), or for models whose
        deletes are simply not worth streaming.

    Fixtures (issue #80):
        Saves made with ``raw=True`` — ``manage.py loaddata``, test databases
        restored via ``serialized_rollback`` — are ignored. Fixture rows are
        replayed history, not new facts, and a raw instance may reference FK
        rows that have not been loaded yet.

    Example (single stream):
        @stream_model(
            stream_paths=lambda obj: f"user:{obj.id}:updates",
            to_dataclass=lambda obj: UserData(id=obj.id, name=obj.name),
        )
        class User(models.Model):
            name = models.CharField(max_length=100)

    Example (multiple streams):
        @stream_model(
            stream_paths=lambda obj: [
                f"user:{obj.created_by_id}:projects",
                f"area:{obj.area_id}:projects",
            ],
            to_dataclass=lambda obj: ProjectData(id=obj.id, name=obj.name),
        )
        class Project(models.Model):
            name = models.CharField(max_length=100)
            area = models.ForeignKey(Area, on_delete=models.CASCADE)
            created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    """

    if on_delete not in ("delete", "update", None):
        raise ValueError(
            f"on_delete must be 'delete', 'update', or None; got {on_delete!r}"
        )
    if delete_to_dataclass is not None and on_delete is None:
        raise ValueError(
            "delete_to_dataclass is set but on_delete=None, so no delete event "
            "is ever emitted. Drop one of the two."
        )

    def decorator(model_cls: type[models.Model]) -> type[models.Model]:
        streamed_models.append(model_cls)

        @receiver(post_save, sender=model_cls, weak=False)
        def handle_post_save(
            sender: type[models.Model],  # noqa: ARG001
            instance: models.Model,
            created: bool,
            signal: Any,  # noqa: ARG001
            **kwargs: Any,
        ) -> None:
            # `raw=True` means a fixture load (loaddata, serialized_rollback):
            # replayed history, not a new fact, and the instance may reference
            # FK rows that are not loaded yet. See issue #80.
            if kwargs.get("raw"):
                return
            action = "create" if created else "update"
            # Django tells the receiver which database the save went to. That
            # was dropped here, so a save routed elsewhere recorded its event on
            # the default database instead (#159).
            create_stream_event(
                stream_paths,
                to_dataclass,
                instance,
                action,
                using=kwargs.get("using"),
            )

        if on_delete is not None:
            delete_transformer = delete_to_dataclass or to_dataclass
            # Bound inside the `if` so the closure closes over an already-narrowed
            # `str`; type checkers do not carry the outer narrowing into a nested
            # function body.
            delete_event_type: str = on_delete

            @receiver(post_delete, sender=model_cls, weak=False)
            def handle_post_delete(
                sender: type[models.Model],  # noqa: ARG001
                instance: models.Model,
                signal: Any,  # noqa: ARG001
                **kwargs: Any,
            ) -> None:
                # Same alias forwarding as the save path: a delete routed to
                # another database records its event there too (#159).
                create_stream_event(
                    stream_paths,
                    delete_transformer,
                    instance,
                    delete_event_type,
                    using=kwargs.get("using"),
                )

        return model_cls

    return decorator
