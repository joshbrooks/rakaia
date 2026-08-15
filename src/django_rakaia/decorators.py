"""
Django decorators for streaming model changes to stream events.

Supports multi-stream events where a single event can appear in multiple
streams with independent, monotonic offsets per stream.
"""

import dataclasses
import json
import time
from collections.abc import Callable
from typing import Any, Literal

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models, transaction
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from django_rakaia.django_store import write_enveloped_event
from django_rakaia.models import Stream, StreamEvent

StreamPathResolver = (
    str
    | Callable[[models.Model], str]
    | list[str]
    | Callable[[models.Model], list[str]]
)
DataclassTransformer = Callable[[models.Model], Any]
DeleteEventType = Literal["delete", "update"] | None


def _get_or_create_stream(stream_id: str) -> Stream:
    """Get or create a Stream record."""
    stream, _ = Stream.objects.get_or_create(stream_id=stream_id)
    return stream


def create_stream_event(
    stream_paths: str | list[str] | Callable[[models.Model], str | list[str]],
    to_dataclass: DataclassTransformer,
    instance: models.Model,
    action: str,
) -> StreamEvent:
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

    Returns:
        The created StreamEvent instance.

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

    # `event_ts` is the deterministic merge key (ADR 0002 item 5), and this door
    # always stamps one: leaving it NULL would force `merge_replay` onto
    # transport time, which is the ordering trap that item closed. A raw
    # protocol append is the other case — no logical time unless the producer
    # set one — which is why the shared writer takes it as an argument rather
    # than deciding it.
    #
    # Everything else about the envelope — the `"append"` sentinel, merging the
    # ambient `provenance()` block into `metadata`, `{}` rather than NULL,
    # locking offset allocation — is `write_enveloped_event`'s to decide, shared
    # with `DjangoStreamStore.append`. `@stream_model` used to have its own copy
    # of those rules and the copies had drifted (#131).
    event_ts = time.time()

    # Atomic because `get_next_offset` locks the per-path high-water for the
    # duration of the transaction. A savepoint when the caller already has one
    # open, so the event stays inside the save that produced it.
    with transaction.atomic():
        # One envelope shared by every entry: a fan-out is one event appearing
        # in several streams, not several events that look alike.
        event, _entries = write_enveloped_event(
            [_get_or_create_stream(stream_id) for stream_id in stream_ids],
            payload,
            label=action,
            event_ts=event_ts,
        )

    return event


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
            create_stream_event(stream_paths, to_dataclass, instance, action)

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
                **kwargs: Any,  # noqa: ARG001
            ) -> None:
                create_stream_event(
                    stream_paths, delete_transformer, instance, delete_event_type
                )

        return model_cls

    return decorator
