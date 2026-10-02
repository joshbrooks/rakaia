"""
Django Integration for Rakaia Durable Streams.

This module provides Django-specific integration for the Durable Streams protocol,
allowing model changes (save/delete) to automatically emit stream events.

Usage:
    from django_rakaia.decorators import stream_model, create_stream_event

    @stream_model(
        stream_paths=lambda obj: f"user:{obj.id}:updates",
        to_dataclass=lambda obj: MyData(id=obj.id, name=obj.name),
    )
    class MyModel(models.Model):
        name = models.CharField(max_length=100)

    # Or use create_stream_event for built-in models. A hand-wired receiver
    # does not get `@stream_model`'s `raw` guard — write it yourself, or every
    # `loaddata` row appends a phantom event (issue #80).
    @receiver(post_save, sender=User)
    def handle_user_save(sender, instance, created, **kwargs):
        if kwargs.get("raw"):
            return
        create_stream_event(
            stream_paths=f"user:{instance.id}:activity",
            to_dataclass=to_user_data,
            instance=instance,
            action="create" if created else "update",
        )

Public API:
    - stream_model: Decorator for automatically streaming model changes
    - create_stream_event: Helper function for manually creating stream events
    - register_stream_event_admin: Register stream event model with Django admin
    - diff_effects_against_rows: Verify replayed effects reproduce the projection
      (import from django_rakaia.verification — see that module; not eagerly
      imported here because it pulls in the ORM before apps are ready)
"""

from typing import TYPE_CHECKING

default_app_config = "django_rakaia.apps.DjangoRakaiaConfig"

# =============================================================================
# Public API
# =============================================================================
#
# Resolved lazily (PEP 562). Eager imports here would pull the ORM in at package
# import time and raise `AppRegistryNotReady` during Django's own startup, which
# is why this package exported nothing at all for so long — and why every
# consumer import was forced to name a submodule, pinning the module *layout*
# rather than the surface.
#
# Lazy resolution gets both: `from django_rakaia import DjangoExecutor` works,
# and nothing is imported until the name is actually touched. Importing the
# package stays free.
#
# See `docs/public-api.md` for what these guarantees mean and what they exclude.

#: name -> the module that defines it.
_EXPORTS: dict[str, str] = {
    # -- emitting events ----------------------------------------------------
    "stream_model": "django_rakaia.decorators",
    "create_stream_event": "django_rakaia.decorators",
    "append_event": "django_rakaia.envelope",
    "fold_events": "django_rakaia.envelope",
    "SCRATCH_PATH": "django_rakaia.envelope",
    # -- stores -------------------------------------------------------------
    "get_store": "django_rakaia.store",
    "reset_store_cache": "django_rakaia.store",
    "DjangoStreamStore": "django_rakaia.django_store",
    # -- replaying ----------------------------------------------------------
    "DjangoExecutor": "django_rakaia.effect_executor",
    "DjangoProjectionReader": "django_rakaia.projection_reader",
    "replay_stream": "django_rakaia.replay",
    # -- verifying a rebuild -------------------------------------------------
    "diff_effects_against_rows": "django_rakaia.verification",
    "PreloadedProjectionReader": "django_rakaia.verification",
    "PreloadMismatch": "django_rakaia.verification",
    "DiffReport": "django_rakaia.verification",
    "RowDiff": "django_rakaia.verification",
    "FieldDiff": "django_rakaia.verification",
    "VerificationError": "django_rakaia.verification",
    "VacuousVerification": "django_rakaia.verification",
    # The value-equality rule resolves to its own module, not the verify path:
    # both the verify and the write path depend on it and neither owns it (#160).
    # The exported *names* are unchanged (Tier 1); which module defines them is
    # not, and `django_rakaia.verification` still re-exports both.
    "canonical_value": "django_rakaia.canonicalisation",
    "DEFAULT_NORMALIZERS": "django_rakaia.canonicalisation",
    # `Normalizer` is exported because `DjangoExecutor(normalizers=...)` and
    # `diff_effects_against_rows(normalizers=...)` are both public and both take
    # a sequence of them, so a consumer writing its own could name the parameter
    # type only by importing from a submodule. Additive: a new Tier 1 name, not
    # a changed one. The three concrete `normalize_*` functions stay unexported —
    # `DEFAULT_NORMALIZERS` already lets a consumer extend the set
    # (`(*DEFAULT_NORMALIZERS, mine)`) without naming them individually.
    "Normalizer": "django_rakaia.canonicalisation",
    "GREEN": "django_rakaia.verification",
    "RED": "django_rakaia.verification",
    "VACUOUS": "django_rakaia.verification",
    # -- rebuild isolation ---------------------------------------------------
    # `rebuild_and_verify` composes everything in this block with the verify
    # block above (#184). The pieces stay exported: it is the shortest route to
    # a trustworthy answer, not the only permitted one, and a consumer with a
    # different question — a partial replay, a merge, a rebuild it wants to keep
    # — still needs the guards on their own.
    "rebuild_and_verify": "django_rakaia.rebuild",
    "GuardNotArmed": "django_rakaia.rebuild",
    "ScratchAliasNotEmpty": "django_rakaia.rebuild",
    "deny_database_access": "django_rakaia.hermeticity",
    "assert_no_live_writes": "django_rakaia.hermeticity",
    "AmbientDatabaseAccess": "django_rakaia.hermeticity",
    "LiveWriteLeaked": "django_rakaia.hermeticity",
    # -- checking a stream against its table ------------------------------
    "stream_coverage": "django_rakaia.coverage",
    "StreamCoverage": "django_rakaia.coverage",
    # -- reading -------------------------------------------------------------
    "ModelStreamReader": "django_rakaia.streams",
    "materialize_history": "django_rakaia.history",
    # -- subscriber cursors --------------------------------------------------
    "poll_consumer": "django_rakaia.subscription",
    "load_cursor": "django_rakaia.subscription",
    "commit_cursor": "django_rakaia.subscription",
    # -- what happened to an event a consumer could not apply -----------------
    # The store, not the model. `ConsumerOutcome` stays Tier 2 and out of `__all__`
    # with the other models — see `docs/public-api.md`.
    "DjangoOutcomeStore": "django_rakaia.outcomes",
    # -- consuming a stream, durably -----------------------------------------
    # The factory is the entry point; the class and the cursor store are named
    # they are what it returns and what it wires in, and the error because a
    # caller catching it must be able to spell it without a submodule import.
    "django_consumer": "django_rakaia.consumer",
    "DjangoConsumer": "django_rakaia.consumer",
    "DjangoConsumerCursorStore": "django_rakaia.consumer",
    "CallerTransactionOpen": "django_rakaia.consumer",
    # -- mounting ------------------------------------------------------------
    "get_asgi_app": "django_rakaia.integration",
    "register_stream_event_admin": "django_rakaia.admin",
    "ProvenanceMiddleware": "django_rakaia.middleware",
}

if TYPE_CHECKING:
    # For type checkers only: the runtime path is `__getattr__` below, which they
    # cannot follow. Without this every name reads as `Any` to a consumer.
    from .admin import register_stream_event_admin
    from .canonicalisation import DEFAULT_NORMALIZERS, Normalizer, canonical_value
    from .consumer import (
        CallerTransactionOpen,
        DjangoConsumer,
        DjangoConsumerCursorStore,
        django_consumer,
    )
    from .coverage import StreamCoverage, stream_coverage
    from .decorators import create_stream_event, stream_model
    from .django_store import DjangoStreamStore
    from .effect_executor import DjangoExecutor
    from .envelope import SCRATCH_PATH, append_event, fold_events
    from .hermeticity import (
        AmbientDatabaseAccess,
        LiveWriteLeaked,
        assert_no_live_writes,
        deny_database_access,
    )
    from .history import materialize_history
    from .integration import get_asgi_app
    from .middleware import ProvenanceMiddleware
    from .outcomes import DjangoOutcomeStore
    from .projection_reader import DjangoProjectionReader
    from .rebuild import GuardNotArmed, ScratchAliasNotEmpty, rebuild_and_verify
    from .replay import replay_stream
    from .store import get_store, reset_store_cache
    from .streams import ModelStreamReader
    from .subscription import commit_cursor, load_cursor, poll_consumer
    from .verification import (
        GREEN,
        RED,
        VACUOUS,
        DiffReport,
        FieldDiff,
        PreloadedProjectionReader,
        PreloadMismatch,
        RowDiff,
        VacuousVerification,
        VerificationError,
        diff_effects_against_rows,
    )

# Written out rather than computed from `_EXPORTS`, for the same reason as
# rakaia's: a type checker only believes a literal list.
__all__ = [
    "AmbientDatabaseAccess",
    "CallerTransactionOpen",
    "DEFAULT_NORMALIZERS",
    "DiffReport",
    "DjangoConsumer",
    "DjangoConsumerCursorStore",
    "DjangoExecutor",
    "DjangoOutcomeStore",
    "DjangoProjectionReader",
    "DjangoStreamStore",
    "FieldDiff",
    "GREEN",
    "GuardNotArmed",
    "LiveWriteLeaked",
    "ModelStreamReader",
    "Normalizer",
    "PreloadMismatch",
    "PreloadedProjectionReader",
    "ProvenanceMiddleware",
    "RED",
    "RowDiff",
    "SCRATCH_PATH",
    "ScratchAliasNotEmpty",
    "StreamCoverage",
    "VACUOUS",
    "VacuousVerification",
    "VerificationError",
    "append_event",
    "assert_no_live_writes",
    "canonical_value",
    "commit_cursor",
    "create_stream_event",
    "deny_database_access",
    "diff_effects_against_rows",
    "django_consumer",
    "fold_events",
    "get_asgi_app",
    "get_store",
    "load_cursor",
    "materialize_history",
    "poll_consumer",
    "rebuild_and_verify",
    "register_stream_event_admin",
    "replay_stream",
    "reset_store_cache",
    "stream_coverage",
    "stream_model",
]


def __getattr__(name: str):
    """Resolve a public name on first use (PEP 562).

    Deliberately does **not** cover the ORM models. They are usable and
    documented, but at a weaker stability tier than this surface — see
    `docs/public-api.md`. Import them from `django_rakaia.models`, which makes
    the weaker guarantee visible at the import site rather than hiding it among
    the stable names.
    """
    module_path = _EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    return getattr(import_module(module_path), name)


def __dir__() -> list[str]:
    return sorted([*__all__, *globals()])
