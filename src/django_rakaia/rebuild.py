"""One call for the whole question a rebuild gate asks: **can the log
reconstruct this projection, and does the result match what production holds?**

Answering it by hand means composing six interfaces in the right order — move the
log off the guarded database, arm the write guard outside and the read guard
inside, record the effects while still applying them, replay, diff in bulk — and then separately remembering the part that is written down
nowhere: *a pass means nothing unless the guards were actually armed.*
`hermeticity.py` says so in prose and leaves it to the caller's discipline. ADR
0003 records what that discipline is worth in practice: the first production
consumer left the read guard unwired for months.

:func:`rebuild_and_verify` is that composition executed rather than described. It
arms both guards, **proves** the read guard is live by deliberately tripping it,
refuses a from-scratch claim it cannot honour, and returns a
:class:`~django_rakaia.verification.DiffReport` whose ``verdict`` distinguishes
"nothing disagreed" from "nothing was compared".

    report = rebuild_and_verify("submissions", into="rebuild",
                                live_models=[Submission, ProjectLink])
    report.raise_if_diff()   # or inspect report.problems

**What is compared against what.** The replay's effects are the log's claim;
:func:`~django_rakaia.verification.diff_effects_against_rows` checks that claim
against the **live** rows. The ``into`` alias is not the thing being diffed — it
exists so a stage > 0 handler can read what stage 0 wrote, which is a fact a
:class:`~rakaia.executors.CollectingExecutor` cannot supply. Both are needed at
once, and `ReplayResult` reports a *count* of effects applied rather than the
effects themselves, which is why the executor here is a recording tee.

See ADR 0003 (``docs/adr/0003-handler-hermeticity.md``).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from rakaia.effects import Effect, Executor
from rakaia.replay import replay
from rakaia.types import StreamMessage

from .hermeticity import (
    AmbientDatabaseAccess,
    assert_no_live_writes,
    deny_database_access,
)
from .projection_reader import DjangoProjectionReader
from .verification import DiffReport, Normalizer, diff_effects_against_rows

if TYPE_CHECKING:  # pragma: no cover - typing only
    from django.db.models import Model

    from rakaia.protocols import ReadableStore
    from rakaia.registry import HandlerRegistry, UpcasterRegistry
    from rakaia.replay import OnDriftPolicy

__all__ = [
    "GuardNotArmed",
    "ScratchAliasNotEmpty",
    "rebuild_and_verify",
]


class GuardNotArmed(RuntimeError):
    """The read guard did not fire on a deliberate ambient query, so a green
    verdict from this run would be unsupported.

    Raised rather than warned: the whole value of the gate is that a pass is
    evidence, and a pass obtained with the guard disarmed is not."""


class ScratchAliasNotEmpty(RuntimeError):
    """The ``into`` alias already holds rows for a model being rebuilt.

    A "from scratch" claim cannot be made on top of a previous run's output: a
    stage > 0 handler reading the ``into`` alias would see rows this replay did
    not produce, so the effects — and therefore the verdict — are no longer
    derived from the log alone. Nothing is deleted here; truncating the caller's
    database on the strength of a keyword argument would be the more dangerous
    of the two behaviours."""


@dataclass
class _Drained:
    """The stream's messages, held off the database being guarded.

    A one-method :class:`~rakaia.protocols.ReadableStore`, which is all
    :func:`~rakaia.replay.replay` asks of a store. Holding the messages verbatim
    — rather than re-appending them into an in-memory
    :class:`~rakaia.store.StreamStore` — keeps offsets and the whole envelope
    (label, metadata, ``event_ts``) exactly as the durable log recorded them,
    where a round trip through ``append`` would remint them.
    """

    messages: list[StreamMessage]

    def read(
        self, path: str, offset: str | None = None
    ) -> tuple[list[StreamMessage], bool]:
        # `replay` reads the whole stream once, from the start, and the path was
        # fixed when the drain happened; both arguments are part of the protocol
        # rather than of this use of it.
        del path, offset
        return list(self.messages), True


class _RecordingExecutor:
    """Applies through ``inner`` **and** keeps what it applied.

    Both halves are needed: applying is what lets a stage > 0 handler read what
    stage 0 wrote, and the effects are what the diff compares against the live
    rows. `ReplayResult.effects_applied` is a count, so the effects have to be
    captured on the way past.
    """

    def __init__(self, inner: Executor) -> None:
        self._inner = inner
        self.effects: list[Effect] = []

    def apply(self, effects: Iterable[Effect]):
        # Materialise once: `inner.apply` would otherwise consume a generator
        # this has already walked, and apply nothing at all.
        batch = list(effects)
        self.effects.extend(batch)
        return self._inner.apply(batch)


def _prove_read_guard_armed(alias: str) -> None:
    """Trip the read guard on purpose, and object if it does not fire.

    This is the check `hermeticity.py` asks the caller to remember. It is one
    ``SELECT`` against the rakaia stream table — a model that exists wherever
    this package is installed, so it needs nothing from the caller.

    A cheaper check is available — `armed_deny_aliases` reports which aliases
    carry a wrapper — and is deliberately not used, because this one subsumes it:
    with no wrapper the query simply succeeds, and the absence is reported all
    the same. Two checks where the weaker is unreachable behind the stronger is
    one check and some dead code.
    """
    from .models import Stream

    try:
        Stream.objects.using(alias).exists()
    except AmbientDatabaseAccess:
        return
    raise GuardNotArmed(
        f"a deliberate ambient read of {alias!r} did not raise, so the read "
        f"guard is not in force and this run would certify nothing. Either it "
        f"was never armed, or something bypassed it -- another execute_wrapper "
        f"that returns without calling through, or ORM work handed to a second "
        f"thread (Django keeps connections in a thread-local, so the guard does "
        f"not cross the hop)."
    )


def _require_empty_scratch(models: Sequence[type[Model]], into: str) -> None:
    for model in models:
        if model.objects.using(into).exists():
            raise ScratchAliasNotEmpty(
                f"{model.__name__} already has rows on the {into!r} alias, so "
                f"this would not be a from-scratch rebuild. Truncate {into!r} "
                f"first (it is meant to be disposable)."
            )


def rebuild_and_verify(
    stream_path: str,
    *,
    into: str,
    live_models: Sequence[type[Model]],
    source: ReadableStore | None = None,
    live_using: str = "default",
    registry: HandlerRegistry | None = None,
    upcaster_registry: UpcasterRegistry | None = None,
    normalizers: Sequence[Normalizer] | None = None,
    event_match: str | None = None,
    on_drift: OnDriftPolicy = "warn",
) -> DiffReport:
    """Rebuild ``stream_path`` into ``into`` under both guards and diff the result
    against the live rows.

    Args:
        stream_path: The stream to replay.
        into: A **disposable** connection alias to rebuild into. Must hold no
            rows for any of ``live_models`` (see :class:`ScratchAliasNotEmpty`).
        live_models: The models this rebuild is expected to reconstruct. They arm
            the write guard, and they are the set checked for a stale ``into``.
            Required, and deliberately so: defaulting it to empty would disarm
            half the gate without saying anything, which is the failure this
            module exists to stop. Pass ``()`` to opt out explicitly.
        source: Where to read the log. Defaults to a
            :class:`~django_rakaia.django_store.DjangoStreamStore` on
            ``live_using``. Its messages are read **before** the guards go up and
            held in memory for the replay, because a store on the guarded alias
            would trip the read guard on its own reads — correctly, since a
            hermetic rebuild must not read the database it is reconstructing.
        live_using: The alias holding production, guarded and diffed against.
        registry, upcaster_registry, event_match, on_drift: Forwarded to
            :func:`~rakaia.replay.replay`.
        normalizers: Forwarded to
            :func:`~django_rakaia.verification.diff_effects_against_rows`.

    Returns:
        The :class:`~django_rakaia.verification.DiffReport`. Read ``verdict`` or
        ``certified`` rather than ``ok``: a replay that produced no effects
        compared nothing, and ``ok`` is vacuously true for it.

    Raises:
        GuardNotArmed: the read guard did not fire on a deliberate probe.
        ScratchAliasNotEmpty: ``into`` holds rows from an earlier run.
        AmbientDatabaseAccess: a handler read the live database.
        LiveWriteLeaked: the live row counts changed during the rebuild.
    """
    from .django_store import DjangoStreamStore

    _require_empty_scratch(live_models, into)

    # Off the guarded alias first, with the guard still down — the drain is the
    # step that makes a blanket deny on `live_using` armable at all.
    log = source if source is not None else DjangoStreamStore(using=live_using)
    drained = _Drained(list(log.read(stream_path)[0]))

    recorder = _RecordingExecutor(_django_executor(into))

    # The write guard goes outside, and covers the diff as well as the replay: it
    # counts rows rather than intercepting statements, so it tolerates the diff's
    # legitimate reads of `live_using`. The read guard cannot — it fails on every
    # statement — so it covers the replay only. (The drain is outside both, by
    # necessity: it is the read that moves the log off the guarded alias.)
    with assert_no_live_writes(*live_models, using=live_using):
        with deny_database_access(live_using):
            _prove_read_guard_armed(live_using)
            replay(
                drained,
                stream_path,
                recorder,
                handler_registry=registry,
                upcaster_registry=upcaster_registry,
                event_match=event_match,
                on_drift=on_drift,
                reader=DjangoProjectionReader(using=into),
            )

        # Guard down: the diff's whole job is to read the live rows. `preload`
        # is the bulk-fetching reader — one query per lookup shape rather than
        # one per effect — built by the diff from the batch it is diffing.
        return diff_effects_against_rows(
            recorder.effects,
            preload=True,
            using=live_using,
            normalizers=normalizers,
        )


def _django_executor(into: str) -> Executor:
    from .effect_executor import DjangoExecutor

    return DjangoExecutor(using=into)
