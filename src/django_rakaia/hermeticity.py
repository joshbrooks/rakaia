"""The two halves of a rebuild gate: a from-scratch replay must neither **read**
nor **write** the live database it is proving it can reconstruct.

* :func:`deny_database_access` — the *read* guard. Handlers must read only
  through the injected reader (the alias under rebuild), never an ambient
  default-DB manager.
* :func:`assert_no_live_writes` — the *write* guard. The live database's row
  counts must be unchanged across the rebuild.

Together they make "the log alone rebuilt this" a checkable property rather than
a convention a reviewer has to remember. Neither subsumes the other: a ``SELECT``
changes no row counts, and a row count says nothing about what was read. They
also differ in where they can be armed — see `assert_no_live_writes` for why the
write guard is the one you can wrap a whole rebuild in.

**Why reads matter as much as writes.** A pure rakaia handler is
``event -> Effect``; a stage > 0 handler is ``event, reader -> Effect``. Every
fact it needs comes from the event or the ``reader`` (which carries ``using=``).
A handler — or a helper it calls — that instead reaches for
``SomeModel.objects...`` binds to the **default** connection, which:

* breaks replay determinism: the output depends on live DB state, not the log; and
* defeats disposable-DB verification: the rebuild silently consults production,
  so a green gate no longer means "reconstructed from the log alone".

Such a read is invisible to the write-side guard (a ``SELECT`` changes no row
counts), so it needs its own gate.

Usage — the write guard around the whole rebuild, the read guard around the
handler-dispatch region inside it::

    from rakaia.store import StreamStore
    from django_rakaia.hermeticity import (
        assert_no_live_writes,
        deny_database_access,
    )

    with assert_no_live_writes(Submission, ProjectLink):
        with deny_database_access("default"):
            replay(store, path, DjangoExecutor(using="rebuild"),
                   reader=DjangoProjectionReader(using="rebuild"), ...)

**The event source must not read the denied alias either.** Read the log from a
store that does not touch it — an in-memory ``StreamStore``, or a store on
another alias. A ``DjangoStreamStore`` on ``default`` would itself trip the
guard, which is correct: a truly hermetic rebuild reads its log from somewhere
other than the database it is proving it can reconstruct.

That is the whole obstacle in practice, and it is six lines to clear. Drain the
durable log into memory *first*, with the guard down, then arm it around the
replay alone::

    durable = get_store()                       # DjangoStreamStore on "default"
    mem = StreamStore()
    mem.create(path)
    messages, _has_more = durable.read(path)
    for m in messages:
        # m.data is already the encoded bytes — re-encoding double-wraps it.
        mem.append(path, m.data,
                   AppendOptions(label=m.label, metadata=m.metadata,
                                 event_ts=m.event_ts))

    with deny_database_access("default"):
        replay(mem, path, DjangoExecutor(using="rebuild"),
               reader=DjangoProjectionReader(using="rebuild"), ...)

Worth stating because the first production consumer left this guard unwired for
months, having recorded "a blanket deny on ``default`` trips on the store's own
reads" as the blocker. It is not a blocker; it is the drain above.

**A pass only means something if you have watched the guard fail.** Nothing here
distinguishes "no handler read the database" from "the guard was never armed",
so pair the run with a deliberate ambient read and check that it raises::

    with deny_database_access("default"):
        SomeModel.objects.filter(pk=1).exists()   # must raise AmbientDatabaseAccess

See ADR 0003 (``docs/adr/0003-handler-hermeticity.md``).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:  # pragma: no cover - typing only
    from django.db.models import Model


class AmbientDatabaseAccess(RuntimeError):
    """A guarded connection alias was queried inside ``deny_database_access`` —
    a handler (or a helper it calls) read the database directly instead of
    through the injected reader."""


class LiveWriteLeaked(RuntimeError):
    """A guarded model's row count changed inside ``assert_no_live_writes`` — a
    rebuild mutated the live database it was only supposed to reconstruct."""


def _preview(sql: str, limit: int = 120) -> str:
    """A single-line, length-bounded preview of the offending SQL for the error."""
    flat = " ".join(sql.split())
    return flat if len(flat) <= limit else flat[:limit] + "…"


@contextmanager
def deny_database_access(*aliases: str) -> Iterator[None]:
    """Raise :class:`AmbientDatabaseAccess` on any query to ``aliases`` in the block.

    Installs a Django ``execute_wrapper`` on each aliased connection, so it
    catches **every** statement the ORM issues on those aliases — a SELECT,
    INSERT, UPDATE or DELETE, including one buried in a handler helper — before
    it reaches the database. Called with no aliases it is a no-op.

    This is the read-side complement of the Partisipa rebuild gate's
    ``assert_no_live_writes`` (which counts rows). Use it around the
    handler-dispatch region of a from-scratch rebuild to make an ambient
    default-DB read a loud gate failure instead of a silent determinism leak.
    """
    from django.db import connections

    def _blocker(alias: str):
        # Django calls the wrapper positionally as
        # (execute, sql, params, many, context); only `sql` is used here.
        def _wrap(_execute, sql, _params, _many, _context):  # noqa: ANN001
            raise AmbientDatabaseAccess(
                f"replay touched the {alias!r} database directly "
                f"(SQL: {_preview(sql)}). A hermetic rebuild reads only through "
                f"the injected reader/executor alias — route this read through "
                f"the reader, or move it to a stage that has one."
            )

        # Tagged so `assert_no_live_writes` can suspend *this* wrapper for its
        # own row counts without disturbing a wrapper the consumer installed.
        _wrap._rakaia_deny_guard = True  # type: ignore[attr-defined]
        return _wrap

    with ExitStack() as stack:
        for alias in aliases:
            # `execute_wrapper` is a `@contextmanager` at runtime, but the
            # django-types stub annotates the *undecorated* generator, so it
            # advertises `Iterator[None]`. Say what the call actually returns
            # rather than suppress the mismatch.
            stack.enter_context(
                cast(
                    AbstractContextManager[None],
                    connections[alias].execute_wrapper(_blocker(alias)),
                )
            )
        yield


def armed_deny_aliases() -> tuple[str, ...]:
    """The aliases `deny_database_access` is currently guarding **on this thread**.

    Django keeps connections in a thread-local, so the wrapper
    `deny_database_access` installs is only visible to the thread that armed it.
    Anything that hands ORM work to another thread has to carry the guard across
    itself, and this is how it asks what to carry — see
    `DjangoStreamStore.run_sync`, the one such hop in this package.

    Reading `execute_wrappers` does not open a connection: `connections[alias]`
    returns the wrapper object, and the socket is opened lazily on first query.
    """
    from django.db import connections

    return tuple(
        alias
        for alias in connections
        if any(
            getattr(w, "_rakaia_deny_guard", False)
            for w in connections[alias].execute_wrappers
        )
    )


@contextmanager
def _without_deny_guards(alias: str) -> Iterator[None]:
    """Suspend rakaia's own `deny_database_access` wrappers on `alias`.

    Counting rows to *check* for a leak is the guard's bookkeeping, not the
    rebuild reading live data — so it must not be reported as a leak, and the
    two guards must compose in either nesting order (#101). Without this,
    `deny_database_access` on the outside made `assert_no_live_writes`'s closing
    `COUNT(*)` raise `AmbientDatabaseAccess` blaming *the rebuild* for a query
    the guard itself issued, sending the reader after a leak that did not exist.

    Only wrappers this module installed are removed — a consumer's own
    `execute_wrapper` (query logging, timing) still sees these counts, since
    suppressing it would be a surprise we have no business causing.
    """
    from django.db import connections

    conn = connections[alias]
    saved = list(conn.execute_wrappers)
    conn.execute_wrappers[:] = [
        w for w in saved if not getattr(w, "_rakaia_deny_guard", False)
    ]
    try:
        yield
    finally:
        conn.execute_wrappers[:] = saved


@contextmanager
def assert_no_live_writes(
    *models: type[Model], using: str = "default"
) -> Iterator[None]:
    """Assert the ``using`` row counts of ``models`` are unchanged across the block.

    The write-side mirror of :func:`deny_database_access`. Raises
    :class:`LiveWriteLeaked` naming the drift if a write leaked — a rebuild that
    mutates the database it is proving it can reconstruct is a defect, not a
    warning. Called with no models it is a no-op.

    Wrap the **whole** rebuild::

        with assert_no_live_writes(Submission, ProjectLink):
            replay(store, path, DjangoExecutor(using="rebuild"),
                   reader=DjangoProjectionReader(using="rebuild"), ...)

    Composes with :func:`deny_database_access` in **either** nesting order: this
    guard's own row counts are taken with rakaia's deny wrappers suspended, so
    its bookkeeping is never mistaken for the rebuild reading live data (#101).

    **Why this exists alongside the read-side guard.** `deny_database_access`
    installs a statement wrapper, so it can only be armed around a region where
    *no* legitimate query to the alias happens — and a rebuild often cannot meet
    that bar, because the event log itself may live on the alias being guarded
    (the constraint noted in this module's docstring). Counting rows tolerates
    arbitrary reads and still catches a mutation, so this is the guard you can
    put around everything. Use both where you can: they catch different leaks,
    and a ``SELECT`` is invisible here just as a row count is invisible there.

    **The leak it is built for.** Postgres' ``session_replication_role =
    replica`` disables *triggers* but **not** Django ``post_save``/``pre_save``
    receivers, so a receiver that saves without a ``using=`` writes to the live
    alias from inside a rebuild — silently, while the gate reports green.
    Suppressing the receiver prevents it; this *verifies* the prevention held,
    which is the part a reviewer cannot be expected to remember.

    **What it does not catch.** It compares counts, so an in-place ``UPDATE`` of
    an existing row leaves it silent. Counts are what a rebuild leak actually
    looks like (a receiver minting rows), they are cheap enough to wrap a whole
    replay, and they need no per-model knowledge of which columns matter. For
    field-level assurance, diff the projection afterwards with
    :func:`django_rakaia.verification.diff_effects_against_rows`.
    """
    if not models:
        yield
        return

    def _counts() -> dict[type[Model], int]:
        with _without_deny_guards(using):
            return {m: m.objects.using(using).count() for m in models}

    before = _counts()
    try:
        yield
    finally:
        # `finally`, not `else`: a rebuild that raised half-way must still be
        # held to the invariant — a leak that happened before the failure is
        # exactly the kind that would otherwise go unnoticed.
        drift = {
            m.__name__: (before[m], after)
            for m, after in _counts().items()
            if after != before[m]
        }
        if drift:
            raise LiveWriteLeaked(
                f"rebuild isolation VIOLATED — the {using!r} database changed "
                f"during the block: {drift} (model: before -> after). A rebuild "
                f"must write only to its disposable alias. Something wrote "
                f"without a `using=` — most often a post_save/pre_save receiver, "
                f"which `session_replication_role = replica` does not disable."
            )
