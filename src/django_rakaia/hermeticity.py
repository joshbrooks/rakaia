"""Assert a replay is *hermetic* — its handlers read only through the injected
reader (the alias under rebuild), never an ambient default-DB manager.

The Partisipa rebuild gate already carries a *write*-side guard
(``assert_no_live_writes``): it proves a from-scratch rebuild does not **write**
the production database. This is its mirror — it proves the rebuild does not
**read** it either. Together they make "the log alone rebuilt this" a checkable
property rather than a convention a reviewer has to remember.

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

Usage — wrap the handler-dispatch region of a rebuild::

    from rakaia.store import StreamStore
    from django_rakaia.hermeticity import deny_database_access

    with deny_database_access("default"):
        replay(store, path, DjangoExecutor(using="rebuild"),
               reader=DjangoProjectionReader(using="rebuild"), ...)

**The event source must not read the denied alias either.** Read the log from a
store that does not touch it — an in-memory ``StreamStore``, or a store on
another alias. A ``DjangoStreamStore`` on ``default`` would itself trip the
guard, which is correct: a truly hermetic rebuild reads its log from somewhere
other than the database it is proving it can reconstruct.

See ADR 0003 (``docs/adr/0003-handler-hermeticity.md``).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager


class AmbientDatabaseAccess(RuntimeError):
    """A guarded connection alias was queried inside ``deny_database_access`` —
    a handler (or a helper it calls) read the database directly instead of
    through the injected reader."""


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

        return _wrap

    with ExitStack() as stack:
        for alias in aliases:
            stack.enter_context(connections[alias].execute_wrapper(_blocker(alias)))
        yield
