"""P1 — handler hermeticity: a replay's handlers read only through the injected
reader, never an ambient default-DB manager.

`deny_database_access("default")` is the read-side mirror of the Partisipa
rebuild gate's `assert_no_live_writes`. These tests prove it (a) lets a pure
from-scratch rebuild into the disposable `overlay` DB run without touching
`default`, and (b) turns an ambient `Model.objects` read inside a handler into a
loud `AmbientDatabaseAccess` — the leak that would otherwise make a green gate
lie. See ADR 0003.
"""

from __future__ import annotations

import pytest

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.hermeticity import AmbientDatabaseAccess, deny_database_access
from django_rakaia.projection_reader import DjangoProjectionReader
from rakaia.effects import Upsert
from rakaia.registry import HandlerRegistry, UpcasterRegistry
from rakaia.replay import replay
from rakaia.seed import seed_stream
from rakaia.store import StreamStore

# A *plain* model (no `@stream_model`): saving it fires no post_save append, so
# the only default-DB access a rebuild can make is a handler's own ambient read
# — exactly what these tests are isolating.
from .models import FinanceLine

pytestmark = pytest.mark.django_db(databases=["default", "overlay"])


def _pure_handler(event):
    """A well-behaved projection: a pure function of the event."""
    return Upsert(
        model_label="test_django_rakaia.FinanceLine",
        lookup={"submission_id": event["name"]},
        defaults={"suku": "s"},
    )


def _impure_handler(event):
    """The anti-pattern (finding #1): an ambient read on the *default* manager
    mid-projection instead of going through the injected reader."""
    FinanceLine.objects.filter(
        submission_id=event["name"]
    ).exists()  # ambient default read
    return _pure_handler(event)


def _mem_store(events: list[dict]) -> StreamStore:
    """An in-memory log so the *event source* never touches a guarded alias —
    only the handlers' own reads can trip the guard."""
    return seed_stream("s", events)


def _replay(store: StreamStore, registry: HandlerRegistry) -> None:
    replay(
        store,
        "s",
        DjangoExecutor(using="overlay"),
        handler_registry=registry,
        upcaster_registry=UpcasterRegistry(),
        reader=DjangoProjectionReader(using="overlay"),
    )


class TestDenyDatabaseAccess:
    def test_pure_rebuild_never_touches_default(self):
        store = _mem_store([{"name": "Alpha"}, {"name": "Beta"}])
        reg = HandlerRegistry()
        reg.register("pure", "s", _pure_handler, 0, None)

        with deny_database_access("default"):
            _replay(store, reg)

        # Rebuilt in the disposable alias; production (default) never read.
        assert (
            FinanceLine.objects.using("overlay").filter(submission_id="Alpha").exists()
        )
        assert FinanceLine.objects.using("default").count() == 0

    def test_ambient_default_read_in_handler_is_caught(self):
        store = _mem_store([{"name": "Gamma"}])
        reg = HandlerRegistry()
        reg.register("impure", "s", _impure_handler, 0, None)

        with (
            pytest.raises(AmbientDatabaseAccess, match="default"),
            deny_database_access("default"),
        ):
            _replay(store, reg)

    def test_guard_is_scoped_to_named_aliases(self):
        # Reads to a *non*-denied alias pass straight through — the guard is not
        # a blanket freeze, so the rebuild's own overlay reads/writes proceed.
        store = _mem_store([{"name": "Delta"}])
        reg = HandlerRegistry()
        reg.register("pure", "s", _pure_handler, 0, None)

        with deny_database_access("default"):
            _replay(store, reg)
            assert (
                FinanceLine.objects.using("overlay")
                .filter(submission_id="Delta")
                .exists()
            )

    def test_no_aliases_is_a_noop(self):
        store = _mem_store([{"name": "Epsilon"}])
        reg = HandlerRegistry()
        reg.register("impure", "s", _impure_handler, 0, None)
        # With nothing denied, even the ambient read is allowed.
        with deny_database_access():
            _replay(store, reg)
        assert (
            FinanceLine.objects.using("overlay")
            .filter(submission_id="Epsilon")
            .exists()
        )


# ---------------------------------------------------------------------------
# Does the guard survive the thread hop in DjangoStreamStore.run_sync?
# ---------------------------------------------------------------------------
#
# `deny_database_access` works by installing an `execute_wrapper` on a
# connection. Django stores connections in a thread-local
# (`ConnectionHandler` uses `asgiref.local.Local(thread_critical=True)`), so a
# different thread gets a *different* connection object with its own, empty,
# `execute_wrappers` list.
#
# `DjangoStreamStore.run_sync` deliberately moves ORM work onto another thread
# (`sync_to_async(..., thread_sensitive=True)`), because Django refuses ORM
# access from an async context. Those two facts are in tension, and the tests
# below record which one wins rather than assuming.


@pytest.mark.xfail(
    strict=True,
    reason=(
        "KNOWN GAP: deny_database_access is thread-local, so a write issued "
        "through DjangoStreamStore.run_sync steps straight past it and the "
        "gate stays green. Left failing on purpose -- fixing it means changing "
        "the guard, not the test. See the note above."
    ),
)
@pytest.mark.django_db(transaction=True)
async def test_deny_database_access_should_block_a_write_through_run_sync():
    """What the guard would have to do to be safe around async store work.

    It does not do it today. `deny_database_access` installs an
    `execute_wrapper` on `connections["default"]`, and that connection object
    is thread-local; `run_sync` hands the ORM call to another thread, which
    resolves `connections["default"]` to a *different* connection with an
    empty wrapper list. The append is executed, committed, and never seen.

    Scope: the guard is documented for a *synchronous* replay -- `with
    deny_database_access("default"): replay(...)` -- which is how every
    example and every other test uses it, and where it does hold (see the
    control test below). Nothing in the codebase arms it around `await`-ed
    store work today, so this is a sharp edge in the guard's contract rather
    than a live hole in the rebuild gate. It becomes a real hole the moment
    someone wraps an async rebuild in it, and it fails silently when they do,
    which is the dangerous shape.
    """
    from django_rakaia.django_store import DjangoStreamStore
    from django_rakaia.models import StreamEntry

    store = DjangoStreamStore()
    await store.run_sync(store.create, "guarded")

    with deny_database_access("default"), pytest.raises(AmbientDatabaseAccess):
        await store.run_sync(store.append, "guarded", b'{"x": 1}')

    # Reached only if the guard did not raise: the write really landed, so the
    # leak is a completed write and not merely a query that dodged the wrapper.
    assert await StreamEntry.objects.filter(stream__stream_id="guarded").acount() == 0


@pytest.mark.django_db(transaction=True)
def test_deny_database_access_does_hold_on_the_calling_thread():
    """The control for the test above: same guard, no thread hop, blocks."""
    from django_rakaia.django_store import DjangoStreamStore

    store = DjangoStreamStore()
    store.create("guarded-sync")

    with deny_database_access("default"), pytest.raises(AmbientDatabaseAccess):
        store.append("guarded-sync", b'{"x": 1}')
