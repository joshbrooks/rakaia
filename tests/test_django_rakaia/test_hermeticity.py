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
# access from an async context. Those two facts were in tension, and the guard
# lost: work crossing the hop was invisible to it, so the gate reported green
# without having checked anything.
#
# Fixed in #147 by having `run_sync` re-arm the caller's guard on the worker
# thread (`hermeticity.armed_deny_aliases`). These tests hold that fix in place
# from both sides of the boundary.


@pytest.mark.django_db(transaction=True)
async def test_deny_database_access_blocks_a_write_through_run_sync():
    """The guard survives the thread hop `run_sync` makes.

    `deny_database_access` installs an `execute_wrapper` on
    `connections["default"]`, and that connection object is thread-local, so
    the worker thread `run_sync` dispatches to resolves `connections["default"]`
    to a *different* connection. Before #147 that connection had an empty
    wrapper list and the append was executed, committed, and never seen.

    `run_sync` now asks `armed_deny_aliases()` what the caller is guarding and
    re-arms it inside the worker, so the guard means the same thing on both
    sides. This test fails if that propagation is removed.
    """
    from django_rakaia.django_store import DjangoStreamStore
    from django_rakaia.models import StreamEntry

    store = DjangoStreamStore()
    await store.run_sync(store.create, "guarded")

    with deny_database_access("default"), pytest.raises(AmbientDatabaseAccess):
        await store.run_sync(store.append, "guarded", b'{"x": 1}')

    # The guard must *prevent*, not merely report: nothing may have landed.
    assert await StreamEntry.objects.filter(stream__stream_id="guarded").acount() == 0


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
async def test_run_sync_guard_does_not_block_an_unguarded_alias():
    """Propagation must carry the guard, not freeze the process.

    The counterpart to the test above, and the one that would catch the fix
    over-reaching. `DjangoStreamStore` has no `using=` — it always works on the
    default alias — so the case to prove is the mirror: guarding the *overlay*
    alias must leave the store's default-alias work running normally across the
    same thread hop. If propagation froze the worker outright rather than
    carrying the specific guard, this would fail and the guard would be
    unusable rather than merely leaky.
    """
    from django_rakaia.django_store import DjangoStreamStore
    from django_rakaia.models import StreamEntry

    store = DjangoStreamStore()
    await store.run_sync(store.create, "unguarded")

    with deny_database_access("overlay"):
        await store.run_sync(store.append, "unguarded", b'{"x": 1}')

    landed = await StreamEntry.objects.filter(stream__stream_id="unguarded").acount()
    assert landed == 1


@pytest.mark.django_db(transaction=True)
def test_deny_database_access_does_hold_on_the_calling_thread():
    """The control for the test above: same guard, no thread hop, blocks."""
    from django_rakaia.django_store import DjangoStreamStore

    store = DjangoStreamStore()
    store.create("guarded-sync")

    with deny_database_access("default"), pytest.raises(AmbientDatabaseAccess):
        store.append("guarded-sync", b'{"x": 1}')
