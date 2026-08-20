"""The two row-lock sites `test_concurrent_appends.py` does not cover, and the
"did we forget the transaction?" check for all three.

There are exactly three `select_for_update()` calls in this package:

* `models.py` — `Stream.get_next_offset_block`, the offset watermark. Raced in
  `test_concurrent_appends.py`.
* `django_store.py` — `_live_stream(..., for_update=True)`, the stream row. It
  is what makes a writer's closed / content-type / producer checks and its write
  one indivisible step.
* `effect_executor.py` — `DjangoExecutor._retire`, which captures the rows a
  retire is about to flip so it can report exactly those.

A full run touches a lock in ~290 tests, but almost all of them reach one
incidentally on their way to something else — an append allocates an offset, and
allocating locks. Locking is not what those tests are about, and converting them
all to `transaction=True` would buy nothing that the handful here does not,
while making every future run pay truncation teardown. See #148.

**Three different failures hide in the fast test mode, and they need different
tests.** Django's docs are explicit about the first two:

* *"Evaluating a queryset with select_for_update() in autocommit mode on
  backends which support SELECT ... FOR UPDATE is a TransactionManagementError
  error … If allowed, this would facilitate data corruption"* — but *"since
  TestCase automatically wraps each test in a transaction, calling
  select_for_update() in a TestCase even outside an atomic() block will (perhaps
  unexpectedly) pass"*. So a plain `django_db` test lends its own transaction to
  library code that forgot to open one. `TestOpensItsOwnTransaction` removes
  that crutch.
* A lock that is never contended proves nothing about serialisation.
  `TestStreamRowLock` and `TestRetireLock` contend it.
* The transaction has to be opened on the **right database**. A bare
  `transaction.atomic()` binds to `default`, so code pointed at another alias
  writes in autocommit — and `django_db(databases=[...])` has pytest-django open
  a transaction on *every declared alias*, which supplies the missing one just as
  readily. `TestTheTransactionIsOpenedOnTheStoresAlias` removes that crutch too;
  it is how #180 shipped an unusable alias past a full green suite.

All three need `transaction=True`. The first two also need a backend that has row
locks at all — on SQLite `select_for_update()` compiles to nothing and *"an error
isn't raised"* either, so neither failure can be observed there. Run with
`RAKAIA_TEST_DB=postgres` (the `test-postgres` CI job).

**`requires_row_locks` goes on the test, not the class.** This whole file skips on
SQLite if you mark a class with it, which would also skip the failures that *are*
visible there: an aliased write that does not roll back fails on SQLite, while a
`select_for_update` raise cannot happen at all. Marking per test keeps the
SQLite-visible cases running on the default leg.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest
from django.db import connection, connections, transaction
from django.db.models import QuerySet
from django.test.utils import CaptureQueriesContext

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.models import Stream
from rakaia.effects import Retire, Transition
from rakaia.types import AppendOptions

from .models import Alert

requires_row_locks = pytest.mark.skipif(
    not connection.features.has_select_for_update,
    reason=(
        "backend has no row locks (has_select_for_update is False), so "
        "select_for_update() compiles to a plain SELECT and neither a missing "
        "transaction nor a lost race can be observed -- run with "
        "RAKAIA_TEST_DB=postgres"
    ),
)

ALERT_MODEL = "test_django_rakaia.Alert"


def _run(*targets: Any) -> None:
    """Run each callable on its own thread and re-raise the first failure.

    Each thread gets its own database connection -- the whole point -- and each
    has to close it, or the test database cannot be torn down. Same helper as
    `test_concurrent_appends.py`; duplicated rather than shared because a
    conftest would put it in scope for the ~1300 tests that do not want it.
    """
    errors: list[BaseException] = []

    def wrap(fn: Any) -> Any:
        def inner() -> None:
            try:
                fn()
            except BaseException as exc:  # noqa: BLE001 - re-raised below
                errors.append(exc)
            finally:
                connections.close_all()

        return inner

    threads = [threading.Thread(target=wrap(t)) for t in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    for t in threads:
        assert not t.is_alive(), "a racing thread deadlocked"
    if errors:
        raise errors[0]


def _open_alert(key: str, alert_type: str = "machine_rule") -> Alert:
    return Alert.objects.create(
        stream_key=key, alert_type=alert_type, field_key="", resolved_at=None
    )


#: Set on the thread performing a retire, so `TestRetireLock._slow_update` can
#: stall that thread's UPDATE without stalling its competitor's.
_RETIRING = threading.local()

#: Distinct resolution timestamps, so "who actually resolved this row" is
#: readable from the row itself rather than inferred.
A_TS = "2026-01-01T00:00:00Z"
B_TS = "2026-06-06T00:00:00Z"


def _retire_effect(patch_ts: str, key: str = "retire-race") -> Retire:
    """An open-guarded retire that asks for one transition per row it flips."""
    return Retire(
        model_label=ALERT_MODEL,
        lookup={"stream_key": key, "resolved_at": None},
        patch={"resolved_at": patch_ts},
        transition=Transition(
            kind="alert_resolved",
            key_fields=("stream_key", "alert_type", "field_key"),
        ),
    )


# ---------------------------------------------------------------------------
# Does the library open its own transaction, or was it borrowing the test's?
# ---------------------------------------------------------------------------


@requires_row_locks
@pytest.mark.django_db(transaction=True)
class TestOpensItsOwnTransaction:
    """Each public entry point that locks must open its own transaction.

    With `transaction=True` there is no ambient transaction to inherit, so if
    the `transaction.atomic()` inside the library were removed, the
    `select_for_update()` underneath would run in autocommit and Django would
    raise `TransactionManagementError`. Under a plain `django_db` marker these
    tests would pass either way, which is exactly the blind spot.

    Three tests, one per lock site — replacing the notional cover that ~160
    incidental non-transactional tests were providing.

    A fourth question turned out to hide in the same blind spot: the transaction
    must be opened **on the store's own alias**. A bare `transaction.atomic()`
    binds to `default`, so a store on another alias ran its writes in autocommit
    while opening an empty transaction on `default` — and `select_for_update`
    checks the *target* connection, so on Postgres the alias was unusable for
    writes at all. Under a plain `django_db` marker every one of these passes
    either way, because pytest-django has already opened a transaction on each
    declared alias. See `TestTheTransactionIsOpenedOnTheStoresAlias`.
    """

    def test_append_opens_its_own_transaction(self) -> None:
        """Covers both the stream row lock and the offset watermark."""
        store = DjangoStreamStore()
        store.create("own-txn-append")

        store.append("own-txn-append", b'{"x": 1}')

        assert store.get("own-txn-append") is not None

    def test_close_opens_its_own_transaction(self) -> None:
        store = DjangoStreamStore()
        store.create("own-txn-close")

        store.append("own-txn-close", b'{"x": 1}', AppendOptions(close=True))

        stream = store.get("own-txn-close")
        assert stream is not None
        assert stream.closed

    def test_retire_opens_its_own_transaction(self) -> None:
        """`DjangoExecutor.apply` wraps the batch; `_retire` locks inside it."""
        _open_alert("own-txn-retire")

        report = DjangoExecutor().apply(
            [
                Retire(
                    model_label=ALERT_MODEL,
                    lookup={"stream_key": "own-txn-retire", "resolved_at": None},
                    patch={"resolved_at": "2026-01-01T00:00:00Z"},
                    transition=Transition(
                        kind="alert_resolved",
                        key_fields=("stream_key", "alert_type", "field_key"),
                    ),
                )
            ]
        )

        assert report is not None
        assert not Alert.objects.filter(
            stream_key="own-txn-retire", resolved_at=None
        ).exists()


# ---------------------------------------------------------------------------
# The stream row lock: admission checks and the write are one step
# ---------------------------------------------------------------------------


@requires_row_locks
@pytest.mark.django_db(transaction=True)
class TestStreamRowLock:
    """`_live_stream(..., for_update=True)` serialises writers on one stream.

    The property is not "appends are ordered" — the offset watermark gives that
    — but that a writer's *decision* (is this stream closed? does the content
    type match? is this producer fenced?) cannot be invalidated between reading
    it and acting on it.
    """

    def test_an_append_cannot_overtake_a_committed_close(self) -> None:
        """The forced interleave: B decides while A's close is in flight.

        A takes the stream row and closes it, holding the transaction open. B
        tries to append. If the row lock holds, B blocks until A commits, then
        re-reads the *committed* state and is refused. Without the lock B would
        read the pre-close row, decide the stream is open, and land an event
        after the close — the write the fencing exists to prevent.
        """
        store = DjangoStreamStore()
        store.create("row-lock")

        a_holding = threading.Event()
        b_started = threading.Event()
        outcome: dict[str, Any] = {}

        def closer() -> None:
            with transaction.atomic():
                store.append("row-lock", b'{"last": true}', AppendOptions(close=True))
                a_holding.set()
                # Hold the lock while B reaches it. Without this the two
                # transactions would most likely not overlap at all.
                assert b_started.wait(timeout=10)
                time.sleep(0.5)

        def appender() -> None:
            assert a_holding.wait(timeout=10)
            b_started.set()
            # A refusal is not an exception here: `decide_append` returns a
            # verdict, and a closed stream comes back as a result carrying no
            # message.
            result = store.append("row-lock", b'{"late": true}')
            outcome["result"] = "refused" if result.message is None else "accepted"

        _run(closer, appender)

        assert outcome["result"] == "refused", (
            "an append was accepted after the stream was closed -- the writer "
            "decided against a stale copy of the stream row"
        )

    def test_concurrent_closes_settle_on_one_winner(self) -> None:
        """Two writers closing at once: exactly one may win.

        Both read the row, both see it open, both write. Serialised on the row,
        the loser re-reads the winner's committed close and is refused; the
        stream ends up closed once, by one of them.
        """
        store = DjangoStreamStore()
        store.create("row-lock-close")

        results: list[str] = []
        lock = threading.Lock()
        ready = threading.Barrier(2)

        def closer(tag: str) -> Any:
            def inner() -> None:
                ready.wait(timeout=10)
                result = store.append(
                    "row-lock-close",
                    f'{{"by": "{tag}"}}'.encode(),
                    AppendOptions(close=True),
                )
                with lock:
                    results.append("refused" if result.message is None else "accepted")

            return inner

        _run(closer("a"), closer("b"))

        assert sorted(results) == ["accepted", "refused"], (
            f"expected exactly one close to win, got {results}"
        )
        stream = store.get("row-lock-close")
        assert stream is not None and stream.closed


# ---------------------------------------------------------------------------
# The retire lock: the reported flip set is the set that actually flipped
# ---------------------------------------------------------------------------


@requires_row_locks
@pytest.mark.django_db(transaction=True)
class TestRetireLock:
    """`_retire` captures rows with `select_for_update()` before UPDATEing them.

    A retire carrying a `Transition` promises one notification per row it
    *actually* flipped. It gets that by SELECTing the open rows and then
    UPDATEing them. Under READ COMMITTED those are two points in time, and
    without the lock a concurrent writer can slip between them: the captured
    set and the updated set then disagree, producing a notification for a
    resolution someone else did, or missing one.

    **These tests open that window deliberately.** An earlier version of them
    simply ran two threads and asserted the outcome; both passed with the
    `select_for_update()` removed, because `apply()` issues its SELECT and
    UPDATE back to back and the threads never actually interleaved. A race test
    that cannot lose is not a test. `_slow_update` below patches
    `QuerySet.update` to pause *only on the retiring thread*, which holds the
    window open long enough for the other writer to reach it. With the lock the
    other writer blocks on the captured rows; without it, it gets in.
    """

    @staticmethod
    def _slow_update(monkeypatch: pytest.MonkeyPatch, window: threading.Event) -> None:
        """Pause between a retire's SELECT and its UPDATE, on that thread only.

        `QuerySet.update` is patched process-wide, so the pause is gated on a
        thread-local flag the retiring thread sets. The other thread's own
        `update()` must not be slowed, or it could not exploit the window and
        the test would prove nothing either way.
        """
        original = QuerySet.update

        def patched(self: Any, **kwargs: Any) -> Any:
            if getattr(_RETIRING, "active", False):
                _RETIRING.active = False  # only the retire's own UPDATE waits
                window.set()
                time.sleep(1.0)
            return original(self, **kwargs)

        monkeypatch.setattr(QuerySet, "update", patched)

    def test_a_concurrent_resolve_cannot_split_the_reported_flip_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every row the retire reported must be one the retire itself resolved.

        A captures both open alerts, then stalls before its UPDATE. B tries to
        resolve whichever alerts are still open. Locked, B waits, finds nothing
        open once A commits, and writes nothing — so both rows carry A's
        timestamp and A's report is honest. Unlocked, B resolves `rule_a` with
        its own timestamp, A's open-guarded UPDATE then matches only `rule_b`,
        and A reports two flips having caused one.
        """
        _open_alert("retire-race", "rule_a")
        _open_alert("retire-race", "rule_b")
        window = threading.Event()
        self._slow_update(monkeypatch, window)
        reported: dict[str, Any] = {}

        def retirer() -> None:
            _RETIRING.active = True
            try:
                reported["report"] = DjangoExecutor().apply([_retire_effect(A_TS)])
            finally:
                _RETIRING.active = False

        def resolver() -> None:
            assert window.wait(timeout=10)
            # Open-guarded, exactly as a competing resolve would be: if A has
            # already committed, this matches nothing and writes nothing.
            Alert.objects.filter(stream_key="retire-race", resolved_at=None).update(
                resolved_at=B_TS
            )

        _run(retirer, resolver)

        flipped = [rows for _eff, rows in reported["report"].retire_flips]
        assert len(flipped) == 1
        announced = {r["alert_type"] for r in flipped[0]}
        actually_resolved_by_a = set(
            Alert.objects.filter(
                stream_key="retire-race", resolved_at=A_TS
            ).values_list("alert_type", flat=True)
        )
        assert announced == actually_resolved_by_a, (
            f"the retire announced {sorted(announced)} but resolved "
            f"{sorted(actually_resolved_by_a)} -- a writer got between the "
            "capture and the update"
        )

    def test_two_retires_cannot_both_claim_the_same_rows(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Across two competing retires, each row is announced exactly once.

        A captures the open alerts and stalls. B runs a whole retire of its own.
        Locked, B blocks on A's captured rows, and once A commits B's
        open-guarded capture finds nothing — six announcements in total.
        Unlocked, B captures the same six rows A did and both announce them,
        so a downstream notifier fires twice per resolution.
        """
        for i in range(6):
            _open_alert("retire-dup", f"rule_{i}")
        window = threading.Event()
        self._slow_update(monkeypatch, window)
        seen: list[str] = []
        lock = threading.Lock()

        def record(report: Any) -> None:
            with lock:
                seen.extend(
                    row["alert_type"]
                    for _eff, rows in report.retire_flips
                    for row in rows
                )

        def retirer_a() -> None:
            _RETIRING.active = True
            try:
                record(DjangoExecutor().apply([_retire_effect(A_TS, "retire-dup")]))
            finally:
                _RETIRING.active = False

        def retirer_b() -> None:
            assert window.wait(timeout=10)
            record(DjangoExecutor().apply([_retire_effect(B_TS, "retire-dup")]))

        _run(retirer_a, retirer_b)

        assert len(seen) == len(set(seen)), (
            f"a row was announced as resolved by both retires: {sorted(seen)}"
        )
        assert len(seen) == 6, (
            f"expected all 6 alerts announced exactly once between the two "
            f"retires, got {len(seen)}"
        )


@requires_row_locks
@pytest.mark.django_db(transaction=True)
class TestTheWatermarkReadIsALockingRead:
    """The offset watermark is read once, and that one read still takes the lock.

    `get_next_offset_block` folds what used to be a plain `get_or_create` plus a
    `select_for_update().get()` into a single locked `get_or_create`, halving the
    reads on the hottest path in the package. That is only safe if Django applies
    the queryset's `FOR UPDATE` to the `get` half — it does, but nothing in the
    default run can see it, because SQLite reports `has_select_for_update` false
    and Django then omits the clause entirely.

    `test_concurrent_appends.py` races this lock and proves it *serialises*. This
    asserts the clause is actually emitted, which is the failure mode a fold like
    this introduces: a lock silently dropped still serialises fine under one
    connection, so contention tests keep passing while the guarantee is gone.
    Spying on `QuerySet.select_for_update` would only prove the method was called.
    """

    def test_the_single_watermark_read_carries_for_update(self):
        Stream.objects.create(stream_id="lockread")
        with transaction.atomic():
            stream = Stream.objects.get(stream_id="lockread")
            # Advance off zero first, so the measured call is the steady-state
            # path rather than the once-per-path seeding branch.
            stream.get_next_offset_block(1)
            with CaptureQueriesContext(connection) as ctx:
                stream.get_next_offset_block(1)

        reads = [
            q["sql"]
            for q in ctx.captured_queries
            if "rakaia_streamoffsetwatermark" in q["sql"]
            and q["sql"].lstrip().upper().startswith("SELECT")
        ]
        assert len(reads) == 1, (
            f"expected one watermark read, got {len(reads)}: {reads}"
        )
        assert "FOR UPDATE" in reads[0].upper(), (
            f"the watermark read is not a locking read: {reads[0]}"
        )


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
class TestTheTransactionIsOpenedOnTheStoresAlias:
    """A store on a named alias must open its transaction *there*.

    `transaction.atomic()` with no argument binds to `default`. A store pointed
    at another alias therefore wrote in autocommit while opening an empty
    transaction on `default` — three distinct failures from one missing keyword:
    `select_for_update` raised on Postgres because it checks the target
    connection; a failed append left its event and entry behind on SQLite, which
    is the split-write class #159 exists to prevent; and the stray `BEGIN` on
    `default` tripped `deny_database_access`, which is the whole point of the
    alias.

    `transaction=True` is what makes these visible. Under a plain `django_db`
    marker pytest-django has already opened a transaction on every declared
    alias, which silently supplies both the missing atomic and the absent
    `BEGIN` — so the entire feature can be tested green while being unusable in
    production.

    `requires_row_locks` is applied **per test, not to the class**: only the cases
    whose failure mode *is* the lock need a backend that has one. The rollback and
    the guard fail on SQLite too, and skipping them there would drop the only
    coverage of a defect that shows up on the default test run.
    """

    @requires_row_locks
    def test_a_write_to_the_alias_succeeds_in_real_autocommit(self) -> None:
        # On Postgres this raised `TransactionManagementError: select_for_update
        # cannot be used outside of a transaction` before the fix.
        store = DjangoStreamStore(using="overlay")
        store.create("alias-txn")

        store.append("alias-txn", b'{"x": 1}')

        assert store.get("alias-txn") is not None

    def test_a_failed_write_to_the_alias_rolls_back(self) -> None:
        # The split-write case: without the alias on the atomic, the event and
        # entry were already committed by the time the failure happened.
        from unittest.mock import patch

        from django_rakaia.models import StreamEntry, StreamEvent

        store = DjangoStreamStore(using="overlay")
        store.create("alias-rollback")

        with (
            patch.object(DjangoStreamStore, "_touch", side_effect=RuntimeError("boom")),
            pytest.raises(RuntimeError, match="boom"),
        ):
            store.append("alias-rollback", b'{"x": 1}')

        assert StreamEvent.objects.using("overlay").count() == 0
        assert StreamEntry.objects.using("overlay").count() == 0

    def test_writing_to_the_alias_does_not_touch_the_guarded_default(self) -> None:
        # The claim `hermeticity.py` now makes: give the store the alias and a
        # rebuild that *writes* inside the guard has nothing to work around. The
        # stray `BEGIN` on `default` used to fail this.
        from django_rakaia.hermeticity import deny_database_access

        store = DjangoStreamStore(using="overlay")
        store.create("alias-guarded")

        with deny_database_access("default"):
            store.append("alias-guarded", b'{"x": 1}')

        assert [m.data for m in store.read("alias-guarded")[0]] == [b'{"x": 1}']

    @requires_row_locks
    def test_a_batch_write_to_the_alias_opens_its_own_transaction(self) -> None:
        # `append_many` is a separate write door with its own atomic.
        store = DjangoStreamStore(using="overlay")
        store.create("alias-batch")

        results = store.append_many(
            "alias-batch", [(b'{"n": 1}', None), (b'{"n": 2}', None)]
        )

        assert [r.message is not None for r in results] == [True, True]

    @requires_row_locks
    def test_creating_with_a_body_opens_its_own_transaction(self) -> None:
        # `create(initial_data=...)` routes through offset allocation, so it has
        # its own atomic. Its docstring calls a missing transaction here "a
        # guaranteed 500 in production" and points at the contract's
        # create-with-body case as the cover — but that case never runs on an
        # alias, so this site was unproven off `default`.
        store = DjangoStreamStore(using="overlay")

        store.create("alias-create-body", initial_data=b'{"x": 1}')

        assert [m.data for m in store.read("alias-create-body")[0]] == [b'{"x": 1}']

    @requires_row_locks
    def test_a_fenced_append_on_the_alias_opens_its_own_transaction(self) -> None:
        # The sync inner method, not the async wrapper: the atomic lives here and
        # `append_with_producer` only marshals into it, so this tests the site
        # rather than the thread hop.
        store = DjangoStreamStore(using="overlay")
        store.create("alias-fenced")

        result = store._append_with_producer_sync(
            "alias-fenced",
            b'{"x": 1}',
            AppendOptions(producer_id="p", producer_epoch=1, producer_seq=0),
        )

        assert result.message is not None

    @requires_row_locks
    def test_a_fenced_close_on_the_alias_opens_its_own_transaction(self) -> None:
        # A separate atomic from `append`'s — closing under fencing takes its own.
        store = DjangoStreamStore(using="overlay")
        store.create("alias-fenced-close")

        result = store._close_with_producer_sync("alias-fenced-close", "p", 1, 0)

        assert result is not None
        stream = store.get("alias-fenced-close")
        assert stream is not None
        assert stream.closed
