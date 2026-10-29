"""GitHub #351: a temporary database error is recorded as a permanent failure."""

from __future__ import annotations

from django.db import InterfaceError, OperationalError

from rakaia.outcomes import InMemoryOutcomeStore
from rakaia.store import StreamStore
from rakaia.subscription import consume


def test_a_transient_database_error_is_not_skipped_for_good() -> None:
    store = StreamStore()
    store.create("s")
    store.append("s", b"a")
    committed: list[str] = []
    outcomes = InMemoryOutcomeStore()

    def apply(_message):
        raise OperationalError("server closed the connection unexpectedly")

    result = consume(
        store,
        "s",
        apply=apply,
        consumer="c",
        on_error="skip",
        outcomes=outcomes,
        commit=committed.append,
        transient=(OperationalError, InterfaceError),
    )

    # Temporary errors must be retried: the cursor stays below the event and no
    # permanent "failed" fact is written for it.
    assert committed == [], f"cursor advanced past a transient failure: {committed}"
    assert not result.outcomes


def test_a_transient_error_is_logged_as_a_warning(caplog) -> None:
    store = StreamStore()
    store.create("s")
    store.append("s", b"a")

    def apply(_message):
        raise OperationalError("server closed the connection unexpectedly")

    with caplog.at_level("WARNING", logger="rakaia.subscription"):
        consume(
            store,
            "s",
            apply=apply,
            consumer="c",
            on_error="skip",
            commit=lambda _o: None,
            transient=(OperationalError, InterfaceError),
        )

    assert any(
        "retry" in r.getMessage() and r.levelname == "WARNING" for r in caplog.records
    )
