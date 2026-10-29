"""GitHub #350: a consumer cannot tell its stream was deleted and recreated."""

from __future__ import annotations

import pytest

from django_rakaia.django_store import DjangoStreamStore
from rakaia.subscription import consume


@pytest.mark.xfail(
    strict=True,
    reason="GitHub #350: recreated stream silently drops events below the saved cursor",
)
@pytest.mark.django_db
def test_a_recreated_stream_is_not_reported_as_a_plain_advance() -> None:
    store = DjangoStreamStore()
    store.create("s")
    for payload in (b"a", b"b", b"c"):
        store.append("s", payload)

    # The consumer has read nothing yet; its cursor is the stream's start.
    # Pretend the consumer only got through event 0 before the stream vanished.
    cursor = store.read("s")[0][0].offset

    store.delete("s")
    store.create("s")
    store.append("s", b"new")

    seen: list[bytes] = []
    result = consume(
        store,
        "s",
        apply=lambda m: seen.append(m.data),
        consumer="c",
        on_error="halt",
        cursor=cursor,
    )

    # The new stream's events are all "new" to this consumer; it must be told.
    assert result.status not in ("advanced", "caught_up"), (
        f"recreated stream reported as {result.status!r}; saw {seen!r}"
    )
