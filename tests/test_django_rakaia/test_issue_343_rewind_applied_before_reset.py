"""GitHub #343 item 1: `consume` applies a rewound re-read before the caller can reset."""

from __future__ import annotations

from rakaia.store import StreamStore
from rakaia.subscription import consume


def test_a_rewound_stream_is_not_applied_before_the_caller_can_reset() -> None:
    store = StreamStore()
    store.create("s")
    for payload in (b"a", b"b", b"c"):
        store.append("s", payload)
    cursor = store.get_current_offset("s")

    # The log is rebuilt shorter: the saved cursor now points beyond the head.
    store = StreamStore()
    store.create("s")
    store.append("s", b"x")

    seen: list[bytes] = []
    result = consume(
        store,
        "s",
        apply=lambda m: seen.append(m.data),
        consumer="c",
        on_error="halt",
        cursor=cursor,
    )

    assert result.status == "rewound"
    assert seen == [], (
        "rewound events were applied with no chance to reset derived state"
    )


def test_a_rewound_stream_is_logged_as_an_error(caplog) -> None:
    store = StreamStore()
    store.create("s")
    store.append("s", b"a")
    store.append("s", b"b")
    cursor = store.get_current_offset("s")
    store = StreamStore()
    store.create("s")

    with caplog.at_level("ERROR", logger="rakaia.subscription"):
        consume(
            store,
            "s",
            apply=lambda _m: None,
            consumer="c",
            on_error="halt",
            cursor=cursor,
        )

    assert any(
        "rebuild" in r.getMessage() and r.levelname == "ERROR" for r in caplog.records
    )
