"""GitHub #343 item 1: `consume` applies a rewound re-read before the caller can reset."""

from __future__ import annotations

import pytest

from rakaia.store import StreamStore
from rakaia.subscription import consume


@pytest.mark.xfail(
    strict=True,
    reason="GitHub #343: consume applies a rewound re-read before derived state can be reset",
)
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
