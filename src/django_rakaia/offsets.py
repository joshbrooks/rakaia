"""The durable store's offset format: how to render one, and how to read one.

Its own module because it has three callers at two different layers — the
store (`django_store`), the model property that reports a stream head
(`models.Stream.current_offset`), and the dashboard views that accept an
offset from a client. Keeping it here is what stops the model layer from
importing out of the store layer for its own property, and stops a view from
pulling in the whole store just to read a query parameter.
"""

from __future__ import annotations

import re

from rakaia.types import InvalidOffset

# Offsets are rendered zero-padded so they sort byte-wise lexicographically, as
# the Durable Streams protocol requires (§3, §5.2). 20 digits covers a
# BigAutoField's range (< 2**63). Reads still parse them numerically, so the
# padding is transparent to filtering.
_OFFSET_WIDTH = 20

# This store's offsets, and nothing else. See `parse_offset`.
_PLAIN_INTEGER_OFFSET = re.compile(r"^\d+$")


def format_offset(value: int) -> str:
    """Render an integer offset as the protocol's opaque, sortable string."""
    return f"{value:0{_OFFSET_WIDTH}d}"


def parse_offset(offset: str) -> int:
    """The entry offset `offset` denotes, in the durable store's own format.

    Strict on purpose. `int()` alone would accept far more than this store
    ever issues — it treats underscores as digit separators, so the in-memory
    store's compound `{seq}_{byte}` offset parses cleanly into an unrelated
    number, and a resume read would quietly return the wrong window instead of
    failing. `VALID_OFFSET_PATTERN` cannot catch that either: it is a shared
    syntactic guard, and the protocol makes offsets opaque rather than uniform
    (§6), so only the issuing store can say whether a token is one of its own.
    """
    if not _PLAIN_INTEGER_OFFSET.match(offset):
        raise InvalidOffset(
            f"Not an offset this store issued: {offset!r}. Durable-store "
            f"offsets are plain integers."
        )
    return int(offset)
