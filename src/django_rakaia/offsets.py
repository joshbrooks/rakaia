"""The durable store's offset format, as this package's callers want it.

The format itself — the width, the padding, what counts as one of this store's
tokens — lives in `rakaia.offsets` as `PLAIN`, alongside the in-memory store's
`COMPOUND` and the ordering rule that refuses to compare across the two (#182).
Both stores had four of the five offset rules written down separately and the
fifth, comparison, written down nowhere, which is why it guessed.

What stays here are the two integer-facing helpers this package's three callers
want: the store (`django_store`), the model property that reports a stream head
(`models.Stream.current_offset`), and the dashboard views that accept an offset
from a client. Keeping them here is what stops the model layer from importing out
of the store layer for its own property, and stops a view from pulling in the
whole store just to read a query parameter.
"""

from __future__ import annotations

from rakaia.offsets import PLAIN


def format_offset(value: int) -> str:
    """Render an integer offset as the protocol's opaque, sortable string."""
    return PLAIN.render(value)


def parse_offset(offset: str) -> int:
    """The entry offset `offset` denotes, in the durable store's own format.

    Strict on purpose, and `PLAIN` is what makes it so. `int()` alone would
    accept far more than this store ever issues — it treats underscores as digit
    separators, so the in-memory store's compound `{seq}_{byte}` offset parses
    cleanly into an unrelated number, and a resume read would quietly return the
    wrong window instead of failing.

    Raises `ForeignOffset`, which subclasses `InvalidOffset` — so an existing
    ``except InvalidOffset`` is unaffected.
    """
    return PLAIN.key(offset)[0]
