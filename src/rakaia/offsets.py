"""What a stream position is, per store — all five rules in one place each.

An offset has about five rules: what the first one is, how wide it is, how to
make the next one, how to tell a valid one, and how to compare two. Both stores
implement the same idea in different bytes, so there is a real shared shape here;
it had simply never been written down. Before this module:

* the durable store kept four of its five in `django_rakaia/offsets.py`;
* the in-memory store had them spread across `types.py` and `store.py`, with the
  field width written out three times — and a comment saying a change must
  update "the fields (and ``INITIAL_OFFSET``) in lockstep", which is a rule
  announcing that it has no home; and
* **the fifth rule, comparison, had no home in either.** So the one place that
  needed it — `subscription.poll`, deciding whether a saved cursor sits beyond
  the stream head — carried its own guess.

**Why the comparison could not be got right by trying harder.** The protocol
makes an offset *opaque* (§6): `protocols.ReadableStore` tells consumers to
compare offsets from **the same store**. Two stores' offsets are not on one
scale — an in-memory ``{seq}_{byte}`` counts events and bytes, a durable offset
is an entry id — so there is no correct cross-store answer to compute. The old
fallback compared them as text, where ``'0000000000000000_0000000000000008'``
sorts *above* ``'00000000000000000042'`` because ``'0' < '3'`` settles it before
any digit that means anything is reached. A cursor four events into a stream was
reported as being beyond a head at forty-two.

So the answer is to refuse. `after` raises `ForeignOffset` unless both tokens are
in the same recognised format, which is the same judgement each store already
made alone — see the `owns` docstring for why only a format can make it.

**How a mismatched pair actually arises.** Not from a production migration: the
in-memory store is for tests, demos and the conformance runs, so no deployment
accumulates a cursor under it and then switches. The reachable causes are a test
that builds a cursor with the wrong store, and a saved cursor that has been
corrupted or hand-edited. Refusing turns both into an error naming the offset
instead of a `rewound` result, which claims the log was rebuilt and tells the
consumer to discard its derived state.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .types import InvalidOffset

__all__ = [
    "COMPOUND",
    "FORMATS",
    "PLAIN",
    "ForeignOffset",
    "OffsetFormat",
    "after",
    "format_of",
]


class ForeignOffset(InvalidOffset):
    """An offset was used where its format does not belong — passed to a store
    that did not issue it, or compared against one from another store.

    Subclasses `InvalidOffset` on purpose: both stores already raised that for a
    token they did not recognise, so every existing ``except InvalidOffset``
    still catches this.
    """


@dataclass(frozen=True)
class OffsetFormat:
    """One store's offset format, and every rule about its tokens.

    ``widths`` is one entry per zero-padded numeric field, in order — so it is
    both the shape and the size limit. The limit is a choice rather than a
    consequence (nothing forces 16 or 20), and this is the one place it is
    written, so widening one is an edit here instead of an edit in three places
    that have to agree.
    """

    name: str
    widths: tuple[int, ...]

    @property
    def pattern(self) -> re.Pattern[str]:
        """This format's *shape*: how many numeric fields, joined by ``_``.

        Built from `widths` so the field count cannot drift from what `render`
        produces. It deliberately does **not** pin each field's width: the widths
        are a padding and sort rule for offsets this store issues, not an input
        filter. Clients send unpadded offsets — the dashboard's ``?after=42`` is
        one — and both regexes this replaced accepted them, so tightening to
        ``\\d{20}`` here would reject requests that work today. What the shape
        does settle is the only question `owns` is asked: which of the two stores
        a token belongs to, one field against two.
        """
        return re.compile("^" + "_".join(r"\d+" for _ in self.widths) + "$")

    def owns(self, token: str) -> bool:
        """Whether `token` is one this format issues.

        Only a format can answer this, which is why the check cannot be shared
        across stores as a single syntactic guard. `types.VALID_OFFSET_PATTERN`
        deliberately accepts **both** shapes — it is the protocol server's guard
        on what a client may send — so it cannot tell a durable offset from an
        in-memory one. Nor can `int()`: it reads ``_`` as a digit separator, so
        the in-memory ``{seq}_{byte}`` parses cleanly into an unrelated number.
        """
        return bool(self.pattern.match(token))

    def require(self, token: str) -> None:
        """Raise `ForeignOffset` unless `token` is one this format issues."""
        if not self.owns(token):
            raise ForeignOffset(
                f"Not an offset the {self.name} store issued: {token!r}. Its "
                f"offsets have the form "
                f"{'_'.join('{' + str(w) + ' digits}' for w in self.widths)}. "
                f"An offset is opaque and only comparable within the store that "
                f"issued it — clear the saved position before switching stores."
            )

    def key(self, token: str) -> tuple[int, ...]:
        """`token` as an orderable tuple, most significant field first."""
        self.require(token)
        return tuple(int(part) for part in token.split("_"))

    def render(self, *fields: int) -> str:
        """The token for these field values, zero-padded to this format's widths.

        Padding is what makes offsets sort byte-wise lexicographically, which the
        protocol requires (§3, §5.2) and which is what keeps them monotonic
        across a delete-and-recreate. A width is a *minimum*, not a cap: the
        ordering guarantee holds only while every field stays below its width, so
        a store that could exceed one must widen it here.
        """
        if len(fields) != len(self.widths):
            raise ValueError(
                f"{self.name} offsets have {len(self.widths)} field(s), "
                f"got {len(fields)}: {fields!r}"
            )
        return "_".join(f"{v:0{w}d}" for v, w in zip(fields, self.widths, strict=True))

    def first(self) -> str:
        """The offset a brand-new stream starts at: every field zero."""
        return self.render(*(0 for _ in self.widths))


#: The in-memory `StreamStore`: ``{read_seq}_{byte_offset}``.
#:
#: 16 digits each. Both bounds are unreachable rather than generous — the byte
#: offset is capped by the process's memory (this store holds every message in
#: RAM, so 10**16 bytes ≈ 10 PB is impossible) and so is 10**16 recreations of
#: one path.
COMPOUND = OffsetFormat(name="in-memory", widths=(16, 16))

#: `DjangoStreamStore`: a single zero-padded entry id.
#:
#: 20 digits covers a ``BigAutoField``'s range (< 2**63). Reads parse it
#: numerically, so the padding is transparent to filtering.
PLAIN = OffsetFormat(name="durable", widths=(20,))

#: Every format rakaia issues. Order matters only for `format_of`'s search, and
#: the patterns are disjoint (one field versus two), so it does not.
FORMATS: tuple[OffsetFormat, ...] = (COMPOUND, PLAIN)


def format_of(token: str) -> OffsetFormat | None:
    """The format that issued `token`, or ``None`` if no format claims it."""
    for fmt in FORMATS:
        if fmt.owns(token):
            return fmt
    return None


def after(a: str, b: str) -> bool:
    """Whether offset `a` sorts strictly after `b`.

    Raises:
        ForeignOffset: if either token is unrecognised, or the two come from
            different formats. There is no cross-store ordering to return — see
            this module's docstring — and the alternative to raising is the
            confident wrong answer this replaced.
    """
    fmt_a, fmt_b = format_of(a), format_of(b)
    if fmt_a is None or fmt_b is None or fmt_a is not fmt_b:
        raise ForeignOffset(
            f"Cannot order {a!r} against {b!r}: "
            f"{'unrecognised offset format' if fmt_a is None or fmt_b is None else f'{fmt_a.name} against {fmt_b.name}'}"
            f". An offset is opaque and only comparable within the store that "
            f"issued it, so there is no answer here to compute — clear the saved "
            f"position before switching stores."
        )
    return fmt_a.key(a) > fmt_b.key(b)
