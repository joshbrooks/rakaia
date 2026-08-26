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
sorts *above* ``'00000000000000000042'``: the first sixteen characters are zeros
in both, and at the seventeenth the compound token has ``'_'`` (0x5F) against the
plain one's ``'0'`` (0x30), so it wins there — before either token has reached a
digit that means anything. A cursor four events into a stream was reported as
being beyond a head at forty-two.

So the answer is to refuse — but only for *that* pair. `after` raises
`ForeignOffset` when both tokens are recognised and their formats differ, which
is the same judgement each store already made alone (see the `owns` docstring for
why only a format can make it). It does **not** refuse a token it merely does not
recognise: the protocol's own rule is that an opaque offset sorts byte-wise, a
third-party `CursorStore` is a documented seam, and its ULID or timestamp offsets
are no less orderable for being unfamiliar.

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
    "MAX_OFFSET_LENGTH",
    "PLAIN",
    "ForeignOffset",
    "OffsetFormat",
    "after",
    "format_of",
    "is_syntactically_valid",
]

#: The longest offset a URL should carry, from `docs/protocol.md` §6 ("Servers
#: **SHOULD** keep offsets reasonably short (under 256 characters) since they
#: appear in every request URL").
MAX_OFFSET_LENGTH = 256

#: The characters §6 forbids outright, because they collide with query-string
#: syntax and would split one offset into several parameters.
_FORBIDDEN_IN_OFFSET = frozenset(",&=?/")


def is_syntactically_valid(token: str) -> bool:
    """Whether `token` could be *any* store's offset — not whether it is ours.

    This is the guard a protocol server puts in front of a client-supplied
    offset, and the whole of what it may check. `docs/protocol.md` §6 gives the
    rule verbatim: offsets are opaque, case-sensitive single tokens; they **MUST
    NOT** contain ``,`` ``&`` ``=`` ``?`` ``/``; they **SHOULD** be URL-safe and
    under 256 characters. Everything past that is the issuing store's business.

    It used to enumerate rakaia's own two formats — a *format* check wearing a
    *syntax* check's docstring — and that had already failed once. The pattern
    accepted only the compound form, which was invisible while the server could
    only be handed the in-memory store; the moment the durable store could back
    it, every resume 400'd on an offset the server had just issued. Widening it
    by one format fixed the instance and left the rule. A third-party store
    minting ULIDs is a supported deployment (`StreamServerStore` is a documented
    seam with its own conformance suite) and would have hit the same wall.

    **Nothing is let through that was not already refused one layer down.** A
    token this accepts is handed to the store, and a store rejects one it did
    not issue — `OffsetFormat.require` raises `ForeignOffset`, which is an
    `InvalidOffset`, which the handler maps to the same 400. What changes is
    which layer decides, and the store is the layer the design already names as
    the authority. What that costs is a store call instead of a regex match for
    junk, which is real and negligible.

    Whitespace is refused even though §6 does not name it: the spec asks for
    URL-safe characters, and a token with a space in it is far more likely to be
    a mangled request than a store's considered choice of format.
    """
    if not token or len(token) >= MAX_OFFSET_LENGTH:
        return False
    if any(c in _FORBIDDEN_IN_OFFSET for c in token):
        return False
    return not any(c.isspace() for c in token)


class ForeignOffset(InvalidOffset):
    """An offset was used where its format does not belong — passed to a store
    that did not issue it, or compared against one from another store.

    Subclasses `InvalidOffset` on purpose: every store already raised that for a
    token it did not recognise, so every existing ``except InvalidOffset``
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
        does settle is the only question `owns` is asked: which *format* a token
        belongs to, one field against two.

        A format, not a store — those stopped being the same thing when
        `JsonlStreamStore` began issuing `PLAIN` alongside `DjangoStreamStore`.
        Two stores sharing a format are indistinguishable here, and deliberately
        so, since it is what lets a copy between them preserve every offset. See
        `docs/adr/0006-changing-backends-is-a-copy.md`, which cites this rule.
        """
        return re.compile("^" + "_".join(r"\d+" for _ in self.widths) + "$")

    def owns(self, token: str) -> bool:
        """Whether `token` is one this format issues.

        Only a format can answer this, which is why the check cannot be shared
        across stores as a single syntactic guard. `is_syntactically_valid` in
        this module accepts **any** shape a store might issue — it is the
        protocol server's guard on what a client may send — so it cannot tell a
        durable offset from an in-memory one, and does not try. Nor can `int()`:
        it reads ``_`` as a digit separator, so the in-memory ``{seq}_{byte}``
        parses cleanly into an unrelated number.
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

    Refuses **only** the pair that has no answer: two tokens this library can see
    are from *different* rakaia stores. Everything else is ordered, and by the
    rule the protocol already states — an offset is "opaque, lexicographically
    sortable" (§6), so byte order is the contract for any format, including one
    this library has never seen.

    That last part is deliberate and is a correction. `protocols` promises a
    third-party `CursorStore` plugs in, and its offsets may be ULIDs, timestamps
    or hex — none of which match `COMPOUND` or `PLAIN`. Refusing every
    unrecognised token would have broken that seam for the sake of a pair that
    only arises from misuse, and would have blamed a store switch that never
    happened. So an unrecognised pair falls back to the byte-wise comparison the
    protocol mandates.

    Recognised same-format pairs get `key` instead of byte order, which is a
    strict improvement rather than a different rule: it agrees with byte order on
    every token a store *renders* (that is what the padding is for) and is also
    right for the unpadded ones a client may send — see `pattern`.

    Raises:
        ForeignOffset: if both tokens are recognised and come from different
            formats. There is no cross-store ordering to return — see this
            module's docstring — and the alternative to raising is the confident
            wrong answer this replaced.
    """
    fmt_a, fmt_b = format_of(a), format_of(b)
    if fmt_a is not None and fmt_b is not None and fmt_a is not fmt_b:
        raise ForeignOffset(
            f"Cannot order {a!r} against {b!r}: {fmt_a.name} against "
            f"{fmt_b.name}. An offset is opaque and only comparable within the "
            f"store that issued it, so there is no answer here to compute — "
            f"clear the saved position before switching stores."
        )
    if fmt_a is not None and fmt_b is not None:
        return fmt_a.key(a) > fmt_b.key(b)
    return a > b
