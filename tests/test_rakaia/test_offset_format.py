"""A stream position's rules, and what happens when two stores' positions meet.

Five rules govern an offset: what the first one is, how wide it is, how to make
the next one, how to tell a valid one, and how to compare two. The durable store
keeps four of them in one file. The in-memory store has them spread across two
files with the width written out three times, and the fifth — comparison — had no
home at all, which is why it guessed.

The protocol makes an offset **opaque**: `protocols.ReadableStore` tells consumers
to compare offsets from *the same store*. So there is no correct answer to
"is this in-memory offset after that durable one" to go and compute. The only
honest answers are to refuse, or to guess; it guessed.
"""

from __future__ import annotations

import pytest

from rakaia.offsets import COMPOUND, PLAIN, ForeignOffset, after, format_of
from rakaia.store import StreamStore
from rakaia.subscription import poll
from rakaia.types import INITIAL_OFFSET


class TestComparingAcrossFormats:
    def test_it_refuses_rather_than_guessing(self):
        # 4 events consumed in the in-memory store, against a durable head of 42.
        mem = "0000000000000000_0000000000000008"
        durable = "00000000000000000042"
        with pytest.raises(ForeignOffset):
            after(mem, durable)

    def test_the_guess_it_used_to_make_was_wrong(self):
        # Kept as a statement of the defect: lexicographically the in-memory
        # token sorts *above* the durable one, because '0' < '3' decides it
        # before any digit that means anything is reached. So the old fallback
        # answered "yes, the cursor is beyond the head" for a cursor four events
        # into a stream whose head is at forty-two.
        mem = "0000000000000000_0000000000000008"
        durable = "00000000000000000042"
        assert (mem > durable) is True  # what `a > b` returned
        assert format_of(mem) is COMPOUND
        assert format_of(durable) is PLAIN

    def test_an_unrecognised_token_is_refused_too(self):
        with pytest.raises(ForeignOffset):
            after("not-an-offset", "00000000000000000042")

    def test_within_one_format_it_still_answers(self):
        assert after("00000000000000000042", "00000000000000000009") is True
        assert after("0000000000000000_0000000000000008", INITIAL_OFFSET) is True
        assert after(INITIAL_OFFSET, "0000000000000000_0000000000000008") is False

    def test_equal_offsets_are_not_after_each_other(self):
        assert after(INITIAL_OFFSET, INITIAL_OFFSET) is False


class TestPollRefusesAForeignCursor:
    """The public seam. Today this reports `rewound` — "the log was rebuilt
    beneath you, reset your derived state" — for a cursor that is simply from
    another store. The data converges after the re-read, so nothing is corrupted;
    the consumer is told the wrong reason and does a full re-read for it."""

    def test_a_cursor_from_another_store_raises(self):
        store = StreamStore()
        store.create("s")
        for i in range(4):
            store.append("s", f"m{i}".encode())

        with pytest.raises(ForeignOffset):
            poll(store, "s", "00000000000000000042")

    def test_a_cursor_from_this_store_is_unaffected(self):
        store = StreamStore()
        store.create("s")
        for i in range(4):
            store.append("s", f"m{i}".encode())

        result = poll(store, "s", INITIAL_OFFSET)
        assert result.status == "advanced"
        assert len(result.messages) == 4


class TestTheWidthHasOneHome:
    def test_the_first_offset_is_the_format_saying_so(self):
        # `types.INITIAL_OFFSET` is a literal, and the two places that build a
        # compound offset wrote `:016d` out by hand. A change to the width had to
        # be made in three places in lockstep — which the code says out loud, in
        # a comment, rather than preventing.
        assert COMPOUND.first() == INITIAL_OFFSET

    def test_the_format_renders_what_the_store_renders(self):
        assert COMPOUND.render(0, 0) == INITIAL_OFFSET
        assert COMPOUND.render(3, 128) == f"{3:016d}_{128:016d}"
        assert PLAIN.render(42) == "00000000000000000042"


class TestRefusalIsCheckedAtEveryLevel:
    """The three mutations that survived a first pass, each now pinned.

    Worth naming: dropping the `is None` half of `after`'s guard leaves the
    *one*-unknown case still raising — via `None is not PLAIN` — so it looks
    covered. It is the two-unknown case that then falls through to
    `None.key(...)` and raises `AttributeError` instead of `ForeignOffset`.
    """

    def test_two_unrecognised_tokens_are_refused_not_crashed_on(self):
        with pytest.raises(ForeignOffset):
            after("garbage", "rubbish")

    def test_a_format_rejects_the_other_store_s_token(self):
        with pytest.raises(ForeignOffset):
            COMPOUND.require("00000000000000000042")
        with pytest.raises(ForeignOffset):
            PLAIN.require("0000000000000000_0000000000000008")

    def test_the_store_rejects_a_foreign_offset_on_read(self):
        """Why `require` has to raise, not merely report.

        Accepting a durable offset here does not fail loudly: this store compares
        offsets as text, and a plain integer sorts *below* every compound token,
        so the read matches everything and a resume returns the whole stream —
        or, with the guard disabled, wedges a long-poll waiting on a position
        that can never arrive. Disabling `require` and running the full suite
        hangs rather than failing, which is how this case was found.
        """
        store = StreamStore()
        store.create("s")
        store.append("s", b"m")
        with pytest.raises(ForeignOffset):
            store.read("s", "00000000000000000042")

    def test_render_refuses_the_wrong_number_of_fields(self):
        with pytest.raises(ValueError, match="2 field"):
            COMPOUND.render(1)
        with pytest.raises(ValueError, match="1 field"):
            PLAIN.render(1, 2)
