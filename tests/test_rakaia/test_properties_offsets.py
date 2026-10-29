"""Properties of an offset: what `render` writes, `key` reads back, and byte order agrees with.

The protocol (§3, §5.2) needs offsets to sort byte-wise in the same order as the
numbers they stand for; zero-padding is the only thing that delivers that. These
hold for every field value below its width, which is the range a store can issue.
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from rakaia.offsets import COMPOUND, FORMATS, PLAIN, after, format_of


def _field(width):
    # Pick the digit count first, weighted towards the full width: padding one
    # digit short only misorders a (width-1)-digit value against a full one, a
    # pair that uniform draws over 0..10**width almost never produce.
    digits = st.one_of(st.integers(1, width), st.integers(width - 1, width))
    return digits.flatmap(lambda k: st.integers(10 ** (k - 1) - (k == 1), 10**k - 1))


def _fields(fmt):
    return st.tuples(*(_field(w) for w in fmt.widths))


@pytest.mark.parametrize("fmt", FORMATS, ids=lambda f: f.name)
def test_key_reads_back_what_render_wrote(fmt):
    @given(_fields(fmt))
    def check(values):
        token = fmt.render(*values)
        assert fmt.key(token) == values
        assert format_of(token) is fmt

    check()


@pytest.mark.parametrize("fmt", FORMATS, ids=lambda f: f.name)
def test_byte_order_matches_numeric_order(fmt):
    @given(_fields(fmt), _fields(fmt))
    def check(a, b):
        ta, tb = fmt.render(*a), fmt.render(*b)
        assert (ta > tb) == (a > b)
        assert (ta < tb) == (a < b)
        assert after(ta, tb) == (a > b)

    check()


def test_formats_under_test_are_the_ones_issued():
    assert set(FORMATS) == {COMPOUND, PLAIN}
