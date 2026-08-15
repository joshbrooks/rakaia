"""Shared conformance contract for the projection-reader surface
(`ProjectionReader`).

Rakaia reads already-built projections through more than one reader, and the
whole of rebuild verification rests on two of them — `DjangoProjectionReader`
and its batch-fetching subclass `PreloadedProjectionReader` — returning the same
answer for the same lookup. `diff_effects_against_rows` swaps one for the other
purely as an optimisation, so a disagreement between them would not fail loudly;
it would quietly report a clean rebuild that is not clean. This is the reader
twin of `tests/store_contract.py`. Subclass `ProjectionReaderContract` in each
backend's test package and provide a `seam` fixture returning a
:class:`ReaderSeam`. See #121.

This module is intentionally not named `test_*`, so pytest does not collect it
directly; only the backend subclasses run it.

**Behaviour, not just `isinstance`.** `ProjectionReader` is `runtime_checkable`,
but that only checks method *presence* — a hole `protocols.py` already records
biting this repo once, when a store's `get()` changed its return type and the
suite's `hasattr` check noticed nothing. So the suite pins the *signature* too,
`model_label` being **positional-only** in particular: `protocols.py` declares it
with `/`, and a reader that names it instead breaks any caller passing a keyword
lookup called `model_label`, and diverges the moment one is written.

**What it does not pin, and why.**

* *The return type of `filter`/`query`.* Django returns a lazy `QuerySet`, the
  in-memory reader a list. The protocol says "queryset-like iterable" and means
  it, so the suite asserts what iterating yields, never the container.
* *Row type.* An ORM instance and a `SimpleNamespace` are both "a row with
  attributes"; the suite reads columns, never the class.
* *Which row a multi-match `get` returns.* `get` is the single-match accessor;
  when a lookup matches several, "the first" is only defined by an ordering the
  protocol does not mandate. The suite pins that a match returns *a* matching
  row and an absence returns `None` without raising.
* *Freshness.* `PreloadedProjectionReader` is a point-in-time snapshot by
  design — it must not be used during live staged replay — so "sees a write made
  after construction" is deliberately not contract. Every fixture here builds
  the reader after the rows exist.
* *`using=` alias routing.* Django-specific; covered by
  `tests/test_django_rakaia/test_using_seam.py`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

import pytest

from rakaia.effects import Effect, Upsert


@dataclass(frozen=True)
class ReaderSeam:
    """The one seam this contract needs.

    Rows have to exist before a reader can be asked about them, and one reader
    under test (`PreloadedProjectionReader`) takes the batch at construction —
    so the seam is a pair of callables rather than a bare reader: `apply` puts
    the rows there, then `make_reader` is handed the same effects and returns
    the reader to interrogate.

    `model` must name a projection with the columns the suite writes:
    ``stream_key`` (str), ``alert_type`` (str), ``field_key`` (str) and
    ``severity`` (str).
    """

    apply: Callable[[list[Effect]], Any]
    """Materialise a batch of effects into the projections."""

    make_reader: Callable[[list[Effect]], Any]
    """Build the reader under test over the rows `apply` just wrote, given the
    same effects (a preloading reader uses them; a plain one ignores them)."""

    model: str
    """`model_label` of the projection described above."""


class ProjectionReaderContract:
    """Contract every projection reader must uphold.

    Subclasses provide::

        @pytest.fixture
        def seam(self):
            return ReaderSeam(
                apply=MyExecutor().apply,
                make_reader=lambda effects: MyReader(),
                model="app.Alert",
            )
    """

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _rows(seam: ReaderSeam, *keys: str, severity: str = "error") -> list[Effect]:
        return [
            Upsert(
                model_label=seam.model,
                lookup={"stream_key": "s", "alert_type": "machine", "field_key": k},
                defaults={"severity": severity},
            )
            for k in keys
        ]

    def _reader(self, seam: ReaderSeam, effects: list[Effect]) -> Any:
        seam.apply(effects)
        return seam.make_reader(effects)

    # -- protocol -----------------------------------------------------------

    def test_satisfies_the_projection_reader_protocol(self, seam):
        from rakaia import ProjectionReader

        assert isinstance(self._reader(seam, self._rows(seam, "a")), ProjectionReader)

    def test_model_label_is_positional_only(self, seam):
        """`protocols.py` declares `model_label` with a `/`, and the readers must
        agree: a caller filtering a projection that happens to have a column
        called ``model_label`` would otherwise collide with the parameter, and a
        reader that names it cannot be swapped for one that does not."""
        reader = self._reader(seam, self._rows(seam, "a"))

        with pytest.raises(TypeError):
            reader.get(model_label=seam.model, field_key="a")
        with pytest.raises(TypeError):
            reader.filter(model_label=seam.model, field_key="a")
        with pytest.raises(TypeError):
            reader.query(model_label=seam.model)

    # -- get ----------------------------------------------------------------

    def test_get_returns_the_matching_row(self, seam):
        reader = self._reader(seam, self._rows(seam, "a", "b"))

        row = reader.get(
            seam.model, stream_key="s", alert_type="machine", field_key="a"
        )
        assert row is not None
        assert row.field_key == "a"
        assert row.severity == "error"

    def test_get_returns_none_when_nothing_matches(self, seam):
        """Absence is an answer, not an exception — every caller here (the diff
        helper, a stage>0 handler resolving a reference) treats a missing row as
        data, so `get` must never raise `DoesNotExist`."""
        reader = self._reader(seam, self._rows(seam, "a"))

        assert reader.get(seam.model, field_key="nope") is None

    def test_get_returns_none_for_an_empty_projection(self, seam):
        reader = self._reader(seam, [])

        assert reader.get(seam.model, field_key="a") is None

    def test_get_returns_a_matching_row_when_several_match(self, seam):
        """Which one is not contract; that it is one of them, and not an error,
        is."""
        reader = self._reader(seam, self._rows(seam, "a", "b"))

        row = reader.get(seam.model, stream_key="s")
        assert row is not None
        assert row.field_key in ("a", "b")

    def test_get_with_no_lookup_is_the_whole_model(self, seam):
        reader = self._reader(seam, self._rows(seam, "a"))

        assert reader.get(seam.model) is not None

    # -- filter -------------------------------------------------------------

    def test_filter_yields_the_matching_rows(self, seam):
        reader = self._reader(seam, self._rows(seam, "a", "b", "c"))

        result = reader.filter(seam.model, stream_key="s", alert_type="machine")
        assert isinstance(result, Iterable)
        assert sorted(r.field_key for r in result) == ["a", "b", "c"]

    def test_filter_narrows_to_the_lookup(self, seam):
        reader = self._reader(seam, self._rows(seam, "a", "b"))

        assert [r.field_key for r in reader.filter(seam.model, field_key="b")] == ["b"]

    def test_filter_yields_nothing_when_nothing_matches(self, seam):
        reader = self._reader(seam, self._rows(seam, "a"))

        assert list(reader.filter(seam.model, field_key="nope")) == []

    def test_filter_is_re_iterable(self, seam):
        """A queryset-like result can be walked more than once — handlers do
        (count, then loop), and a bare generator would silently come back empty
        the second time."""
        reader = self._reader(seam, self._rows(seam, "a", "b"))

        result = reader.filter(seam.model, stream_key="s")
        first = sorted(r.field_key for r in result)
        second = sorted(r.field_key for r in result)
        assert first == second == ["a", "b"]

    # -- query --------------------------------------------------------------

    def test_query_yields_every_row_of_the_model(self, seam):
        reader = self._reader(seam, self._rows(seam, "a", "b"))

        result = reader.query(seam.model)
        assert isinstance(result, Iterable)
        assert sorted(r.field_key for r in result) == ["a", "b"]

    def test_query_of_an_empty_projection_yields_nothing(self, seam):
        reader = self._reader(seam, [])

        assert list(reader.query(seam.model)) == []
