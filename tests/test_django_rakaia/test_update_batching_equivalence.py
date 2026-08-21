"""`batch_updates` must change the statement count and nothing else.

`test_update_batching.py` checks *where* the collapse happens, case by case. This
file checks the one property that makes the flag safe to turn on, and it checks it
by measurement rather than by argument: **run the same effects down both paths and
compare what the columns hold.**

That distinction matters because the guard on this feature was wrong four times,
and every one of those was a case that had been reasoned about in Python and never
run against a column. `Decimal("1.0") == Decimal("1.00")` is a true statement
about Python that says nothing about what a `DecimalField(decimal_places=2)`
stores. So the cases below are organised by what is *observable*, and the ones
that turn out not to be observable say so.

The comparison is on `repr` of every column, not `==`: `0.0` and `-0.0` are equal
and `Decimal("1.0")` and `Decimal("1.00")` are equal, which is the whole hazard
under test. An `==` comparison here would agree with itself and prove nothing.
"""

from __future__ import annotations

import datetime as dt
import enum
import uuid
from contextlib import contextmanager
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from django.db import connections, models
from django.db.models import F
from django.test.utils import CaptureQueriesContext

from django_rakaia.effect_executor import DjangoExecutor
from rakaia.effects import Update

from .models import History, Measure, SukuProjection

pytestmark = pytest.mark.django_db(databases=["default", "overlay"])

HISTORY = "test_django_rakaia.History"
MEASURE = "test_django_rakaia.Measure"
SUKU = "test_django_rakaia.SukuProjection"

#: The unbatched path runs here, the batched path there. Two aliases give two
#: independent copies of one schema, which is what lets the same effects be
#: applied twice from the same starting state. Both run the same backend, and the
#: executor is alias-agnostic apart from `using=`, so a difference between the two
#: snapshots is a difference the flag made.
PLAIN, BATCHED = "default", "overlay"


class Marker(models.TextChoices):
    """A `str` subclass, which is what every idiomatic Django choices field is."""

    YES = "y", "Yes"


class Actor(models.IntegerChoices):
    SYSTEM = 1, "System"


class Bare(enum.Enum):
    """No mixin, so equality is identity."""

    YES = "y"


class _Ci(str):
    """A `str` subclass with its own equality — the thing the value-type test
    exists to keep out, since a subclass may define equality however it likes."""

    def __eq__(self, other: object) -> bool:
        return str.lower(self) == str.lower(other)  # type: ignore[arg-type]

    def __hash__(self) -> int:
        return hash(str.lower(self))


class Subclassed(_Ci, enum.Enum):
    YES = _Ci("y")


class LooseEq(str, enum.Enum):
    """Plain `str` values, but equality overridden on the enum itself — so two
    members with values 'y' and 'Y' compare and hash alike."""

    LOWER = "y"
    UPPER = "Y"

    def __eq__(self, other: object) -> bool:
        return str.lower(self) == str.lower(other)  # type: ignore[arg-type]

    def __hash__(self) -> int:
        return hash(str.lower(self))


def _columns(model: type) -> list[str]:
    return [f.name for f in model._meta.concrete_fields if not f.primary_key]


def _snapshot(model: type, alias: str) -> list[str]:
    """Every row's every column, as `repr`, order-independent."""
    cols = _columns(model)
    return sorted(
        repr({c: repr(getattr(row, c)) for c in cols})
        for row in model.objects.using(alias).all()
    )


def _seed(model: type, rows: list[dict]) -> None:
    for alias in (PLAIN, BATCHED):
        for row in rows:
            model.objects.using(alias).create(**row)


def _apply(alias: str, effects: list[Update], *, batch: bool) -> int:
    with CaptureQueriesContext(connections[alias]) as ctx:
        DjangoExecutor(using=alias, batch_updates=batch).apply(effects)
    return sum(
        1
        for q in ctx.captured_queries
        if q["sql"].lstrip().upper().startswith("UPDATE")
    )


def _both_paths(
    model: type, effects: list[Update]
) -> tuple[list[str], list[str], int, int]:
    plain_n = _apply(PLAIN, effects, batch=False)
    batched_n = _apply(BATCHED, effects, batch=True)
    return _snapshot(model, PLAIN), _snapshot(model, BATCHED), plain_n, batched_n


def assert_flag_changes_only_the_statement_count(
    model: type, effects: list[Update], *, expect_collapse: bool
) -> None:
    """The invariant, plus the check that the case is not vacuous.

    `expect_collapse` is not decoration. A case where the collapse declined and a
    case where it ran both leave the columns identical, so without asserting
    which happened, every test here would pass against an executor that ignored
    the flag entirely.
    """
    plain, batched, plain_n, batched_n = _both_paths(model, effects)
    assert batched == plain, (
        f"the flag changed what was stored\n  off: {plain}\n  on:  {batched}"
    )
    if expect_collapse:
        assert batched_n < plain_n, (
            f"expected a collapse but both paths issued {plain_n} UPDATEs — the "
            f"equality assertion above is therefore vacuous"
        )
    else:
        assert batched_n == plain_n, (
            f"expected no collapse, but the batched path issued {batched_n} "
            f"UPDATEs against {plain_n}"
        )


@contextmanager
def collapse_everything_hashable():
    """The guard's *first* shape — no allowlist at all — so a hazard can be shown
    to be a hazard.

    An exclusion nothing can distinguish from its absence is an exclusion no test
    can defend. Forcing the collapse is how these cases tell "this type is unsafe"
    apart from "this type was never examined".
    """
    saved = DjangoExecutor.__dict__["_batch_key"]

    def unguarded(eff: Update):
        defaults = eff.defaults or {}
        if not defaults or len(eff.lookup) != 1:
            return None
        ((field, value),) = eff.lookup.items()
        if "__" in field or value is None:
            return None
        try:
            return (
                eff.model_label,
                field,
                frozenset((k, type(v), v) for k, v in defaults.items()),
            )
        except TypeError:
            return None

    DjangoExecutor._batch_key = staticmethod(unguarded)  # type: ignore[method-assign]
    try:
        yield
    finally:
        DjangoExecutor._batch_key = saved  # type: ignore[method-assign]


def _history(*ids: str, **overrides) -> list[dict]:
    base = {"version": 1, "marker": "+", "actor": None, "ts": 0.0, "snapshot": {}}
    return [dict(base, submission_id=i, **overrides) for i in ids]


def _upd(label: str, lookup: dict, **defaults) -> Update:
    return Update(model_label=label, lookup=lookup, defaults=defaults)


class TestWhatCollapses:
    def test_a_fanned_out_literal_update(self):
        # The case #199 asked for: one logical change, one Update per row.
        _seed(History, _history("a", "b", "c"))
        assert_flag_changes_only_the_statement_count(
            History,
            [_upd(HISTORY, {"submission_id": i}, marker="y") for i in "abc"],
            expect_collapse=True,
        )

    def test_row_sets_that_overlap_because_the_lookup_field_is_written(self):
        # The safety argument, measured. Effect 1 moves `a` into effect 2's
        # lookup, so unbatched the second statement matches both rows — and with
        # a literal, writing it twice is writing it once.
        _seed(History, [*_history("a", marker="x"), *_history("b", marker="y")])
        assert_flag_changes_only_the_statement_count(
            History,
            [
                _upd(HISTORY, {"marker": "x"}, marker="y"),
                _upd(HISTORY, {"marker": "y"}, marker="y"),
            ],
            expect_collapse=True,
        )

    def test_lookup_values_of_different_types(self):
        # Only `defaults` are allowlisted; a lookup value can be anything. Django
        # coerces both to the column's type, so `"1"` and `1` gather safely.
        _seed(History, _history("1", "2"))
        assert_flag_changes_only_the_statement_count(
            History,
            [
                _upd(HISTORY, {"submission_id": "1"}, marker="y"),
                _upd(HISTORY, {"submission_id": 2}, marker="y"),
            ],
            expect_collapse=True,
        )

    def test_a_null_default_on_a_nullable_column(self):
        _seed(SukuProjection, [{"suku": s, "status": "old"} for s in ("s1", "s2")])
        assert_flag_changes_only_the_statement_count(
            SukuProjection,
            [_upd(SUKU, {"suku": s}, status=None) for s in ("s1", "s2")],
            expect_collapse=True,
        )


class TestWhatDoesNotCollapse:
    """Each of these leaves the columns identical *because it declined*. The
    `expect_collapse=False` half of the assertion is what makes that claim, and
    `TestWhichExclusionsAreLoadBearing` is what says whether it mattered."""

    def test_an_expression_default(self):
        _seed(History, _history("a", "b", version=5))
        assert_flag_changes_only_the_statement_count(
            History,
            [
                _upd(HISTORY, {"submission_id": i}, version=F("version") + 1)
                for i in "ab"
            ],
            expect_collapse=False,
        )

    def test_a_null_lookup_value_mixed_with_a_real_one(self):
        # `filter(f=None)` is `f IS NULL`, and Django strips None from an `__in`
        # right-hand side — so gathering these two would emit `status IN ('x')`
        # and the null-matching effect would write nothing at all.
        #
        # Note the shape. Two effects can only *group* if they share a field and
        # their defaults, so a pair of null-lookup effects would be identical and
        # `check_disjoint_defaults` rejects them before this is reached. The
        # reachable hazard is a null alongside a value — which is what a reconcile
        # over an open `resolved_at IS NULL` predicate produces.
        _seed(
            SukuProjection,
            [{"suku": "s1", "status": None}, {"suku": "s2", "status": "x"}],
        )
        assert_flag_changes_only_the_statement_count(
            SukuProjection,
            [
                _upd(SUKU, {"status": None}, ksp_total=1),
                _upd(SUKU, {"status": "x"}, ksp_total=1),
            ],
            expect_collapse=False,
        )

    def test_an_out_of_range_integer_lookup_value(self):
        _seed(History, _history("a", "b", actor=7))
        assert_flag_changes_only_the_statement_count(
            History,
            [
                _upd(HISTORY, {"actor": 7}, marker="y"),
                _upd(HISTORY, {"actor": 2**70}, marker="y"),
            ],
            expect_collapse=False,
        )

    def test_a_composite_lookup(self):
        _seed(History, _history("a", "b"))
        assert_flag_changes_only_the_statement_count(
            History,
            [
                _upd(HISTORY, {"submission_id": i, "version": 1}, marker="y")
                for i in "ab"
            ],
            expect_collapse=False,
        )

    def test_a_traversing_lookup(self):
        _seed(History, _history("aa", "ab"))
        assert_flag_changes_only_the_statement_count(
            History,
            [
                _upd(HISTORY, {"submission_id__startswith": p}, marker="y")
                for p in ("aa", "ab")
            ],
            expect_collapse=False,
        )

    def test_an_unhashable_default(self):
        _seed(History, _history("a", "b"))
        assert_flag_changes_only_the_statement_count(
            History,
            [_upd(HISTORY, {"submission_id": i}, snapshot={"k": 1}) for i in "ab"],
            expect_collapse=False,
        )

    def test_an_enum_over_a_str_subclass(self):
        # `v.value` is an instance of the subclass rather than of `str`, so the
        # value-type test declines — and it should, because the subclass is free to
        # define equality however it likes.
        _seed(History, _history("a", "b"))
        assert_flag_changes_only_the_statement_count(
            History,
            [_upd(HISTORY, {"submission_id": i}, marker=Subclassed.YES) for i in "ab"],
            expect_collapse=False,
        )

    def test_an_enum_that_overrides_equality(self):
        # Plain `str` values, so the value-type test passes — but the enum's own
        # case-insensitive `__eq__`/`__hash__` would make two members with values
        # 'y' and 'Y' group, and one member's value would reach the other's rows.
        _seed(History, _history("a", "b"))
        assert_flag_changes_only_the_statement_count(
            History,
            [_upd(HISTORY, {"submission_id": i}, marker=LooseEq.LOWER) for i in "ab"],
            expect_collapse=False,
        )

    def test_a_plain_enum_with_no_mixin(self):
        # Identity equality, which is *stricter* than value equality and so would
        # be safe to collapse — declined anyway, because Django has no sensible
        # column form for it and admitting it buys nothing. Note what it does
        # store: `str(Bare.YES)` is `'Bare.YES'`, not `'y'`. That needs a column
        # with room, which is why this case is on `status` (16) rather than
        # `History.marker` (1) — Postgres rejects the overlong value and SQLite
        # quietly accepts it, so on the default leg the first version of this test
        # passed for the wrong reason.
        _seed(SukuProjection, [{"suku": s, "status": "old"} for s in ("s1", "s2")])
        assert_flag_changes_only_the_statement_count(
            SukuProjection,
            [_upd(SUKU, {"suku": s}, status=Bare.YES) for s in ("s1", "s2")],
            expect_collapse=False,
        )


class TestDjangoChoicesFields:
    """`models.TextChoices` and `models.IntegerChoices` are the idiomatic way to
    write a choices column, and a status change fanned across many rows is the
    single commonest shape of the case this feature exists for.

    They were excluded until the widening in `_is_collapsible_value`, for a reason
    that was an accident rather than a decision: `type(v)` is the enum class, not
    `str`, so the exact-type test never matched. The feature declined on exactly
    what it was built for.
    """

    def test_a_text_choices_default_collapses(self):
        _seed(History, _history("a", "b", "c"))
        assert_flag_changes_only_the_statement_count(
            History,
            [_upd(HISTORY, {"submission_id": i}, marker=Marker.YES) for i in "abc"],
            expect_collapse=True,
        )

    def test_it_stores_the_member_value_not_its_repr(self):
        # The equality assertion above compares the two paths against each other,
        # so it would be satisfied by both storing `'Marker.YES'`. This says what
        # the column actually holds.
        _seed(History, _history("a", "b"))
        DjangoExecutor(using=BATCHED, batch_updates=True).apply(
            [_upd(HISTORY, {"submission_id": i}, marker=Marker.YES) for i in "ab"]
        )
        assert set(History.objects.using(BATCHED).values_list("marker", flat=True)) == {
            "y"
        }

    def test_an_integer_choices_default_collapses(self):
        _seed(History, _history("a", "b", "c"))
        assert_flag_changes_only_the_statement_count(
            History,
            [_upd(HISTORY, {"submission_id": i}, actor=Actor.SYSTEM) for i in "abc"],
            expect_collapse=True,
        )

    def test_a_member_and_its_bare_value_do_not_group(self):
        # `Marker.YES == "y"` and they hash alike, but `type(v)` is part of the
        # grouping key, so these stay apart. Declining is the safe direction and
        # this pins it: grouping them would rest on the enum's equality agreeing
        # with `str`'s, which `_is_collapsible_value` checks for the *member* but
        # has no reason to trust across two different types.
        _seed(History, _history("a", "b"))
        assert_flag_changes_only_the_statement_count(
            History,
            [
                _upd(HISTORY, {"submission_id": "a"}, marker=Marker.YES),
                _upd(HISTORY, {"submission_id": "b"}, marker="y"),
            ],
            expect_collapse=False,
        )


class TestWhichExclusionsAreLoadBearing:
    """Force the collapse and see whether anything actually changes.

    This is the part that was missing when the guard was wrong four times: each
    narrowing was justified by a Python-level counterexample, and nobody ran it
    against a column to find out whether the column could tell the difference.
    """

    def test_an_expression_over_overlapping_rows_is_the_hazard(self):
        # `a` starts at 1 and `b` at 2. Unbatched: effect 1 moves `a` to 2, so
        # effect 2 matches both and both reach 3. Batched: one statement over
        # {1,2} increments each once, so `a` stops at 2. This is the difference
        # the whole guard exists to prevent, and it needs *both* halves —
        # overlap and a non-idempotent write.
        _seed(History, [*_history("a", version=1), *_history("b", version=2)])
        effects = [
            _upd(HISTORY, {"version": 1}, version=F("version") + 1),
            _upd(HISTORY, {"version": 2}, version=F("version") + 1),
        ]
        with collapse_everything_hashable():
            plain, batched, _, batched_n = _both_paths(History, effects)
        assert batched_n == 1, "the collapse did not happen, so nothing is demonstrated"
        assert batched != plain, (
            "forcing the collapse of an expression over overlapping rows did not "
            "change the result — the exclusion this pins would be unnecessary"
        )

    def test_the_same_expression_over_disjoint_rows_is_not(self):
        # Non-idempotency alone is harmless: without overlap each row is
        # incremented once either way. The guard is broader than the hazard,
        # which is the right direction, but it is worth knowing why.
        _seed(History, _history("a", "b", version=5))
        effects = [
            _upd(HISTORY, {"submission_id": i}, version=F("version") + 1) for i in "ab"
        ]
        with collapse_everything_hashable():
            plain, batched, _, batched_n = _both_paths(History, effects)
        assert batched_n == 1
        assert batched == plain

    @pytest.mark.parametrize(
        ("label", "model", "seed", "effects"),
        [
            pytest.param(
                "float -0.0 vs 0.0",
                History,
                _history("a", "b", ts=9.0),
                [
                    _upd(HISTORY, {"submission_id": "a"}, ts=0.0),
                    _upd(HISTORY, {"submission_id": "b"}, ts=-0.0),
                ],
                id="float-signed-zero",
            ),
            pytest.param(
                "Decimal 1.0 vs 1.00",
                Measure,
                [{"ref": uuid.UUID(int=i), "amount": Decimal("9.99")} for i in (1, 2)],
                [
                    _upd(MEASURE, {"ref": uuid.UUID(int=1)}, amount=Decimal("1.0")),
                    _upd(MEASURE, {"ref": uuid.UUID(int=2)}, amount=Decimal("1.00")),
                ],
                id="decimal-scale",
            ),
            pytest.param(
                "one instant, two zones",
                Measure,
                [{"ref": uuid.UUID(int=i), "amount": 0} for i in (1, 2)],
                [
                    _upd(
                        MEASURE,
                        {"ref": uuid.UUID(int=1)},
                        observed_at=dt.datetime(2026, 1, 1, 12, tzinfo=ZoneInfo("UTC")),
                    ),
                    _upd(
                        MEASURE,
                        {"ref": uuid.UUID(int=2)},
                        observed_at=dt.datetime(
                            2026, 1, 1, 12, tzinfo=ZoneInfo("UTC")
                        ).astimezone(ZoneInfo("Asia/Dili")),
                    ),
                ],
                id="aware-datetime-zones",
            ),
        ],
    )
    def test_representation_hazards_are_not_observable_through_the_orm(
        self, label, model, seed, effects
    ):
        """A tripwire, not a licence.

        Each of these is a pair of values Python calls equal and hash-equal, so
        forcing the collapse writes one member's value to the other's row. On both
        backends the column cannot tell: Django coerces an aware datetime to UTC,
        the `DecimalField`'s scale erases the trailing zero, `-0.0` round-trips as
        `0.0`, and `True` reaches an integer column as `1`.

        So these types are excluded on a Python-level argument the database does
        not support. That is still the right call — the allowlist is conservative
        by design and a wrong entry costs a missed collapse — but it should be
        recorded honestly rather than as a demonstrated counterexample.

        The assertion is the useful part: if a Django release, a backend, or a
        column type change makes one of these observable, this goes red, and the
        exclusion becomes load-bearing for a reason a reader can then check.
        """
        _seed(model, seed)
        with collapse_everything_hashable():
            plain, batched, _, batched_n = _both_paths(model, effects)
        assert batched_n == 1, f"{label}: the collapse did not happen"
        assert batched == plain, (
            f"{label} IS now observable through the ORM — the exclusion has "
            f"become load-bearing, and the docstring should say so:\n"
            f"  off: {plain}\n  on:  {batched}"
        )

    def test_the_value_type_in_the_key_is_not_load_bearing_either(self):
        """`type(v)` is part of the grouping key so that `1` and `True` do not
        group. Dropping it, they do — and the column still cannot tell.

        A separate forcing from the allowlist one, because these three are kept
        apart by the key's `type(v)` component rather than by
        `_COLLAPSIBLE_DEFAULTS`: both types are on the allowlist. Same
        conclusion as the cases above, reached a different way: Django coerces
        `True` to the integer column's `1` on the way in, so writing one
        member's value to the other's row is invisible.
        """
        saved = DjangoExecutor.__dict__["_batch_key"]

        def type_blind(eff: Update):
            defaults = eff.defaults or {}
            if not defaults or len(eff.lookup) != 1:
                return None
            ((field, value),) = eff.lookup.items()
            if "__" in field or value is None:
                return None
            if not all(
                type(v) in (type(None), bool, int, str, bytes)
                for v in defaults.values()
            ):
                return None
            return (eff.model_label, field, frozenset(defaults.items()))

        _seed(History, _history("a", "b"))
        effects = [
            _upd(HISTORY, {"submission_id": "a"}, actor=1),
            _upd(HISTORY, {"submission_id": "b"}, actor=True),
        ]
        DjangoExecutor._batch_key = staticmethod(type_blind)  # type: ignore[method-assign]
        try:
            plain, batched, _, batched_n = _both_paths(History, effects)
        finally:
            DjangoExecutor._batch_key = saved  # type: ignore[method-assign]

        assert batched_n == 1, "the collapse did not happen"
        assert batched == plain, (
            '`1`/`True`/`"1"` are now distinguishable once stored, so the '
            f"`type(v)` component of the key has become load-bearing:\n"
            f"  off: {plain}\n  on:  {batched}"
        )

    def test_an_enum_overriding_equality_is_a_real_hazard(self):
        """The `__eq__` half of the enum widening, demonstrated rather than argued.

        `LooseEq.LOWER` ('y') and `LooseEq.UPPER` ('Y') compare and hash alike, so
        with the guard off they group and one member's value reaches the other's
        rows. Unlike the representation cases above, the column *can* tell: these
        are two different strings.

        This is why admitting `TextChoices` is a check on the member's equality and
        not simply `isinstance(v, str)`.
        """
        _seed(History, _history("a", "b"))
        effects = [
            _upd(HISTORY, {"submission_id": "a"}, marker=LooseEq.LOWER),
            _upd(HISTORY, {"submission_id": "b"}, marker=LooseEq.UPPER),
        ]
        with collapse_everything_hashable():
            plain, batched, _, batched_n = _both_paths(History, effects)
        assert batched_n == 1, "the collapse did not happen, so nothing is demonstrated"
        assert batched != plain, (
            "grouping two members of a loosely-equal enum did not change what was "
            "stored — the equality check in `_is_collapsible_value` would then be "
            f"unnecessary:\n  off: {plain}\n  on:  {batched}"
        )
