"""Shared conformance contract for the effect executor surface (`Executor`).

Rakaia has more than one executor that actually *applies* effects — the durable
`DjangoExecutor` and the in-memory `InMemoryProjections` — and handlers are written
against whichever one the demo, test or app happens to hold. "Test on the
in-memory executor, ship on the Django one" is only safe if both converge to the
same rows from the same batch, so this is the executor twin of
`tests/store_contract.py`. Subclass `ExecutorContract` in each backend's test
package and provide a `seam` fixture returning an :class:`ExecutorSeam`. See
ADR 0002 / #121.

This module is intentionally not named `test_*`, so pytest does not collect it
directly; only the backend subclasses run it.

**What it pins.** The invariants a handler author relies on without being able to
see the executor:

* three ordered passes over the whole batch — every write, then every delete,
  then every retire — so convergence never depends on handler emission order;
* a `produces=` row is therefore recorded before any effect that `Ref`s it, even
  one emitted earlier in the batch;
* exactly one `RefResolver` per `apply()` call, so a `Ref` resolves within a
  batch and never across two;
* `check_disjoint_defaults` runs before any write, so a colliding batch raises
  with nothing applied;
* `retire_flips` reported only for retires that opted in with a `Transition`.

**What it does not pin, and why.**

* *The return value.* `DjangoExecutor` returns an `ApplyReport`; an executor that
  observes no flips may return `None`, which `rakaia.effects.Executor` explicitly
  permits and the replay orchestrator treats as empty. So the suite asserts
  read-back row state, and inspects a report only when one is returned.
* *`skip_unchanged`.* A Django-only write-churn optimisation whose only
  observable difference is which UPDATEs are issued (`auto_now`, `post_save`) —
  a query-count property, not a converged-state one.
* *`using=` alias routing.* Django-specific: the in-memory executor has a single
  namespace, so "same batch, other database" has no meaning for it. Covered for
  the Django side by `tests/test_django_rakaia/test_using_seam.py`.
* *Transaction rollback.* `DjangoExecutor` wraps a batch in `transaction.atomic`;
  the in-memory one has no transaction. The one all-or-nothing property that is
  contract — a colliding batch applying nothing — holds for both because
  `check_disjoint_defaults` runs *before* the first write, not because of the
  transaction.
* *Lookup operator coverage.* The suite uses exact match, `__in` and `__isnull`
  only. Everything richer (spanning lookups, `__gte`, `Q` objects) is ORM
  surface the in-memory executor deliberately does not reimplement.
* *Relational behaviour.* FKs, cascades and uniqueness constraints are database
  features, not executor ones.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from rakaia.effects import (
    Delete,
    Effect,
    EffectCollisionError,
    Exclude,
    ExternalEffect,
    Ref,
    Retire,
    SpareKeys,
    Transition,
    UnresolvedRefError,
    Update,
    Upsert,
)


@dataclass(frozen=True)
class ExecutorSeam:
    """The one seam this contract needs: something to apply with, something to
    read back through, and a projection to write to.

    `model` must name a projection whose columns are the ones the suite writes:
    ``stream_key`` (str), ``alert_type`` (str), ``field_key`` (str),
    ``severity`` (str), ``message`` (str), ``resolved_at`` (str or NULL,
    defaulting to NULL) and ``dismissed_version`` (int or NULL, defaulting to
    NULL). The Django binding points it at the `Alert` test model; the in-memory
    binding can point it anywhere.
    """

    executor: Any
    """The `Executor` under test."""

    reader: Any
    """A `ProjectionReader` over the rows `executor` writes."""

    model: str
    """`model_label` of the projection described above."""


class ExecutorContract:
    """Contract every applying executor must uphold.

    Subclasses provide::

        @pytest.fixture
        def seam(self):
            return ExecutorSeam(
                executor=MyExecutor(), reader=MyReader(), model="app.Alert"
            )
    """

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _upsert(seam: ExecutorSeam, key: str, **rest: Any) -> Effect:
        """An upsert of one row of `seam.model`, natural-keyed on `field_key`."""
        return Upsert(
            model_label=seam.model,
            lookup={"stream_key": "s", "alert_type": "machine", "field_key": key},
            defaults={"severity": "error", "message": f"about {key}", **rest},
        )

    @staticmethod
    def _keys(seam: ExecutorSeam, **lookup: Any) -> list[str]:
        """The `field_key` of every matching row, sorted — the read-back state."""
        return sorted(r.field_key for r in seam.reader.filter(seam.model, **lookup))

    # -- protocol -----------------------------------------------------------

    def test_satisfies_the_executor_protocol(self, seam):
        from rakaia import Executor

        assert isinstance(seam.executor, Executor)

    # -- the individual ops -------------------------------------------------

    def test_upsert_creates_then_converges(self, seam):
        """Re-applying the same batch is a no-op, not a second row — the
        property that makes replay safe to run twice."""
        seam.executor.apply([self._upsert(seam, "a")])
        seam.executor.apply([self._upsert(seam, "a")])

        assert self._keys(seam) == ["a"]
        assert seam.reader.get(seam.model, field_key="a").message == "about a"

    def test_upsert_updates_an_existing_row_in_place(self, seam):
        seam.executor.apply([self._upsert(seam, "a", severity="info")])
        seam.executor.apply([self._upsert(seam, "a", severity="error")])

        assert seam.reader.get(seam.model, field_key="a").severity == "error"

    def test_update_writes_matching_rows_and_never_inserts(self, seam):
        """`Update` is the update-if-exists primitive a secondary owner of a
        multi-owned row emits unconditionally: it must write what is there and
        mint nothing when there is not."""
        seam.executor.apply([self._upsert(seam, "a")])
        seam.executor.apply(
            [
                Update(
                    model_label=seam.model,
                    lookup={"field_key": "a"},
                    defaults={"severity": "warning"},
                ),
                Update(
                    model_label=seam.model,
                    lookup={"field_key": "ghost"},
                    defaults={"severity": "warning"},
                ),
            ]
        )

        assert self._keys(seam) == ["a"]
        assert seam.reader.get(seam.model, field_key="a").severity == "warning"

    def test_update_with_empty_defaults_is_a_noop(self, seam):
        seam.executor.apply([self._upsert(seam, "a")])
        seam.executor.apply(
            [Update(model_label=seam.model, lookup={"field_key": "a"}, defaults={})]
        )

        assert seam.reader.get(seam.model, field_key="a").severity == "error"

    def test_delete_removes_the_rows_in_scope(self, seam):
        seam.executor.apply([self._upsert(seam, "a"), self._upsert(seam, "b")])
        seam.executor.apply([Delete(model_label=seam.model, lookup={"field_key": "a"})])

        assert self._keys(seam) == ["b"]

    def test_delete_spares_rows_matched_by_exclude(self, seam):
        """The flat, positional `reconcile_children` primitive."""
        seam.executor.apply(
            [self._upsert(seam, k) for k in ("a", "b", "c")],
        )
        seam.executor.apply(
            [
                Delete(
                    model_label=seam.model,
                    lookup={"stream_key": "s"},
                    spare=Exclude({"field_key__in": ["a", "c"]}),
                )
            ]
        )

        assert self._keys(seam) == ["a", "c"]

    def test_delete_spares_composite_natural_keys(self, seam):
        """The `SpareKeys` primitive: delete the whole scope *except* these."""
        seam.executor.apply([self._upsert(seam, k) for k in ("a", "b", "c")])
        seam.executor.apply(
            [
                Delete(
                    model_label=seam.model,
                    lookup={"stream_key": "s"},
                    spare=SpareKeys([{"alert_type": "machine", "field_key": "b"}]),
                )
            ]
        )

        assert self._keys(seam) == ["b"]

    def test_delete_with_empty_spare_keys_spares_nothing(self, seam):
        seam.executor.apply([self._upsert(seam, "a")])
        seam.executor.apply(
            [
                Delete(
                    model_label=seam.model,
                    lookup={"stream_key": "s"},
                    spare=SpareKeys([]),
                )
            ]
        )

        assert self._keys(seam) == []

    def test_retire_patches_rows_instead_of_deleting_them(self, seam):
        """A retire soft-deletes: the row survives, its liveness sentinel is set."""
        seam.executor.apply([self._upsert(seam, "a"), self._upsert(seam, "b")])
        seam.executor.apply(
            [
                Retire(
                    model_label=seam.model,
                    lookup={"stream_key": "s", "resolved_at__isnull": True},
                    patch={"resolved_at": "2024-01-01T00:00:00"},
                    spare=SpareKeys([{"alert_type": "machine", "field_key": "b"}]),
                )
            ]
        )

        assert self._keys(seam) == ["a", "b"]  # nothing deleted
        assert self._keys(seam, resolved_at__isnull=True) == ["b"]
        assert seam.reader.get(seam.model, field_key="a").resolved_at == (
            "2024-01-01T00:00:00"
        )

    def test_external_effects_are_ignored(self, seam):
        """Replay decides whether externals reach an executor; one that arrives
        is dropped, never applied and never an error."""
        seam.executor.apply(
            [
                ExternalEffect(kind="email", payload={"to": "a@b.c"}),
                self._upsert(seam, "a"),
            ]
        )

        assert self._keys(seam) == ["a"]

    # -- the three ordered passes -------------------------------------------
    #
    # Each of these emits a batch whose result differs depending on whether the
    # executor walks it once in order or three times by op. They are written so
    # the in-order answer is the *wrong* one, which is the only way to pin the
    # ordering from outside.

    def test_every_write_applies_before_any_delete(self, seam):
        """A delete emitted first still runs after the upsert emitted second, so
        the freshly written row is inside its scope and goes."""
        seam.executor.apply(
            [
                Delete(model_label=seam.model, lookup={"stream_key": "s"}),
                self._upsert(seam, "a"),
            ]
        )

        assert self._keys(seam) == []  # in emission order this would be ["a"]

    def test_every_write_applies_before_any_retire(self, seam):
        seam.executor.apply(
            [
                Retire(
                    model_label=seam.model,
                    lookup={"stream_key": "s"},
                    patch={"resolved_at": "2024-01-01T00:00:00"},
                ),
                self._upsert(seam, "a"),
            ]
        )

        assert self._keys(seam, resolved_at__isnull=True) == []

    def test_every_delete_applies_before_any_retire(self, seam):
        """Deletes are the middle pass, so a row in both scopes is gone before
        the retire could have soft-deleted it."""
        seam.executor.apply([self._upsert(seam, "a")])
        seam.executor.apply(
            [
                Retire(
                    model_label=seam.model,
                    lookup={"stream_key": "s"},
                    patch={"resolved_at": "2024-01-01T00:00:00"},
                ),
                Delete(model_label=seam.model, lookup={"stream_key": "s"}),
            ]
        )

        assert self._keys(seam) == []  # deleted outright, not retained as retired

    def test_reconcile_converges_regardless_of_emission_order(self, seam):
        """The property the passes exist for: the same reconcile batch, emitted
        two different ways, lands on the same rows."""
        prune = Delete(
            model_label=seam.model,
            lookup={"stream_key": "s"},
            spare=SpareKeys([{"field_key": "a"}, {"field_key": "b"}]),
        )
        current = [self._upsert(seam, "a"), self._upsert(seam, "b")]

        seam.executor.apply([self._upsert(seam, "stale")])
        seam.executor.apply([prune, *current])
        prune_first = self._keys(seam)

        seam.executor.apply([Delete(model_label=seam.model, lookup={})])
        seam.executor.apply([self._upsert(seam, "stale")])
        seam.executor.apply([*current, prune])

        assert prune_first == self._keys(seam) == ["a", "b"]

    # -- refs ---------------------------------------------------------------

    def test_ref_binds_to_the_row_a_sibling_produced(self, seam):
        seam.executor.apply(
            [
                Upsert(
                    model_label=seam.model,
                    lookup={"stream_key": "s", "alert_type": "m", "field_key": "a"},
                    defaults={"severity": "error"},
                    produces="anchor",
                ),
                Upsert(
                    model_label=seam.model,
                    lookup={"stream_key": "s", "alert_type": "m", "field_key": "b"},
                    defaults={"dismissed_version": Ref("anchor")},
                ),
            ]
        )

        anchor = seam.reader.get(seam.model, field_key="a")
        assert seam.reader.get(seam.model, field_key="b").dismissed_version == anchor.pk

    def test_ref_can_name_a_column_other_than_the_primary_key(self, seam):
        seam.executor.apply(
            [
                Upsert(
                    model_label=seam.model,
                    lookup={"stream_key": "s", "alert_type": "m", "field_key": "a"},
                    defaults={"severity": "critical"},
                    produces="anchor",
                ),
                Upsert(
                    model_label=seam.model,
                    lookup={"stream_key": "s", "alert_type": "m", "field_key": "b"},
                    defaults={"severity": Ref("anchor", "severity")},
                ),
            ]
        )

        assert seam.reader.get(seam.model, field_key="b").severity == "critical"

    def test_a_ref_from_a_delete_emitted_first_still_resolves(self, seam):
        """Refs are order-free for the same reason the ops are: the producing
        write has already run by the time the delete pass starts."""
        seam.executor.apply(
            [
                Delete(
                    model_label=seam.model,
                    lookup={"alert_type": Ref("anchor", "alert_type")},
                ),
                Upsert(
                    model_label=seam.model,
                    lookup={"stream_key": "s", "alert_type": "m", "field_key": "a"},
                    defaults={"severity": "error"},
                    produces="anchor",
                ),
            ]
        )

        assert self._keys(seam) == []

    def test_refs_do_not_resolve_across_apply_calls(self, seam):
        """One resolver per `apply()`: a batch is the whole scope of a `Ref`, so
        a second batch cannot silently bind to the first one's row."""
        seam.executor.apply(
            [
                Upsert(
                    model_label=seam.model,
                    lookup={"stream_key": "s", "alert_type": "m", "field_key": "a"},
                    defaults={"severity": "error"},
                    produces="anchor",
                )
            ]
        )

        with pytest.raises(UnresolvedRefError):
            seam.executor.apply(
                [
                    Upsert(
                        model_label=seam.model,
                        lookup={
                            "stream_key": "s",
                            "alert_type": "m",
                            "field_key": "b",
                        },
                        defaults={"dismissed_version": Ref("anchor")},
                    )
                ]
            )

    # -- collision detection ------------------------------------------------

    def test_colliding_batch_raises_with_nothing_applied(self, seam):
        """Two owners writing the same column of the same row is always a bug,
        and it is caught *before* the first write — so the batch is all or
        nothing even without a transaction."""
        with pytest.raises(EffectCollisionError):
            seam.executor.apply(
                [
                    self._upsert(seam, "a", severity="info"),
                    self._upsert(seam, "b"),
                    self._upsert(seam, "a", severity="error"),
                ]
            )

        assert self._keys(seam) == []

    def test_disjoint_columns_on_one_row_are_allowed(self, seam):
        """The other half of the multi-owner invariant: two owners may write the
        same row as long as their columns do not overlap."""
        lookup = {"stream_key": "s", "alert_type": "m", "field_key": "a"}
        seam.executor.apply(
            [
                Upsert(
                    model_label=seam.model,
                    lookup=lookup,
                    defaults={"severity": "error"},
                ),
                Update(
                    model_label=seam.model,
                    lookup=lookup,
                    defaults={"message": "from the other owner"},
                ),
            ]
        )

        row = seam.reader.get(seam.model, field_key="a")
        assert (row.severity, row.message) == ("error", "from the other owner")

    # -- retire flips -------------------------------------------------------

    def test_retire_flips_reported_only_when_notifications_were_asked_for(self, seam):
        """A plain retire costs no extra query and reports nothing; one with
        a `Transition` reports the identity of every row it flipped, in the
        order its `key_fields` names. An executor that observes no flips
        may return `None` instead of a report — the orchestrator reads that as
        empty — so the report is only inspected when there is one.
        """
        seam.executor.apply([self._upsert(seam, "a"), self._upsert(seam, "b")])

        silent = seam.executor.apply(
            [
                Retire(
                    model_label=seam.model,
                    lookup={"stream_key": "s", "field_key": "b"},
                    patch={"resolved_at": "2024-01-01T00:00:00"},
                )
            ]
        )
        if silent is not None:
            assert silent.retire_flips == []

        report = seam.executor.apply(
            [
                Retire(
                    model_label=seam.model,
                    lookup={"stream_key": "s", "resolved_at__isnull": True},
                    patch={"resolved_at": "2024-01-02T00:00:00"},
                    transition=Transition(
                        kind="alert_resolved",
                        key_fields=("stream_key", "alert_type", "field_key"),
                    ),
                )
            ]
        )
        if report is not None:
            assert [
                eff.transition.kind
                for eff, _ in report.retire_flips
                if eff.transition is not None
            ] == ["alert_resolved"]
            # Only "a" was still open, so only "a" flipped.
            assert [rows for _, rows in report.retire_flips] == [
                [{"stream_key": "s", "alert_type": "machine", "field_key": "a"}]
            ]
