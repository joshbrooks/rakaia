#!/usr/bin/env python3
"""`multi_owner` — one row, many writers: Refs, shared aggregates, natural keys.

The projection helpers most examples use (`reconcile_children`, `project_latest`)
own a *whole* row. The primitives here are for the harder case where a single
projection row is composed by **several independent owners**, or where a child
FK must point at a sibling row whose primary key doesn't exist until apply time.
None of the other examples exercise them, so this one does — end to end, through
rakaia's own in-memory `InMemoryProjections`, which the shared executor contract
holds to the same behaviour as `DjangoExecutor`:

  * `Ref` / `RefResolver`     — an effect binds an FK to a *sibling* effect's
                                generated primary key, no staging split.
  * `reconcile_aggregate(owns=)` — two reducers share one row; each owns disjoint
                                columns; a vanished group clears only its own.
  * `reconcile_by_key(retire=)`  — reconcile rows on a composite natural key,
                                soft-deleting (not hard-deleting) stale rows.
  * `check_disjoint_defaults`    — the guard that makes multi-owner rows safe.
  * `ExternalEffect`             — not an Effect at all: no executor applies it,
                                replay hands them back, the app routes them.

Runs as a plain script (no Django):

    just multi-owner-demo
    # or: uv run python examples/multi_owner/demo.py
"""

from __future__ import annotations

from rakaia import (
    Effect,
    EffectCollisionError,
    ExternalEffect,
    InMemoryProjections,
    Ref,
    Update,
    Upsert,
    check_disjoint_defaults,
    reconcile_aggregate,
    reconcile_by_key,
)

AREA = "geo.Area"
PROJECT = "geo.Project"
BALANCE = "fin.Balance"
ALERT = "qa.Alert"


def _hdr(title: str) -> None:
    print(f"\n{title}")
    print("-" * 62)


def section_refs() -> None:
    _hdr("[1] Ref / RefResolver: bind an FK to a sibling's generated pk")
    ex = InMemoryProjections()
    # One batch: create an Area, then a Project whose area_id must point at the
    # Area's primary key — which does not exist until the first effect applies.
    # `produces=`/`Ref` wires them without a natural-key reader lookup.
    ex.apply(
        [
            Upsert(
                model_label=AREA,
                lookup={"name": "Zone-North"},
                defaults={},
                produces="north",
            ),
            Upsert(
                model_label=PROJECT,
                lookup={"name": "Irrigation-7"},
                defaults={"area_id": Ref("north")},
            ),
        ]
    )
    area = ex.rows(AREA)[0]
    project = ex.rows(PROJECT)[0]
    assert project["area_id"] == area["pk"]
    print(
        f"    Area pk={area['pk']}; Project.area_id={project['area_id']} — FK wired ✓"
    )

    # A dry run (CollectingExecutor) keeps the literal Ref; only an *applying*
    # executor resolves it. So a forward/typo Ref is caught, not silently NULL.
    from rakaia import RefResolver, UnresolvedRefError

    try:
        RefResolver().resolve_value(Ref("does-not-exist"))
    except UnresolvedRefError:
        print(
            "    a Ref to an unproduced id raises UnresolvedRefError (never silent NULL) ✓"
        )
    else:  # pragma: no cover
        raise AssertionError("expected UnresolvedRefError")


def section_multi_owner_aggregate() -> None:
    _hdr("[2] reconcile_aggregate(owns=): two reducers, one shared row")
    ex = InMemoryProjections()

    # A per-suku Balance row is co-owned. Because owns= per-group effects are
    # update-if-exists (they never mint a row), one owner owns the row's
    # *existence*: a ROSTER that upserts a row per active suku. Two more owners
    # each recompute *their own columns only* — a STATUS reducer owns `status`,
    # a FINANCE reducer owns `ksp_total`. Single-owner mode would DELETE the
    # whole row when one reducer's group vanished, clobbering the others; owns=
    # null-clears only its own columns instead.
    def roster_pass(sukus: list[str]) -> list[Effect]:
        return [
            Upsert(model_label=BALANCE, lookup={"report_id": 1, "suku": s}, defaults={})
            for s in sukus
        ]

    def status_pass(groups: dict[str, dict]) -> list[Effect]:
        return reconcile_aggregate(
            BALANCE, {"report_id": 1}, "suku", groups, owns=("status",)
        )

    def finance_pass(groups: dict[str, dict]) -> list[Effect]:
        return reconcile_aggregate(
            BALANCE, {"report_id": 1}, "suku", groups, owns=("ksp_total",)
        )

    # First replay: roster mints both rows; both owners fill their column.
    ex.apply(roster_pass(["north", "south"]))
    ex.apply(status_pass({"north": {"status": "open"}, "south": {"status": "open"}}))
    ex.apply(finance_pass({"north": {"ksp_total": 500}, "south": {"ksp_total": 300}}))
    north = next(r for r in ex.rows(BALANCE) if r["suku"] == "north")
    assert north["status"] == "open" and north["ksp_total"] == 500
    print("    north: status=open ksp_total=500  (written by two owners) ✓")

    # Second replay: finance no longer has a 'north' contributor; roster and
    # status still do. owns= null-clears ONLY finance's column on north; the
    # row and the status owner's column both survive.
    ex.apply(roster_pass(["north", "south"]))
    ex.apply(status_pass({"north": {"status": "closed"}, "south": {"status": "open"}}))
    ex.apply(finance_pass({"south": {"ksp_total": 300}}))  # north vanished here
    north = next(r for r in ex.rows(BALANCE) if r["suku"] == "north")
    assert north["ksp_total"] is None  # finance's column cleared
    assert north["status"] == "closed"  # status owner untouched
    print("    north: ksp_total->None (finance's col) but status='closed' kept ✓")
    print("    the shared row survived a vanished group in one owner ✓")


def section_reconcile_by_key() -> None:
    _hdr("[3] reconcile_by_key(retire=patch): soft-delete on a natural key")
    ex = InMemoryProjections()

    # QA alerts keyed by (alert_type, field_key). A re-derivation should stamp
    # `resolved_at` on alerts that no longer fire — NOT hard-delete them, so the
    # audit trail survives — and leave authored rows alone via retire_filter.
    def alerts(items: list[dict], event_ts: str) -> list[Effect]:
        return reconcile_by_key(
            ALERT,
            scope={"submission_id": "sub-1"},
            key_fields=("alert_type", "field_key"),
            items=items,
            key_fn=lambda v: {
                "alert_type": v["alert_type"],
                "field_key": v["field_key"],
            },
            defaults_fn=lambda v: {"severity": v["severity"], "resolved_at": None},
            retire_filter={"alert_type__in": ["ff4", "sf11"]},  # machine types only
            retire={"resolved_at": event_ts},  # soft-delete: stamp, don't drop
        )

    ex.apply(
        alerts(
            [
                {"alert_type": "ff4", "field_key": "budget", "severity": "high"},
                {"alert_type": "sf11", "field_key": "date", "severity": "low"},
            ],
            event_ts="t1",
        )
    )
    assert len({r["pk"] for r in ex.rows(ALERT)}) == 2
    print("    2 alerts raised (ff4/budget, sf11/date), both live ✓")

    # Re-derive: the ff4 violation is fixed and no longer present.
    ex.apply(
        alerts(
            [
                {"alert_type": "sf11", "field_key": "date", "severity": "low"},
            ],
            event_ts="t2",
        )
    )
    ff4 = next(r for r in ex.rows(ALERT) if r["alert_type"] == "ff4")
    sf11 = next(r for r in ex.rows(ALERT) if r["alert_type"] == "sf11")
    assert ff4["resolved_at"] == "t2"  # soft-deleted, row retained
    assert sf11["resolved_at"] is None  # still firing, untouched
    print("    ff4 stamped resolved_at=t2 (row kept); sf11 still open ✓")


def section_disjoint_guard() -> None:
    _hdr("[4] check_disjoint_defaults: the invariant that keeps owners honest")
    # Two owners writing DISJOINT columns of the same row is fine...
    ok = [
        Update(
            model_label=BALANCE, lookup={"suku": "north"}, defaults={"status": "open"}
        ),
        Update(
            model_label=BALANCE, lookup={"suku": "north"}, defaults={"ksp_total": 5}
        ),
    ]
    check_disjoint_defaults(ok)  # no raise
    print("    disjoint columns on a shared row: allowed ✓")

    # ...but two owners writing the SAME column is a collision — caught early.
    clash = [
        Update(
            model_label=BALANCE, lookup={"suku": "north"}, defaults={"status": "open"}
        ),
        Update(
            model_label=BALANCE, lookup={"suku": "north"}, defaults={"status": "closed"}
        ),
    ]
    try:
        check_disjoint_defaults(clash)
    except EffectCollisionError as exc:
        print(f"    same column from two owners: EffectCollisionError ✓\n      ({exc})")
    else:  # pragma: no cover
        raise AssertionError("expected EffectCollisionError")


def section_external_effects() -> None:
    _hdr("[5] external effects: route the ones no executor will apply")
    sent: list[str] = []
    effects = [
        Upsert(
            model_label=BALANCE, lookup={"suku": "north"}, defaults={"status": "closed"}
        ),
        ExternalEffect(
            kind="email", payload={"to": "pm@example.org", "re": "north closed"}
        ),
        ExternalEffect(kind="webhook", payload={"url": "/hooks/closed"}),
    ]
    handlers = {
        "email": lambda e: sent.append(f"email->{e.payload['to']}"),
        "webhook": lambda e: sent.append(f"webhook->{e.payload['url']}"),
    }
    count = 0
    for eff in effects:  # the write effect is ignored — it is not external
        if isinstance(eff, ExternalEffect) and eff.kind in handlers:
            handlers[eff.kind](eff)
            count += 1
    assert count == 2 and len(sent) == 2
    print(f"    dispatched {count} external effects: {sent} ✓")


def main() -> None:
    print("=" * 62)
    print("  Rakaia effect/projection primitives — one row, many writers")
    print("=" * 62)
    section_refs()
    section_multi_owner_aggregate()
    section_reconcile_by_key()
    section_disjoint_guard()
    section_external_effects()
    print("\nAll multi-owner effect checks passed ✓")


if __name__ == "__main__":
    main()
