"""One call for "can the log rebuild this, and does it match?" (#184).

Answering that today means composing six interfaces and remembering a rule that
is written down nowhere: drain the log off the guarded database, arm the write
guard outside and the read guard inside, build a collector, replay, build a
snapshot reader, diff — and separately know that a pass means nothing unless the
guards were actually armed. ADR 0003 records the first consumer leaving the read
guard unwired for months, having recorded the drain as a blocker.

`rebuild_and_verify` is that composition, executed rather than described. The
tests here are mostly about the parts a caller gets wrong: whether the guards
were really on, whether anything was actually compared, and whether the log came
from somewhere the guard could be armed against.
"""

from __future__ import annotations

import pytest
from django.db import connection

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.rebuild import rebuild_and_verify
from django_rakaia.verification import RED, VACUOUS
from rakaia.effects import Upsert
from rakaia.registry import HandlerRegistry

from .models import FinanceLine

pytestmark = pytest.mark.django_db(databases=["default", "overlay"])

requires_concurrent_writes = pytest.mark.skipif(
    connection.vendor == "sqlite",
    reason=(
        "the test databases are in-memory SQLite, where a write from a second "
        "connection blocks on the test transaction ('database table is locked') "
        "rather than landing -- run with RAKAIA_TEST_DB=postgres"
    ),
)

PATH = "fin"


def _registry() -> HandlerRegistry:
    reg = HandlerRegistry()
    reg.register(
        name="fin",
        event_match=PATH,
        fn=lambda event: [
            Upsert(
                model_label="test_django_rakaia.FinanceLine",
                lookup={"submission_id": event["id"]},
                defaults={"suku": event["suku"], "delta": event["delta"]},
            )
        ],
        effective_from=0,
    )
    return reg


def _seed_log(*rows: dict) -> DjangoStreamStore:
    """Write the events to the live log, as production would have them."""
    import json

    store = DjangoStreamStore()
    store.create(PATH)
    for row in rows:
        store.append(PATH, json.dumps(row).encode())
    return store


def _live(*rows: dict) -> None:
    """The projection rows the rebuild is being checked against."""
    for row in rows:
        FinanceLine.objects.create(
            submission_id=row["id"], suku=row["suku"], delta=row["delta"]
        )


class TestTheVerdict:
    def test_a_matching_rebuild_certifies(self):
        rows = [
            {"id": "a", "suku": "s1", "delta": 1},
            {"id": "b", "suku": "s2", "delta": 2},
        ]
        _seed_log(*rows)
        _live(*rows)

        report = rebuild_and_verify(
            PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
        )

        assert report.certified
        assert report.compared == 2

    def test_a_drifted_row_is_reported_not_raised(self):
        _seed_log({"id": "a", "suku": "s1", "delta": 1})
        _live({"id": "a", "suku": "s1", "delta": 999})  # production disagrees

        report = rebuild_and_verify(
            PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
        )

        assert not report.certified
        assert report.verdict == RED

    def test_a_missing_live_row_is_a_difference(self):
        _seed_log({"id": "a", "suku": "s1", "delta": 1})
        # nothing in the projection at all

        report = rebuild_and_verify(
            PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
        )

        assert not report.certified

    def test_comparing_nothing_is_refused_rather_than_certified(self):
        # The vacuity trap: an empty stream, an `event_match` that stopped
        # matching, a registry that failed to autodiscover — all produce zero
        # effects, and "nothing disagreed" reads as a pass. `raise_if_diff`
        # already refuses this; the verdict has to carry it too. (A *renamed*
        # path is not in this class: the drain raises `StreamNotFound`, so it
        # fails loudly rather than vacuously.)
        DjangoStreamStore().create(PATH)  # exists, no events

        report = rebuild_and_verify(
            PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
        )

        assert report.verdict == VACUOUS
        assert not report.certified


class TestItLeavesProductionAlone:
    def test_the_rebuilt_rows_land_on_the_scratch_alias(self):
        rows = [{"id": "a", "suku": "s1", "delta": 1}]
        _seed_log(*rows)
        _live(*rows)

        rebuild_and_verify(
            PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
        )

        assert FinanceLine.objects.using("overlay").count() == 1
        assert FinanceLine.objects.using("default").count() == 1  # untouched

    # `transaction=True`, not the module's plain `django_db`: the leaking write
    # is on a second connection, so it commits for real rather than joining the
    # test's rolled-back transaction. Without this the row outlives the test and
    # every later count assertion in the suite sees it.
    @pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
    @requires_concurrent_writes
    def test_a_write_the_read_guard_cannot_see_is_still_caught(self):
        """The write guard earns its place on the leak the read guard misses.

        `deny_database_access` installs a per-connection wrapper, and Django keeps
        connections in a thread-local — so ORM work handed to another thread is
        invisible to it. That is not hypothetical: it is why
        `armed_deny_aliases` exists. Counting rows tolerates the hop.
        """
        from concurrent.futures import ThreadPoolExecutor

        from django_rakaia.hermeticity import LiveWriteLeaked

        _seed_log({"id": "a", "suku": "s1", "delta": 1})
        _live({"id": "a", "suku": "s1", "delta": 1})

        reg = HandlerRegistry()

        def leaky(event):  # noqa: ARG001
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(
                    FinanceLine.objects.create,
                    submission_id="leak",
                    suku="x",
                    delta=0,
                ).result()
            return []

        reg.register(name="leak", event_match=PATH, fn=leaky, effective_from=0)

        with pytest.raises(LiveWriteLeaked):
            rebuild_and_verify(
                PATH, into="overlay", live_models=[FinanceLine], registry=reg
            )

    def test_a_read_of_production_is_caught(self):
        from django_rakaia.hermeticity import AmbientDatabaseAccess

        _seed_log({"id": "a", "suku": "s1", "delta": 1})

        reg = HandlerRegistry()

        def peeking(event):  # noqa: ARG001
            # A handler consulting the live database instead of its reader — the
            # defect the read guard exists for, and invisible to the write guard.
            FinanceLine.objects.filter(submission_id="anything").exists()
            return []

        reg.register(name="peek", event_match=PATH, fn=peeking, effective_from=0)

        with pytest.raises(AmbientDatabaseAccess):
            rebuild_and_verify(
                PATH, into="overlay", live_models=[FinanceLine], registry=reg
            )


class TestTheGuardsAreProvablyArmed:
    """A pass means nothing unless the guards were on.

    `hermeticity.py` says this in prose and leaves it to the caller's
    discipline — "pair the run with a deliberate ambient read and check that it
    raises". Nobody does. Since the guards are armed inside this call, the check
    belongs inside it too.
    """

    def test_the_read_guard_is_armed_during_the_replay(self):
        seen: list[tuple[str, ...]] = []

        _seed_log({"id": "a", "suku": "s1", "delta": 1})
        _live({"id": "a", "suku": "s1", "delta": 1})

        reg = HandlerRegistry()

        def observe(event):  # noqa: ARG001
            from django_rakaia.hermeticity import armed_deny_aliases

            seen.append(armed_deny_aliases())
            return []

        reg.register(name="obs", event_match=PATH, fn=observe, effective_from=0)
        rebuild_and_verify(
            PATH, into="overlay", live_models=[FinanceLine], registry=reg
        )

        assert seen, "the handler never ran"
        assert "default" in seen[0], seen


class TestItRefusesAClaimItCannotHonour:
    def test_a_stale_scratch_alias_is_refused_rather_than_rebuilt_onto(self):
        # A leftover row on `into` is read by stage > 0 handlers, so the effects
        # are no longer derived from the log alone — but the diff is against
        # *live*, so nothing downstream would notice.
        from django_rakaia.rebuild import ScratchAliasNotEmpty

        _seed_log({"id": "a", "suku": "s1", "delta": 1})
        FinanceLine.objects.using("overlay").create(
            submission_id="stale", suku="old", delta=7
        )

        with pytest.raises(ScratchAliasNotEmpty):
            rebuild_and_verify(
                PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
            )

    def test_the_stale_check_only_looks_at_the_models_being_rebuilt(self):
        # Opting out with `()` is explicit, and an unrelated model's rows on the
        # scratch alias are not this call's business.
        rows = [{"id": "a", "suku": "s1", "delta": 1}]
        _seed_log(*rows)
        _live(*rows)

        report = rebuild_and_verify(
            PATH, into="overlay", live_models=(), registry=_registry()
        )

        assert report.certified


class TestTheLogComesFromOffTheGuardedAlias:
    def test_a_caller_supplied_store_is_used_instead_of_the_live_log(self):
        import json

        from rakaia.store import StreamStore

        _live({"id": "a", "suku": "s1", "delta": 1})
        # Nothing in the durable log at all: if the drain ignored `source` the
        # replay would compare nothing and the verdict would be VACUOUS.
        elsewhere = StreamStore()
        elsewhere.create(PATH)
        elsewhere.append(
            PATH, json.dumps({"id": "a", "suku": "s1", "delta": 1}).encode()
        )

        report = rebuild_and_verify(
            PATH,
            into="overlay",
            live_models=[FinanceLine],
            source=elsewhere,
            registry=_registry(),
        )

        assert report.certified


class TestItChecksThatItChecked:
    """The self-check exists because a green verdict obtained with the guard
    disarmed is indistinguishable, to the caller, from a real one — and the
    guard's own docstring hands that check to the caller's discipline."""

    def test_a_disarmed_read_guard_is_refused_not_certified(self, monkeypatch):
        from contextlib import contextmanager

        from django_rakaia import rebuild as rebuild_mod
        from django_rakaia.rebuild import GuardNotArmed

        rows = [{"id": "a", "suku": "s1", "delta": 1}]
        _seed_log(*rows)
        _live(*rows)

        @contextmanager
        def _disarmed(*_aliases):
            yield  # a guard that guards nothing — the state ADR 0003 describes

        monkeypatch.setattr(rebuild_mod, "deny_database_access", _disarmed)

        with pytest.raises(GuardNotArmed):
            rebuild_and_verify(
                PATH, into="overlay", live_models=[FinanceLine], registry=_registry()
            )

    def test_the_write_guard_covers_more_than_the_replay(self):
        # The read guard can only cover the replay -- the diff's whole job is to
        # read live rows. So the write guard has to wrap the outside, and this
        # pins that it does: a leak from the diff phase is still caught.
        from django_rakaia.hermeticity import LiveWriteLeaked

        rows = [{"id": "a", "suku": "s1", "delta": 1}]
        _seed_log(*rows)
        _live(*rows)

        leaked = []

        def leaky_normalizer(_model, _field, value):
            # Once: the normalizer is called per field, and a second insert on a
            # unique column would fail on its own terms rather than as a leak.
            if not leaked:
                leaked.append(
                    FinanceLine.objects.create(submission_id="leak", suku="x", delta=0)
                )
            return value

        with pytest.raises(LiveWriteLeaked):
            rebuild_and_verify(
                PATH,
                into="overlay",
                live_models=[FinanceLine],
                registry=_registry(),
                normalizers=[leaky_normalizer],
            )


class TestStagedHandlersReadTheScratchAlias:
    """Why this applies effects rather than collecting them.

    A stage > 0 handler resolves facts an earlier stage materialised. A
    `CollectingExecutor` never writes, so it cannot answer that question -- but a
    reader pointed at the live database answers it from production, which is the
    determinism leak the whole gate exists to catch. The reader must see the
    scratch alias, and only the scratch alias.
    """

    def test_a_later_stage_sees_what_the_earlier_stage_wrote(self):
        from rakaia.effects import Upsert

        rows = [{"id": "a", "suku": "s1", "delta": 5}]
        _seed_log(*rows)
        _live(*rows)

        reg = HandlerRegistry()
        reg.register(
            name="fin",
            event_match=PATH,
            fn=lambda event: [
                Upsert(
                    model_label="test_django_rakaia.FinanceLine",
                    lookup={"submission_id": event["id"]},
                    defaults={"suku": event["suku"], "delta": event["delta"]},
                )
            ],
            effective_from=0,
        )

        def stage_one(event, reader):
            # Reads the row stage 0 wrote. On the live alias this row does not
            # exist (nothing seeded it), so a reader on `default` would either
            # trip the guard or resolve to None.
            row = reader.get(
                "test_django_rakaia.FinanceLine", submission_id=event["id"]
            )
            return [
                Upsert(
                    model_label="test_django_rakaia.Balance",
                    lookup={"suku": row.suku},
                    defaults={"total": row.delta},
                )
            ]

        reg.register(
            name="bal", event_match=PATH, fn=stage_one, effective_from=0, stage=1
        )

        from .models import Balance

        Balance.objects.create(suku="s1", total=5)  # production's version

        report = rebuild_and_verify(
            PATH, into="overlay", live_models=[FinanceLine, Balance], registry=reg
        )

        assert report.certified
        assert Balance.objects.using("overlay").get(suku="s1").total == 5


class TestTheRecordingTee:
    def test_it_applies_a_generator_it_has_also_recorded(self):
        # The tee walks the batch before handing it on, so a one-shot iterable
        # would otherwise reach the inner executor already exhausted -- recorded
        # as verified, applied nowhere.
        from django_rakaia.rebuild import _RecordingExecutor
        from rakaia.effects import Upsert
        from rakaia.executors import CollectingExecutor

        inner = CollectingExecutor()
        tee = _RecordingExecutor(inner)
        effect = Upsert(
            model_label="test_django_rakaia.FinanceLine",
            lookup={"submission_id": "a"},
            defaults={"delta": 1},
        )

        tee.apply(e for e in [effect])

        assert tee.effects == [effect]
        assert inner.effects == [effect]


class TestWhatItForwardsToReplay:
    """A forwarded argument that quietly stops being forwarded is the worst kind
    of defect in a trust gate: the call still returns a verdict, and the verdict
    still looks like evidence."""

    def test_a_strict_drift_policy_is_honoured(self):
        # `on_drift="raise"` degrading to "warn" is silent -- the run completes
        # and certifies, having replayed handlers whose code no longer matches
        # what produced the recorded rows.
        from rakaia.registry import HandlerDriftError

        rows = [{"id": "a", "suku": "s1", "delta": 1}]
        _seed_log(*rows)
        _live(*rows)

        reg = HandlerRegistry()
        version = reg.register(
            name="fin",
            event_match=PATH,
            fn=lambda event: [],  # noqa: ARG005
            effective_from=0,
        )
        object.__setattr__(version, "source_hash", "deadbeef" * 8)

        with pytest.raises(HandlerDriftError):
            rebuild_and_verify(
                PATH,
                into="overlay",
                live_models=[FinanceLine],
                registry=reg,
                on_drift="raise",
            )

    def test_the_match_string_can_differ_from_the_stream_path(self):
        # Content-routed registries match on something other than the path, so a
        # dropped `event_match` matches nothing and the run reports VACUOUS.
        rows = [{"id": "a", "suku": "s1", "delta": 1}]
        _seed_log(*rows)
        _live(*rows)

        reg = HandlerRegistry()
        reg.register(
            name="fin",
            event_match="routed-elsewhere",
            fn=lambda event: [
                Upsert(
                    model_label="test_django_rakaia.FinanceLine",
                    lookup={"submission_id": event["id"]},
                    defaults={"suku": event["suku"], "delta": event["delta"]},
                )
            ],
            effective_from=0,
        )

        report = rebuild_and_verify(
            PATH,
            into="overlay",
            live_models=[FinanceLine],
            registry=reg,
            event_match="routed-elsewhere",
        )

        assert report.certified

    def test_upcasters_run_before_the_handlers(self):
        # The log holds the old shape; only the upcaster knows the new one. A
        # dropped registry means the handler reads a field that is not there.
        from rakaia.registry import UpcasterRegistry

        _seed_log({"id": "a", "suku": "s1", "old_delta": 3})
        _live({"id": "a", "suku": "s1", "delta": 3})

        ups = UpcasterRegistry()
        ups.register(PATH, 1, lambda event: {**event, "delta": event.pop("old_delta")})

        report = rebuild_and_verify(
            PATH,
            into="overlay",
            live_models=[FinanceLine],
            registry=_registry(),
            upcaster_registry=ups,
        )

        assert report.certified


class TestProductionNeedNotBeTheDefaultAlias:
    """`live_using` has four use sites -- the drain, the write guard, the read
    guard, and the diff reader -- and every one of them is a place a hardcoded
    'default' would look right. Nothing exercises them while every test leaves
    the parameter alone, so this run swaps the two aliases over.
    """

    def test_the_whole_gate_runs_with_the_aliases_swapped(self):
        import json

        rows = [{"id": "a", "suku": "s1", "delta": 1}]

        store = DjangoStreamStore(using="overlay")
        store.create(PATH)
        for row in rows:
            store.append(PATH, json.dumps(row).encode())
        for row in rows:
            FinanceLine.objects.using("overlay").create(
                submission_id=row["id"], suku=row["suku"], delta=row["delta"]
            )

        report = rebuild_and_verify(
            PATH,
            into="default",
            live_using="overlay",
            live_models=[FinanceLine],
            registry=_registry(),
        )

        assert report.certified
        assert FinanceLine.objects.using("default").count() == 1

    def test_drift_against_the_named_live_alias_is_what_is_reported(self):
        # Pins that the *diff* follows `live_using` too, not just the replay:
        # `default` holds a matching row and `overlay` a wrong one, so a reader
        # left on 'default' would certify.
        import json

        store = DjangoStreamStore(using="overlay")
        store.create(PATH)
        store.append(PATH, json.dumps({"id": "a", "suku": "s1", "delta": 1}).encode())
        FinanceLine.objects.using("overlay").create(
            submission_id="a", suku="s1", delta=999
        )

        report = rebuild_and_verify(
            PATH,
            into="default",
            live_using="overlay",
            live_models=[FinanceLine],
            registry=_registry(),
        )

        assert report.verdict == RED
