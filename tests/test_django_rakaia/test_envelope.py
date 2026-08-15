"""One append envelope, one fold ritual — instead of both re-typed per call site.

Two shapes are written by hand at nearly every durable call site in a real
consumer: appending an enveloped event (JSON-encode with Django's encoder,
create-the-stream-if-missing, wrap the label/actor/timestamp in `AppendOptions`),
and folding a batch of events live (seed a scratch in-memory `StreamStore`, then
`replay()` it through a registry with a reader bound). The first production
consumer counted ~37 copies of the append across 18 files and 11 copies of the
fold, and its own module carried the warning that motivates this: *"a second
write path which re-implements the envelope is a path no gate covers."*

The rule these tests exist to enforce is **byte-identical to the hand-rolled
ritual**. The helpers are only worth having if adopting them is a pure deletion,
so every test here pins the helper's output against the longhand it replaces
rather than against a hand-written expectation.
"""

from __future__ import annotations

import json

import pytest
from django.core.serializers.json import DjangoJSONEncoder

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.envelope import SCRATCH_PATH, append_event, fold_events
from django_rakaia.projection_reader import DjangoProjectionReader
from rakaia import AppendOptions, StreamStore, Upsert, provenance
from rakaia.registry import HandlerRegistry

from .models import FinanceLine

pytestmark = pytest.mark.django_db


def _hand_rolled(store, path, payload, *, label, actor=None, event_ts=None):
    """The longhand `append_event` replaces, kept here as the oracle.

    Faithful to the copies in the wild, including the one bug they share: with
    no actor this writes `{"user": None}`, which out-ranks an ambient
    `provenance(user=...)`. The helper omits the key instead — the single
    deliberate divergence, pinned by
    `test_no_actor_does_not_clobber_the_ambient_one`.
    """
    if not store.has(path):
        store.create(path)
    store.append(
        path,
        json.dumps(payload, cls=DjangoJSONEncoder).encode(),
        AppendOptions(label=label, metadata={"user": actor}, event_ts=event_ts),
    )


class TestAppendEventMatchesTheLonghand:
    def test_the_message_is_byte_identical(self):
        payload = {"submission": "s1", "amount": 10}

        longhand = StreamStore()
        _hand_rolled(longhand, "p", payload, label="create", actor=7)
        helper = StreamStore()
        append_event(helper, "p", payload, label="create", actor=7)

        (a,), _ = longhand.read("p")
        (b,), _ = helper.read("p")
        assert a.data == b.data
        assert a.label == b.label
        assert a.metadata == b.metadata

    def test_it_creates_the_stream_when_missing(self):
        store = StreamStore()
        append_event(store, "brand-new", {"n": 1}, label="create")
        assert store.has("brand-new")

    def test_it_appends_to_an_existing_stream_without_resetting_it(self):
        store = StreamStore()
        append_event(store, "p", {"n": 1}, label="create")
        append_event(store, "p", {"n": 2}, label="update")
        messages, _ = store.read("p")
        assert [json.loads(m.data)["n"] for m in messages] == [1, 2]

    def test_the_actor_lands_where_envelope_actor_reads_it(self):
        """`history.envelope_actor` reads `metadata['user']` — the helper must
        write the key the rest of rakaia already agrees on."""
        from rakaia.history import envelope_actor

        store = StreamStore()
        append_event(store, "p", {"user_id": 1}, label="update", actor=42)
        (msg,), _ = store.read("p")
        assert envelope_actor(msg, json.loads(msg.data)) == 42

    def test_django_types_are_encoded(self):
        """A UUID/Decimal/datetime payload is why the encoder is part of the
        ritual rather than a plain `json.dumps`."""
        import uuid
        from decimal import Decimal

        store = StreamStore()
        ref = uuid.uuid4()
        append_event(
            store, "p", {"ref": ref, "amount": Decimal("1.50")}, label="create"
        )
        (msg,), _ = store.read("p")
        assert json.loads(msg.data)["ref"] == str(ref)

    def test_event_ts_is_carried_when_given(self):
        store = StreamStore()
        append_event(store, "p", {"n": 1}, label="create", event_ts=1234.5)
        (msg,), _ = store.read("p")
        assert msg.event_ts == 1234.5

    def test_ambient_provenance_still_merges(self):
        """The helper sets `metadata['user']` explicitly, but must not shut the
        ambient block out of the other fields."""
        store = StreamStore()
        with provenance(url="/imports/"):
            append_event(store, "p", {"n": 1}, label="create", actor=5)
        (msg,), _ = store.read("p")
        assert msg.metadata["user"] == 5
        assert msg.metadata["url"] == "/imports/"

    def test_no_actor_does_not_clobber_the_ambient_one(self):
        """The one place the helper deliberately departs from the longhand.

        `merge_provenance` layers ambient *under* explicit, so writing
        `{"user": None}` for a caller who simply didn't pass an actor would beat
        the user `ProvenanceMiddleware` already stamped on the request — the
        caller's silence would become a positive assertion that nobody did this,
        and `envelope_actor` would fall back to the payload's owner FK. Omit the
        key instead, and the ambient actor survives.
        """
        store = StreamStore()
        with provenance(user=7, url="/imports/"):
            append_event(store, "p", {"n": 1}, label="create")
        (msg,), _ = store.read("p")
        assert msg.metadata["user"] == 7
        assert msg.metadata["url"] == "/imports/"

    def test_no_actor_and_no_ambient_block_keeps_the_bare_default(self):
        """With nothing to record, the message keeps rakaia's no-envelope
        default rather than carrying a `user: None` that means nothing."""
        store = StreamStore()
        append_event(store, "p", {"n": 1}, label="create")
        (msg,), _ = store.read("p")
        assert msg.metadata is None


def _handler(event):
    return Upsert(
        model_label="test_django_rakaia.FinanceLine",
        lookup={"submission_id": event["id"]},
        defaults={"suku": event["suku"]},
    )


def _registry() -> HandlerRegistry:
    registry = HandlerRegistry()
    registry.register(
        name="finance",
        event_match=SCRATCH_PATH,
        fn=_handler,
        effective_from=0,
    )
    return registry


class TestFoldEvents:
    def test_it_projects_a_batch_through_the_registry(self):
        fold_events(
            [{"id": "a", "suku": "one"}, {"id": "b", "suku": "two"}],
            _registry(),
        )
        assert FinanceLine.objects.count() == 2
        assert FinanceLine.objects.get(submission_id="a").suku == "one"

    def test_list_order_is_the_replay_order(self):
        """The scratch stream is seeded in list order, so a later event wins."""
        fold_events(
            [{"id": "a", "suku": "first"}, {"id": "a", "suku": "second"}],
            _registry(),
        )
        assert FinanceLine.objects.get(submission_id="a").suku == "second"

    def test_it_leaves_no_durable_trace(self):
        """The scratch store is in-memory — a live fold must not append to the
        durable log."""
        from django_rakaia.models import StreamEvent

        before = StreamEvent.objects.count()
        fold_events([{"id": "a", "suku": "one"}], _registry())
        assert StreamEvent.objects.count() == before

    def test_an_explicit_executor_is_used(self):
        from rakaia.executors import CollectingExecutor

        executor = CollectingExecutor()
        fold_events([{"id": "a", "suku": "one"}], _registry(), executor=executor)

        assert len(executor.effects) == 1
        assert FinanceLine.objects.count() == 0  # dry run wrote nothing

    def test_an_empty_batch_is_a_no_op(self):
        fold_events([], _registry())
        assert FinanceLine.objects.count() == 0

    def test_successive_folds_do_not_leak_into_each_other(self):
        """Each call gets a fresh scratch store, so event 1 is not replayed
        again by the second call."""
        registry = _registry()
        fold_events([{"id": "a", "suku": "one"}], registry)
        fold_events([{"id": "b", "suku": "two"}], registry)
        assert FinanceLine.objects.count() == 2

    def test_a_reader_is_forwarded_for_staged_folds(self):
        fold_events(
            [{"id": "a", "suku": "one"}],
            _registry(),
            reader=DjangoProjectionReader(),
            executor=DjangoExecutor(),
        )
        assert FinanceLine.objects.count() == 1


class TestTheScratchPathIsRakaiasOwnNamespace:
    """#100 — the default was `produce/submission`, domain language from the
    first consumer sitting in the generic integration. It reads like a
    convention; it is not one. But it *is* load-bearing, because a registry's
    `event_match` has to name it, so every other consumer was registering
    handlers against another project's vocabulary."""

    def test_it_is_namespaced_and_not_consumer_domain_language(self):
        assert SCRATCH_PATH.startswith("_")
        assert "submission" not in SCRATCH_PATH

    def test_an_explicit_scratch_path_overrides_it(self):
        """A consumer whose handlers match their own vocabulary can say so."""
        registry = HandlerRegistry()
        registry.register(
            name="finance", event_match="my/own/path", fn=_handler, effective_from=0
        )
        fold_events([{"id": "a", "suku": "one"}], registry, scratch_path="my/own/path")
        assert FinanceLine.objects.count() == 1

    def test_a_registry_matching_a_different_path_projects_nothing(self):
        """The coupling made explicit: mismatch is silent, which is why the
        constant exists for both sides to name."""
        registry = HandlerRegistry()
        registry.register(
            name="finance",
            event_match="somewhere/else",
            fn=_handler,
            effective_from=0,
        )
        fold_events([{"id": "a", "suku": "one"}], registry)
        assert FinanceLine.objects.count() == 0
