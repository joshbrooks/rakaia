"""Shared conformance contract for the protocol-server store surface.

`tests/store_contract.py` covers `WritableStore` — what `replay()` and
projections need. This covers `StreamServerStore`: the protocol lifecycle a
Durable Streams server needs from whatever is under it — producer fencing,
close, TTL, long-poll, response formatting.

Both the in-memory `StreamStore` and the durable `DjangoStreamStore` must pass
it. That is the point: one protocol server implementation runs on either, so
the Django integration does not need a protocol implementation of its own.

Not named `test_*`, so pytest collects it only via the backend subclasses.

**This file supersedes the "permanent architectural divergence" note that used
to open `store_contract.py`.** Close, TTL, Stream-Seq and producer fencing were
described there as concerns the durable store would never model. They are
modelled now, and asserted here for both. What remains genuinely
backend-specific is only the offset *format* (compound `{seq}_{byte}` vs
zero-padded int) — the protocol mandates opacity, not one format (§6).

Nothing in this file is permitted a per-backend answer. It briefly was: whether
`append_many` flattens a top-level JSON array was recorded here as an open
divergence, keyed on a flag each backend set for itself, so that neither could
drift while #214 was undecided. #214 decided it — both flatten, matching
`append` — so the flag is gone and the question is one assertion again.
"""

from __future__ import annotations

import inspect

import pytest

from rakaia.protocols import StreamServerStore
from rakaia.types import (
    AppendOptions,
    ContentTypeMismatch,
    EmptyJsonArray,
    InvalidJson,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerInvalidEpochSeq,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    ProducerStreamClosed,
    SequenceConflict,
    StreamConfigConflict,
    StreamNotFound,
)
from rakaia.types import Stream as ProtocolStream


def _protocol_methods() -> list[str]:
    """Every method the protocol declares, inherited ones included."""
    return sorted(
        name
        for name in dir(StreamServerStore)
        if not name.startswith("_")
        and inspect.isfunction(getattr(StreamServerStore, name))
    )


def _calls_the_declaration_permits(sig):
    """The extreme calls a declared signature allows: required-only and all-in.

    Dummy values stand in for real arguments — `bind` checks shape, not types.
    If both extremes bind on an implementation, everything between them does
    too (parameters are independent once positional order is fixed).
    """
    minimal_args, minimal_kwargs = [], {}
    maximal_args, maximal_kwargs = [], {}
    for p in sig.parameters.values():
        if p.name == "self":
            continue
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD):
            maximal_args.append(p.name)
            if p.default is p.empty:
                minimal_args.append(p.name)
        elif p.kind is p.KEYWORD_ONLY:
            maximal_kwargs[p.name] = p.name
            if p.default is p.empty:
                minimal_kwargs[p.name] = p.name
    return [(minimal_args, minimal_kwargs), (maximal_args, maximal_kwargs)]


class ServerStoreContract:
    """Contract every protocol-server store must uphold.

    Subclasses provide::

        @pytest.fixture
        def store(self):
            return MyStore()
    """

    # =========================================================================
    # Shape
    # =========================================================================

    def test_satisfies_the_server_store_protocol(self, store):
        from rakaia import StreamServerStore

        assert isinstance(store, StreamServerStore)

    @pytest.mark.parametrize("method", _protocol_methods())
    def test_takes_the_arguments_the_protocol_declares(self, store, method):
        """`isinstance` above only checks that the *names* exist.

        A `runtime_checkable` Protocol does not compare signatures, so a store
        can satisfy it while taking arguments the server does not pass — which
        is how `close_stream_with_producer` came to be declared one way and
        implemented another in both stores at once.

        Checked with `Signature.bind` rather than name-list equality: every
        call the declared signature permits must bind on the implementation.
        That also covers the inherited methods (a name list skipped them), and
        it allows a store to accept a *superset* — extra defaulted parameters
        of its own — which is a valid way to satisfy a Protocol.
        """
        impl = inspect.signature(getattr(store, method))
        declared = inspect.signature(getattr(StreamServerStore, method))
        for args, kwargs in _calls_the_declaration_permits(declared):
            try:
                impl.bind(*args, **kwargs)
            except TypeError as e:
                pytest.fail(
                    f"StreamServerStore.{method} permits a call "
                    f"{type(store).__name__} cannot take: "
                    f"{method}(*{args}, **{kwargs}) — {e}"
                )

    def test_get_exposes_what_a_server_reads(self, store):
        """A server reads exactly these six off a stream."""
        store.create("s", content_type="text/plain", ttl_seconds=60)
        stream = store.get("s")
        for attr in (
            "current_offset",
            "closed",
            "content_type",
            "ttl_seconds",
            "expires_at",
            "last_seq",
        ):
            assert hasattr(stream, attr), f"stream is missing {attr}"

    def test_get_is_none_for_a_missing_stream(self, store):
        assert store.get("nope") is None

    def test_get_returns_the_shared_stream_type(self, store):
        """`get()` returns `rakaia.types.Stream` — the same type from every store.

        `test_get_exposes_what_a_server_reads` only checks `hasattr`, which a
        backend's own row object satisfies too: the durable store used to return
        its ORM `Stream` model, which carries all six of those attributes. So the
        contract passed while the two stores returned structurally similar but
        entirely different types, and `StreamServerStore.get` was declared
        `-> Any`, so the type checker had nothing to say either.

        That is not hypothetical — it is exactly what happened, and it broke the
        first downstream consumer, which was doing
        `StreamEntry.objects.filter(stream=store.get(path))` and relying on
        getting an ORM row back. Returning a snapshot is the *right* call (a
        protocol server is async, and an ORM row is lazy — reading
        `stream.current_offset` off it would issue a query at attribute access,
        which Django refuses from an async context). The mistake was that
        nothing stated it, so the change was invisible until it reached a
        consumer.
        """
        store.create("s", content_type="text/plain", ttl_seconds=60)
        assert isinstance(store.get("s"), ProtocolStream)

    def test_get_does_not_leak_a_backend_row(self, store):
        """The returned metadata is inert: reading it issues no further query.

        The reason the durable store hands back a snapshot rather than its ORM
        row. A server resolves everything inside the store's `run_sync` bridge
        and reads it afterwards, outside — so anything lazy would blow up there
        rather than here.
        """
        store.create("s", ttl_seconds=60)
        stream = store.get("s")
        # Consuming every field a server touches must not need a live connection.
        snapshot = (
            stream.path,
            stream.current_offset,
            stream.content_type,
            stream.ttl_seconds,
            stream.expires_at,
            stream.last_seq,
            stream.closed,
        )
        assert snapshot[0] == "s"

    # =========================================================================
    # Create
    # =========================================================================

    def test_create_is_idempotent_for_matching_config(self, store):
        store.create("s", content_type="application/json")
        store.create("s", content_type="application/json")
        assert store.has("s")

    def test_create_with_different_config_conflicts(self, store):
        store.create("s", content_type="text/plain")
        with pytest.raises(StreamConfigConflict):
            store.create("s", content_type="application/json")

    def test_create_with_a_body_seeds_the_stream(self, store):
        """`PUT` with a body creates and appends in one step.

        This is also the durable store's only write path that ever ran outside
        a transaction — invisible on SQLite (no row locks), a guaranteed
        `TransactionManagementError` 500 on Postgres. The case exists so a
        Postgres CI leg fails if the `transaction.atomic()` around create is
        ever removed.
        """
        import json

        store.create("s", content_type="application/json", initial_data=b'{"id": 1}')
        messages, _ = store.read("s")
        assert json.loads(store.format_response("s", messages)) == [{"id": 1}]

    # =========================================================================
    # Append
    # =========================================================================

    def test_append_returns_the_message(self, store):
        store.create("s")
        result = store.append("s", b'{"id": 1}')
        assert result.message is not None
        assert result.message.offset

    def test_append_to_a_missing_stream_raises(self, store):
        with pytest.raises(StreamNotFound):
            store.append("nope", b'{"id": 1}')

    def test_append_to_a_closed_stream_reports_closed(self, store):
        store.create("s")
        store.close_stream("s")
        result = store.append("s", b'{"id": 1}')
        assert result.stream_closed is True
        assert result.message is None

    def test_content_type_mismatch_raises(self, store):
        store.create("s", content_type="text/plain")
        with pytest.raises(ContentTypeMismatch):
            store.append("s", b'{"id": 1}', AppendOptions(content_type="text/csv"))

    def test_seq_conflict_raises(self, store):
        store.create("s")
        store.append("s", b'{"id": 1}', AppendOptions(seq="5"))
        with pytest.raises(SequenceConflict):
            store.append("s", b'{"id": 2}', AppendOptions(seq="5"))

    def test_seq_advances_lexicographically(self, store):
        """`Stream-Seq` is an opaque string compared byte-wise, so "10" after
        "9" is a conflict and a writer that wants ordering pads its values."""
        store.create("s")
        store.append("s", b'{"id": 1}', AppendOptions(seq="9"))
        with pytest.raises(SequenceConflict):
            store.append("s", b'{"id": 2}', AppendOptions(seq="10"))

        store.create("padded")
        store.append("padded", b'{"id": 1}', AppendOptions(seq="09"))
        result = store.append("padded", b'{"id": 2}', AppendOptions(seq="10"))
        assert result.message is not None

    def test_append_with_close_closes_the_stream(self, store):
        """`Stream-Closed: true` on a POST with a body: append, then close.

        The handler expresses this as `AppendOptions(close=True)`. A store
        that ignores the flag hands the client a 204 implying the close
        happened while the stream stays writable forever.
        """
        store.create("s")
        result = store.append("s", b'{"id": 1}', AppendOptions(close=True))
        assert result.stream_closed is True
        assert result.message is not None, "the body is appended before the close"
        assert store.get("s").closed is True
        refused = store.append("s", b'{"id": 2}')
        assert refused.stream_closed is True
        assert refused.message is None

    # =========================================================================
    # Batch append
    # =========================================================================

    def test_append_many_to_a_closed_stream_refuses_every_item(self, store):
        store.create("s")
        store.close_stream("s")
        results = store.append_many("s", [(b'{"id": 1}', None), (b'{"id": 2}', None)])
        assert all(r.stream_closed and r.message is None for r in results)
        messages, _ = store.read("s")
        assert messages == []

    def test_a_closed_stream_refuses_a_conflicting_batch_rather_than_raising(
        self, store
    ):
        """Closed outranks both conflicts, for a batch as for a single append.

        `test_append_many_to_a_closed_stream_refuses_every_item` uses a clean
        batch, so it cannot see a store that forgets the stream is closed while
        scanning for conflicts: such a store raises `SequenceConflict` here and
        answers the clean batch correctly, which is a refusal turned into a 400
        on one backend and not the other — #181's own shape.
        """
        store.create("s", content_type="application/json")
        store.append("s", b'{"n": 1}', AppendOptions(seq="5", close=True))

        results = store.append_many(
            "s",
            [
                (b'{"n": 2}', AppendOptions(seq="1")),
                (b"raw", AppendOptions(content_type="text/plain")),
            ],
        )
        assert all(r.stream_closed and r.message is None for r in results)

    def test_append_many_validates_content_type_per_item(self, store):
        store.create("s", content_type="text/plain")
        with pytest.raises(ContentTypeMismatch):
            store.append_many(
                "s", [(b"ok", None), (b"no", AppendOptions(content_type="text/csv"))]
            )
        messages, _ = store.read("s")
        assert messages == [], "a refused batch must write nothing"

    def test_append_many_validates_seq_per_item(self, store):
        store.create("s")
        store.append("s", b'{"id": 1}', AppendOptions(seq="5"))
        with pytest.raises(SequenceConflict):
            store.append_many("s", [(b'{"id": 2}', AppendOptions(seq="5"))])

    def test_a_seq_conflict_late_in_a_batch_writes_nothing(self, store):
        """All-or-nothing, on the *other* conflict.

        A single-item batch cannot tell a refusal apart from a refusal that
        happened to leave nothing behind, and the content-type case above is the
        only multi-item one — so a store could scan for content type, skip
        `Stream-Seq` entirely, and stay green while a conflicting batch left its
        prefix written. The durable store's transaction rolls back either way;
        the in-memory store has nothing to roll back and must refuse up front.
        """
        store.create("s")
        with pytest.raises(SequenceConflict):
            store.append_many(
                "s",
                [
                    (b'{"id": 1}', AppendOptions(seq="2")),
                    (b'{"id": 2}', AppendOptions(seq="1")),
                ],
            )
        messages, _ = store.read("s")
        assert messages == [], "a refused batch must write nothing"
        assert store.get("s").last_seq is None

    def test_a_batch_conflicting_with_the_stream_writes_nothing(self, store):
        """The same rule, against the sequence the stream already had.

        The sibling above starts from a fresh stream, so its conflict is between
        two items of the batch — which a store that never read the stream's own
        `Stream-Seq` still catches. This one puts an *admissible* item in front
        of an item that conflicts with the pre-batch value, which is the only
        arrangement that can leave a written prefix behind. Getting it wrong
        breaks all-or-nothing on one backend and not the other: the durable
        store's transaction rolls the prefix back whatever it decided.
        """
        store.create("s")
        store.append("s", b'{"id": 0}', AppendOptions(seq="5"))

        with pytest.raises(SequenceConflict):
            store.append_many(
                "s",
                [
                    (b'{"id": 1}', None),
                    (b'{"id": 2}', AppendOptions(seq="3")),
                ],
            )
        messages, _ = store.read("s")
        assert len(messages) == 1, "the admissible item must not have been written"
        assert store.get("s").last_seq == "5"

    def test_append_many_advances_seq_like_a_loop_of_append(self, store):
        store.create("s")
        store.append_many(
            "s",
            [
                (b'{"id": 1}', AppendOptions(seq="1")),
                (b'{"id": 2}', AppendOptions(seq="2")),
            ],
        )
        with pytest.raises(SequenceConflict):
            store.append("s", b'{"id": 3}', AppendOptions(seq="2"))

    def test_append_many_close_item_refuses_the_rest(self, store):
        """A batch is a loop of `append`: an item with `close=True` closes the
        stream, and the items after it observe the closed stream."""
        store.create("s")
        results = store.append_many(
            "s",
            [
                (b'{"id": 1}', None),
                (b'{"id": 2}', AppendOptions(close=True)),
                (b'{"id": 3}', None),
            ],
        )
        assert results[0].message is not None and not results[0].stream_closed
        assert results[1].message is not None and results[1].stream_closed
        assert results[2].message is None and results[2].stream_closed
        assert store.get("s").closed is True
        messages, _ = store.read("s")
        assert len(messages) == 2, "the item after the close must not be written"

    # -------------------------------------------------------------------------
    # Batch payload validity (#214)
    # -------------------------------------------------------------------------

    def test_a_bad_body_late_in_a_batch_writes_nothing(self, store):
        """All-or-nothing covers the *body*, not only the options.

        This is the case that made #214: the in-memory store's pre-flight read
        the options and never the payloads, so the loop of `append` behind it
        raised `InvalidJson` on item two with item one already persisted — a
        written prefix sitting behind a refusal. The durable store encoded each
        item on its own and did not raise at all.

        The first item is deliberately admissible, because a single-item batch
        cannot tell "refused" from "refused and happened to leave nothing".
        """
        store.create("s", content_type="application/json")
        with pytest.raises(InvalidJson):
            store.append_many("s", [(b'{"id": 1}', None), (b"not json", None)])
        messages, _ = store.read("s")
        assert messages == [], "a refused batch must write nothing"

    def test_an_empty_array_late_in_a_batch_writes_nothing(self, store):
        """The other body the protocol rejects, and a separate code path.

        `[]` parses, so a store could validate JSON syntax and still take the
        no-op append the protocol says is a 400 (§7.1). Same prefix problem.
        """
        store.create("s", content_type="application/json")
        with pytest.raises(EmptyJsonArray):
            store.append_many("s", [(b'{"id": 1}', None), (b"[]", None)])
        messages, _ = store.read("s")
        assert messages == [], "a refused batch must write nothing"

    def test_a_single_item_batch_with_a_bad_body_raises(self, store):
        """The narrow case, so a store cannot pass by only checking item two
        onwards — a batch of one is still a batch.

        Named for the raise and nothing more, because that is all it can see.
        Deleting the pre-flight's `check_payload` leaves this test green on both
        backends: with one item there is no prefix to strand, so "refused by the
        pre-flight" and "raised by the write that followed" leave identical
        state. The sibling above is what pins the pre-flight; this pins only
        that a lone bad body does not get in.
        """
        store.create("s", content_type="application/json")
        with pytest.raises(InvalidJson):
            store.append_many("s", [(b"not json", None)])
        messages, _ = store.read("s")
        assert messages == []

    def test_a_closed_stream_answers_closed_rather_than_raising_on_a_bad_body(
        self, store
    ):
        """Closed outranks payload validity, exactly as it outranks the two
        conflicts.

        A refused item is never written, so its body is never parsed — which is
        what a loop of `append` does, since `append` reports the closed stream
        before it looks at the data. Checking the payloads in a pass of their own
        would tell a client to fix a body whose write can never land.
        """
        store.create("s", content_type="application/json")
        store.close_stream("s")
        results = store.append_many("s", [(b"not json", None)])
        assert results[0].stream_closed and results[0].message is None

    def test_a_bad_body_after_a_close_in_the_batch_is_not_parsed_either(self, store):
        """The same rule where the close comes from *inside* the batch.

        The sibling above starts from an already-closed stream, which a store
        could handle by short-circuiting before it ever reaches the per-item
        loop. Here item one closes the stream and item two — refused because of
        it — carries a body that would raise. It must not.
        """
        store.create("s", content_type="application/json")
        results = store.append_many(
            "s",
            [(b'{"id": 1}', AppendOptions(close=True)), (b"not json", None)],
        )
        assert results[0].message is not None
        assert results[1].message is None and results[1].stream_closed
        messages, _ = store.read("s")
        assert len(messages) == 1

    def test_a_non_json_stream_takes_a_body_that_is_not_json(self, store):
        """Only a stream declared `application/json` constrains its bodies.

        The guard against over-correcting #214 into a store that parses every
        payload: a `text/plain` batch of arbitrary bytes must still be written.
        """
        store.create("s", content_type="text/plain")
        results = store.append_many("s", [(b"not json", None), (b"[]", None)])
        assert all(r.message is not None for r in results)
        messages, _ = store.read("s")
        assert len(messages) == 2

    async def test_append_many_does_not_advance_seq_for_a_refused_item(self, store):
        """An item the fence refuses is not written, so it cannot move
        `Stream-Seq` for the items after it.

        A loop of `append` gets this for free: `last_seq` is only assigned after
        a write lands. A batch has to say so, because its admission scan walks
        every item before anything is written — and if the scan advances the seq
        view on an item that will never be written, a later legitimate item
        collides with a sequence nothing ever took. #181: the two stores
        hand-rolled that scan separately and only one of them got this right.
        """
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"n": 0}', self._producer("p", 2, 0))
        results = await self._sync(
            store.append_many,
            "s",
            [
                # Stale epoch: refused, so its seq="9" is never taken.
                (
                    b'{"n": 1}',
                    AppendOptions(
                        producer_id="p", producer_epoch=1, producer_seq=0, seq="9"
                    ),
                ),
                (b'{"n": 2}', AppendOptions(seq="3")),
            ],
        )
        assert isinstance(results[0].producer_result, ProducerStaleEpoch)
        assert results[0].message is None
        assert results[1].message is not None, (
            "seq='3' is free: the refused item never took seq='9'"
        )
        stream = await self._sync(store.get, "s")
        assert stream.last_seq == "3"

    async def test_append_many_to_a_closed_stream_recognises_the_closing_producer(
        self, store
    ):
        """A batch is admitted on the same terms as a single append, including
        the idempotent re-send of the append that closed the stream.

        `append` reports that tuple as a duplicate so the producer can tell "my
        close landed" from "someone else closed this". A batch that answers a
        bare "closed" to the same tuple tells the producer to give up on a write
        that in fact succeeded.
        """
        await self._sync(store.create, "s")
        closing = AppendOptions(
            producer_id="p", producer_epoch=1, producer_seq=0, close=True
        )
        await store.append_with_producer("s", b'{"n": 1}', closing)

        results = await self._sync(store.append_many, "s", [(b'{"n": 1}', closing)])

        assert results[0].stream_closed is True
        assert results[0].message is None
        assert isinstance(results[0].producer_result, ProducerDuplicate)

    async def test_append_many_items_after_a_close_are_admitted_like_a_loop(
        self, store
    ):
        """The items after an in-batch close observe the stream the close left
        behind — including the closing tuple, so a producer re-sending its own
        closing append inside the same batch is told it is a duplicate rather
        than a bare "closed"."""
        await self._sync(store.create, "s")
        closing = AppendOptions(
            producer_id="p", producer_epoch=1, producer_seq=0, close=True
        )
        results = await self._sync(
            store.append_many, "s", [(b'{"n": 1}', closing), (b'{"n": 1}', closing)]
        )

        assert results[0].message is not None and results[0].stream_closed
        assert results[1].message is None and results[1].stream_closed
        assert isinstance(results[1].producer_result, ProducerDuplicate)

    async def test_append_many_fences_the_second_item_against_the_first(self, store):
        """Within one batch a producer's state advances item by item, so a
        second item repeating the first item's sequence is a duplicate — the
        same answer a loop of `append` gives."""
        await self._sync(store.create, "s")
        opts = self._producer("p", 1, 0)
        results = await self._sync(
            store.append_many, "s", [(b'{"n": 1}', opts), (b'{"n": 2}', opts)]
        )

        assert isinstance(results[0].producer_result, ProducerAccepted)
        assert isinstance(results[1].producer_result, ProducerDuplicate)
        assert results[1].message is None
        messages, _ = await self._sync(store.read, "s")
        assert len(messages) == 1, "the duplicate must not be written"

    # -------------------------------------------------------------------------
    # Batch array flattening (#214)
    # -------------------------------------------------------------------------

    def test_append_many_flattens_a_json_array_like_append(self, store):
        """A batch item whose body is a top-level JSON array becomes one message
        per element, exactly as passing that body to `append` would.

        The two backends used to disagree here (#214): the in-memory store
        inherited the flatten by delegating to `append`, and the durable store
        declined it, on the grounds that a batch item is one event whose payload
        may be a list. Deciding for the flatten keeps `append_many` semantically
        identical to a loop of `append` *within* each backend, which is what
        both of them promise, and stops a list payload reaching `replay()`,
        where it raises.
        """
        import json

        store.create("s", content_type="application/json")
        store.append_many("s", [(b'[{"id": 1}, {"id": 2}]', None)])
        messages, _ = store.read("s")
        assert [json.loads(m.data) for m in messages] == [{"id": 1}, {"id": 2}]

    def test_a_flattened_batch_item_still_returns_exactly_one_result(self, store):
        """One `AppendResult` per input item, even for an item that wrote more
        than one message — its result carries the last of them, which is the
        offset a caller resumes from. This is why the old divergence was easy to
        miss: nothing in the return value reveals how many messages an item
        became."""
        import json

        store.create("s", content_type="application/json")
        results = store.append_many(
            "s", [(b'[{"id": 1}, {"id": 2}]', None), (b'{"id": 3}', None)]
        )
        assert len(results) == 2
        assert all(r.message is not None for r in results)
        messages, _ = store.read("s")
        assert len(messages) == 3
        # Parsed, not compared byte-for-byte: the two backends re-serialise a
        # flattened element with different whitespace, which
        # `test_a_json_array_append_flattens_into_separate_messages` already
        # treats as immaterial.
        assert json.loads(results[0].message.data) == {"id": 2}
        assert json.loads(results[1].message.data) == {"id": 3}

    # =========================================================================
    # Close
    # =========================================================================

    def test_close_reports_the_final_offset(self, store):
        store.create("s")
        store.append("s", b'{"id": 1}')
        result = store.close_stream("s")
        assert result is not None
        assert result.final_offset
        assert result.already_closed is False

    def test_close_is_idempotent(self, store):
        store.create("s")
        first = store.close_stream("s")
        second = store.close_stream("s")
        assert second.already_closed is True
        assert second.final_offset == first.final_offset

    def test_close_of_a_missing_stream_is_none(self, store):
        assert store.close_stream("nope") is None

    def test_closed_shows_on_the_stream(self, store):
        store.create("s")
        assert store.get("s").closed is False
        store.close_stream("s")
        assert store.get("s").closed is True

    async def test_a_different_producer_cannot_reclose_a_closed_stream(self, store):
        """An already-closed stream is reported, never re-closed.

        Overwriting `closed_by` with the second producer's tuple would make a
        retry of the *original* closing tuple unrecognisable as a duplicate —
        the exact idempotence the tuple is recorded for.
        """
        await self._sync(store.create, "s")
        await store.close_stream_with_producer("s", "p1", 0, 0)
        result = await store.close_stream_with_producer("s", "p2", 0, 0)
        assert result.already_closed is True
        assert isinstance(result.producer_result, ProducerStreamClosed)
        by = (await self._sync(store.get, "s")).closed_by
        assert by is not None and by.producer_id == "p1", "closed_by must not move"

    async def test_a_retried_closing_tuple_is_a_duplicate(self, store):
        await self._sync(store.create, "s")
        await store.close_stream_with_producer("s", "p", 0, 0)
        again = await store.close_stream_with_producer("s", "p", 0, 0)
        assert again.already_closed is True
        assert isinstance(again.producer_result, ProducerDuplicate)

    # =========================================================================
    # Producer fencing
    # =========================================================================

    # Async cases need their sync setup routed through the backend's own
    # sync/async bridge: a store on a database cannot be called directly from
    # an async test. The default just calls through, for stores that don't care.
    @staticmethod
    async def _sync(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    @staticmethod
    def _producer(pid: str, epoch: int, seq: int) -> AppendOptions:
        return AppendOptions(producer_id=pid, producer_epoch=epoch, producer_seq=seq)

    async def test_a_new_producer_must_open_at_seq_zero(self, store):
        await self._sync(store.create, "s")
        result = await store.append_with_producer(
            "s", b'{"id": 1}', self._producer("p", 0, 3)
        )
        assert isinstance(result.producer_result, ProducerSequenceGap)
        assert result.message is None

    async def test_a_producer_opening_at_zero_is_accepted(self, store):
        await self._sync(store.create, "s")
        result = await store.append_with_producer(
            "s", b'{"id": 1}', self._producer("p", 0, 0)
        )
        assert isinstance(result.producer_result, ProducerAccepted)
        assert result.message is not None

    async def test_a_replayed_seq_is_a_duplicate_not_a_write(self, store):
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 0, 0))
        again = await store.append_with_producer(
            "s", b'{"id": 1}', self._producer("p", 0, 0)
        )
        assert isinstance(again.producer_result, ProducerDuplicate)
        assert again.message is None
        messages, _ = await self._sync(store.read, "s")
        assert len(messages) == 1, "a duplicate must not write a second time"

    async def test_a_stale_epoch_is_refused(self, store):
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 5, 0))
        result = await store.append_with_producer(
            "s", b'{"id": 2}', self._producer("p", 4, 0)
        )
        assert isinstance(result.producer_result, ProducerStaleEpoch)

    async def test_a_new_epoch_must_also_open_at_zero(self, store):
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 0, 0))
        result = await store.append_with_producer(
            "s", b'{"id": 2}', self._producer("p", 1, 7)
        )
        assert isinstance(result.producer_result, ProducerInvalidEpochSeq)

    async def test_a_gap_is_refused_with_the_expected_seq(self, store):
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 0, 0))
        result = await store.append_with_producer(
            "s", b'{"id": 2}', self._producer("p", 0, 5)
        )
        assert isinstance(result.producer_result, ProducerSequenceGap)
        assert result.producer_result.expected_seq == 1

    async def test_a_refused_write_does_not_advance_the_producer(self, store):
        """A rejected append must leave the sequence where it was."""
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 0, 0))
        await store.append_with_producer("s", b'{"id": 2}', self._producer("p", 0, 9))
        accepted = await store.append_with_producer(
            "s", b'{"id": 2}', self._producer("p", 0, 1)
        )
        assert isinstance(accepted.producer_result, ProducerAccepted)

    async def test_append_with_producer_to_a_closed_stream_reports_closed(self, store):
        """Closed is decided before fencing, on this door too.

        Nothing drove `append_with_producer` against a closed stream, and the
        two stores had drifted underneath: one answered with the fencing
        outcome for a write that could never land anyway, so a producer whose
        seq happened to be wrong learned about the gap instead of learning the
        stream was closed.
        """
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 0, 0))
        await self._sync(store.close_stream, "s")

        result = await store.append_with_producer(
            "s", b'{"id": 2}', self._producer("p", 0, 9)
        )
        assert result.stream_closed is True
        assert result.message is None
        assert isinstance(result.producer_result, ProducerStreamClosed)

    async def test_append_with_producer_retrying_a_closing_tuple_is_a_duplicate(
        self, store
    ):
        """A producer that loses the response to its own closing append must be
        able to tell "my close landed" from "someone else closed this"."""
        await self._sync(store.create, "s")
        opts = AppendOptions(
            producer_id="p", producer_epoch=0, producer_seq=0, close=True
        )
        await store.append_with_producer("s", b'{"id": 1}', opts)

        again = await store.append_with_producer("s", b'{"id": 1}', opts)
        assert again.stream_closed is True
        assert isinstance(again.producer_result, ProducerDuplicate)
        assert again.message is None

    # =========================================================================
    # Producer options on the plain `append` path
    # =========================================================================
    #
    # A protocol server routes fenced writes to `append_with_producer`, so every
    # case above goes through that door. Nothing said what the *other* door does
    # with the same options — and the two stores answered differently:
    # `StreamStore.append` validated the producer inline and recognised the
    # idempotent close-duplicate, while `DjangoStreamStore.append` ignored
    # `options.producer_id` entirely, all while its docstring claimed "outcomes,
    # all now matching the in-memory store".
    #
    # `WritableStore.append` is public and takes an `AppendOptions` with those
    # fields on it, so a consumer calling it directly got adapter-dependent
    # behaviour. These cases make the two doors agree.

    async def test_append_honours_producer_fencing(self, store):
        """The same options through `append` reach the same verdict."""
        await self._sync(store.create, "s")
        await self._sync(store.append, "s", b'{"id": 1}', self._producer("p", 0, 0))

        stale = await self._sync(
            store.append, "s", b'{"id": 2}', self._producer("p", 0, 0)
        )
        assert isinstance(stale.producer_result, ProducerDuplicate)
        assert stale.message is None

        messages, _ = await self._sync(store.read, "s")
        assert len(messages) == 1, "a duplicate must not write a second time"

    async def test_append_refuses_a_stale_epoch(self, store):
        await self._sync(store.create, "s")
        await self._sync(store.append, "s", b'{"id": 1}', self._producer("p", 5, 0))
        result = await self._sync(
            store.append, "s", b'{"id": 2}', self._producer("p", 4, 0)
        )
        assert isinstance(result.producer_result, ProducerStaleEpoch)
        assert result.message is None

    async def test_append_refuses_a_sequence_gap(self, store):
        await self._sync(store.create, "s")
        await self._sync(store.append, "s", b'{"id": 1}', self._producer("p", 0, 0))
        result = await self._sync(
            store.append, "s", b'{"id": 2}', self._producer("p", 0, 5)
        )
        assert isinstance(result.producer_result, ProducerSequenceGap)
        assert result.producer_result.expected_seq == 1

    async def test_append_accepts_a_well_formed_producer_write(self, store):
        await self._sync(store.create, "s")
        result = await self._sync(
            store.append, "s", b'{"id": 1}', self._producer("p", 0, 0)
        )
        assert isinstance(result.producer_result, ProducerAccepted)
        assert result.message is not None

    async def test_append_shares_producer_state_with_append_with_producer(self, store):
        """One producer, one sequence — whichever door each write came through."""
        await self._sync(store.create, "s")
        await self._sync(store.append, "s", b'{"id": 1}', self._producer("p", 0, 0))
        result = await store.append_with_producer(
            "s", b'{"id": 2}', self._producer("p", 0, 1)
        )
        assert isinstance(result.producer_result, ProducerAccepted)

    async def test_append_to_a_closed_stream_reports_the_duplicate_close(self, store):
        """Re-sending the append that closed the stream is idempotent, not a
        bare refusal — the producer needs to tell "my close landed" apart from
        "someone else closed this"."""
        await self._sync(store.create, "s")
        opts = AppendOptions(
            producer_id="p", producer_epoch=0, producer_seq=0, close=True
        )
        await self._sync(store.append, "s", b'{"id": 1}', opts)

        again = await self._sync(store.append, "s", b'{"id": 1}', opts)
        assert again.stream_closed is True
        assert isinstance(again.producer_result, ProducerDuplicate)

    async def test_append_to_a_closed_stream_by_another_producer_is_refused(
        self, store
    ):
        await self._sync(store.create, "s")
        await self._sync(
            store.append,
            "s",
            b'{"id": 1}',
            AppendOptions(
                producer_id="p", producer_epoch=0, producer_seq=0, close=True
            ),
        )
        other = await self._sync(
            store.append, "s", b'{"id": 2}', self._producer("other", 0, 0)
        )
        assert other.stream_closed is True
        assert isinstance(other.producer_result, ProducerStreamClosed)
        assert other.message is None

    # =========================================================================
    # Long-poll
    # =========================================================================

    async def test_wait_returns_immediately_when_messages_exist(self, store):
        await self._sync(store.create, "s")
        await self._sync(store.append, "s", b'{"id": 1}')
        messages, _, _ = await store.wait_for_messages("s", "-1", 5.0)
        assert len(messages) == 1

    async def test_wait_times_out_empty_when_caught_up(self, store):
        await self._sync(store.create, "s")
        await self._sync(store.append, "s", b'{"id": 1}')
        head = await self._sync(store.get_current_offset, "s")
        messages, timed_out, _ = await store.wait_for_messages("s", head, 0.15)
        assert messages == []
        assert timed_out is True

    async def test_wait_returns_on_a_closed_stream(self, store):
        await self._sync(store.create, "s")
        await self._sync(store.append, "s", b'{"id": 1}')
        head = await self._sync(store.get_current_offset, "s")
        await self._sync(store.close_stream, "s")
        messages, _, closed = await store.wait_for_messages("s", head, 5.0)
        assert messages == []
        assert closed is True

    # =========================================================================
    # TTL
    # =========================================================================

    def test_an_expired_stream_reads_as_absent(self, store):
        store.create("s", ttl_seconds=0)
        # ttl_seconds=0 expires as soon as any time has passed.
        import time

        time.sleep(0.01)
        assert store.get("s") is None
        assert store.has("s") is False

    def test_an_expired_stream_has_no_current_offset(self, store):
        """Expiry applies to the head report exactly as to every other read."""
        import time

        store.create("s", ttl_seconds=0)
        time.sleep(0.01)
        assert store.get_current_offset("s") is None

    def test_an_expired_stream_refuses_appends(self, store):
        import time

        store.create("s", ttl_seconds=0)
        time.sleep(0.01)
        with pytest.raises(StreamNotFound):
            store.append("s", b'{"id": 1}')

    def test_touch_is_a_no_op_for_a_missing_stream(self, store):
        store.touch("nope")  # must not raise

    # =========================================================================
    # Response formatting
    # =========================================================================

    def test_format_response_for_a_missing_stream_raises(self, store):
        """Not `b""`: an empty body silently drops JSON-array framing on the
        expiry race, where a failure names what actually happened."""
        with pytest.raises(StreamNotFound):
            store.format_response("nope", [])

    def test_json_mode_formats_one_array(self, store):
        store.create("s", content_type="application/json")
        store.append("s", b'{"id": 1}')
        store.append("s", b'{"id": 2}')
        messages, _ = store.read("s")
        body = store.format_response("s", messages)
        import json

        assert json.loads(body) == [{"id": 1}, {"id": 2}]

    def test_a_json_array_append_flattens_into_separate_messages(self, store):
        """`[a, b]` is two messages, not one message that is an array.

        The protocol flattens a top-level array one level on append. A store
        that keeps the array whole reads back a different shape and, for the
        durable store, hands a *list* to everything expecting an event object.
        """
        import json

        store.create("s", content_type="application/json")
        store.append("s", b'[{"id": 1}, {"id": 2}]')
        messages, _ = store.read("s")
        assert json.loads(store.format_response("s", messages)) == [
            {"id": 1},
            {"id": 2},
        ]

    def test_an_empty_json_array_is_refused_on_append(self, store):
        from rakaia.types import EmptyJsonArray

        store.create("s", content_type="application/json")
        with pytest.raises(EmptyJsonArray):
            store.append("s", b"[]")

    def test_malformed_json_is_refused_before_anything_is_written(self, store):
        from rakaia.types import InvalidJson

        store.create("s", content_type="application/json")
        with pytest.raises(InvalidJson):
            store.append("s", b"{not json")
        messages, _ = store.read("s")
        assert messages == [], "a refused append must not leave a message behind"

    # =========================================================================
    # Content types other than JSON
    # =========================================================================
    #
    # The protocol lets a stream declare any content type; only
    # `application/json` turns on JSON mode. A store that assumes every payload
    # is JSON crashes on the rest — as a 500, since a `json.JSONDecodeError` is
    # not one of the named store failures.

    def test_a_text_stream_round_trips_its_payload(self, store):
        store.create("s", content_type="text/plain")
        store.append("s", b"hello")
        messages, _ = store.read("s")
        assert [m.data for m in messages] == [b"hello"]

    def test_a_text_stream_is_not_reformatted_as_json(self, store):
        """Text that happens to be JSON is still text, byte for byte."""
        store.create("s", content_type="text/plain")
        store.append("s", b'{"a":  1}')
        messages, _ = store.read("s")
        assert [m.data for m in messages] == [b'{"a":  1}']

    def test_a_text_stream_formats_as_the_concatenated_payloads(self, store):
        store.create("s", content_type="text/csv")
        store.append("s", b"a,b\n")
        store.append("s", b"c,d\n")
        messages, _ = store.read("s")
        assert store.format_response("s", messages) == b"a,b\nc,d\n"

    def test_a_binary_payload_round_trips(self, store):
        """Bytes that are not valid UTF-8 must survive too."""
        payload = b"\x89PNG\r\n\x1a\n\xff\xfe"
        store.create("s", content_type="application/octet-stream")
        store.append("s", payload)
        messages, _ = store.read("s")
        assert [m.data for m in messages] == [payload]

    # =========================================================================
    # Offsets
    # =========================================================================

    def test_reading_from_a_foreign_offset_is_refused(self, store):
        """An offset this store did not issue must fail, not resolve to some
        other position.

        The server's syntactic guard cannot tell whose offset a token is — the protocol makes them opaque, not uniform
        (§6). A store that parses one leniently silently returns the wrong
        window: `int("0_5")` is 5 in Python, so the in-memory store's compound
        offset reads as an unrelated position rather than an error.
        """
        from rakaia.types import InvalidOffset

        store.create("s", content_type="application/json")
        store.append("s", b'{"id": 1}')
        messages, _ = store.read("s")
        head = messages[-1].offset

        with pytest.raises(InvalidOffset):
            store.read("s", _foreign_offset(head))


def _foreign_offset(own_offset: str) -> str:
    """An offset in the *other* store's format, to hand to this one."""
    return "1_00000000000000000005" if "_" not in own_offset else "5"
