"""Tests for `rakaia.seed_stream` — the four lines everyone was retyping.

Every case here is forced by a call site that existed before the helper did:
the byte-identical pair in `test_replay.py`/`test_alerts.py` (store passed in,
plain dicts), the Django-side pair that builds its own store and returns it,
the one that passes pre-encoded bytes with a per-event label and metadata, the
management-command one that seeds the process-wide singleton twice, and the
closure in `test_replay.py` that needs a different `event_ts` per event.
"""

from __future__ import annotations

import json

import pytest

from rakaia import AppendOptions, StreamStore, seed_stream


class TestTheStore:
    def test_it_builds_an_in_memory_store_when_none_is_given(self):
        store = seed_stream("p", [{"n": 1}])
        assert isinstance(store, StreamStore)
        messages, _ = store.read("p")
        assert [json.loads(m.data)["n"] for m in messages] == [1]

    def test_it_uses_the_store_it_is_given_and_returns_it(self):
        mine = StreamStore()
        returned = seed_stream("p", [{"n": 1}], store=mine)
        assert returned is mine

    def test_it_seeds_any_writable_store(self):
        """The Django backend and the `get_store()` singleton are `WritableStore`
        implementations, not `StreamStore` — the helper must not assume either."""

        class Recording:
            def __init__(self):
                self.created: list[str] = []
                self.appended: list[tuple[str, bytes]] = []

            def has(self, path: str) -> bool:
                return path in self.created

            def create(self, path: str) -> None:
                self.created.append(path)

            def append(self, path: str, data: bytes, options=None) -> None:  # noqa: ARG002
                self.appended.append((path, data))

        backend = Recording()
        assert seed_stream("p", [{"n": 1}], store=backend) is backend
        assert backend.created == ["p"]
        assert backend.appended == [("p", b'{"n": 1}')]

    def test_seeding_twice_appends_rather_than_truncating(self):
        """`create()` is idempotent by store contract, so the `has()` guard the
        management-command helper carried is not needed."""
        store = seed_stream("p", [{"n": 1}])
        seed_stream("p", [{"n": 2}], store=store)
        messages, _ = store.read("p")
        assert [json.loads(m.data)["n"] for m in messages] == [1, 2]

    def test_no_events_still_creates_the_stream(self):
        store = seed_stream("p", [])
        assert store.has("p")


class TestThePayloads:
    def test_dicts_are_json_encoded(self):
        store = seed_stream("p", [{"n": 1}, {"n": 2}])
        messages, _ = store.read("p")
        assert [m.data for m in messages] == [b'{"n": 1}', b'{"n": 2}']

    def test_pre_encoded_bytes_are_appended_untouched(self):
        raw = b'{"key": "s1", "user_id": 9, "a": 1}'
        store = seed_stream("p", [raw])
        (msg,), _ = store.read("p")
        assert msg.data == raw

    def test_dicts_and_bytes_can_be_mixed(self):
        store = seed_stream("p", [{"n": 1}, b'{"n": 2}'])
        messages, _ = store.read("p")
        assert [json.loads(m.data)["n"] for m in messages] == [1, 2]

    def test_the_events_land_in_list_order(self):
        store = seed_stream("p", [{"n": i} for i in range(5)])
        messages, _ = store.read("p")
        assert [json.loads(m.data)["n"] for m in messages] == [0, 1, 2, 3, 4]


class TestThePerEventEnvelope:
    """Label, metadata and `event_ts` are per event, not per batch — the history
    materializer needs a different label on each event with metadata on only the
    first, and the backfill test needs a different `event_ts` on each."""

    def test_an_event_can_carry_its_own_append_options(self):
        store = seed_stream(
            "p",
            [
                ({"a": 1}, AppendOptions(label="insert", metadata={"user": 42})),
                ({"a": 2}, AppendOptions(label="update")),
            ],
        )
        first, second = store.read("p")[0]
        assert (first.label, first.metadata) == ("insert", {"user": 42})
        assert second.label == "update"
        assert second.metadata in (None, {})

    def test_bytes_can_carry_options_too(self):
        store = seed_stream("p", [(b'{"a": 1}', AppendOptions(label="insert"))])
        (msg,), _ = store.read("p")
        assert (msg.data, msg.label) == (b'{"a": 1}', "insert")

    def test_per_event_event_ts_is_recorded(self):
        store = seed_stream(
            "p",
            [
                ({"k": "a0"}, AppendOptions(event_ts=100.0)),
                ({"k": "a1"}, AppendOptions(event_ts=400.0)),
            ],
        )
        messages, _ = store.read("p")
        assert [m.event_ts for m in messages] == [100.0, 400.0]

    def test_enveloped_and_bare_events_can_be_mixed(self):
        store = seed_stream("p", [{"a": 1}, ({"a": 2}, AppendOptions(label="x"))])
        first, second = store.read("p")[0]
        assert (first.label, second.label) == ("", "x")


class TestTheEncoderHook:
    """A core-tier helper cannot import `DjangoJSONEncoder`, and a second
    `json.dumps` rule is the drift `django_rakaia/envelope.py` warns about — so
    the encoder is a parameter and there is exactly one `dumps` call."""

    def test_the_encoder_class_is_used_for_dict_payloads(self):
        class Shouty(json.JSONEncoder):
            def default(self, o):
                return str(o).upper()

        store = seed_stream("p", [{"v": object.__new__(_Thing)}], encoder=Shouty)
        (msg,), _ = store.read("p")
        assert json.loads(msg.data)["v"] == "THING"

    def test_without_an_encoder_an_unserialisable_payload_raises(self):
        with pytest.raises(TypeError):
            seed_stream("p", [{"v": object.__new__(_Thing)}])

    def test_the_encoder_is_not_applied_to_pre_encoded_bytes(self):
        class Exploding(json.JSONEncoder):
            def default(self, o):  # noqa: ARG002  # pragma: no cover
                raise AssertionError("bytes must not be re-encoded")

        store = seed_stream("p", [b'{"v": 1}'], encoder=Exploding)
        (msg,), _ = store.read("p")
        assert msg.data == b'{"v": 1}'


class _Thing:
    def __str__(self) -> str:
        return "thing"
