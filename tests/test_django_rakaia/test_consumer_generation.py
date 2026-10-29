"""A consumer on the database notices a replaced stream and a dropped connection.

GitHub #350 and #351 through the pieces a deployment actually uses: the durable
stream store, the cursor table, and the error classes `django_rakaia` registers.
`transaction=True` because the consumer refuses to start inside a transaction
(and the alias-aware code is worth a real commit).
"""

from __future__ import annotations

import pytest
from django.db import OperationalError

from django_rakaia.consumer import django_consumer
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import ConsumerCursor, Stream


def _stream(*payloads: bytes) -> DjangoStreamStore:
    store = DjangoStreamStore()
    store.create("s")
    for payload in payloads:
        store.append("s", payload)
    return store


@pytest.mark.django_db(transaction=True)
class TestReplacedStream:
    def test_the_stamp_is_saved_with_the_cursor(self) -> None:
        store = _stream(b"a")
        django_consumer(store, "s", "c").run(lambda _m: None, on_error="halt")

        row = ConsumerCursor.objects.get(consumer_id="c", stream_path="s")
        assert row.generation == store.get_generation("s")

    def test_a_deleted_and_recreated_stream_stops_the_consumer(self) -> None:
        store = _stream(b"a", b"b", b"c")
        consumer = django_consumer(store, "s", "c")
        seen: list[bytes] = []

        def one_then_stop(message):
            if seen:
                raise OperationalError("stop after one")
            seen.append(message.data)

        consumer.run(one_then_stop, on_error="halt")
        before = ConsumerCursor.objects.get(consumer_id="c", stream_path="s").offset

        store.delete("s")
        store.create("s")
        store.append("s", b"new")
        result = consumer.run(lambda m: seen.append(m.data), on_error="halt")

        assert result.status == "replaced" and result.needs_rebuild
        assert seen == [b"a"]
        assert (
            ConsumerCursor.objects.get(consumer_id="c", stream_path="s").offset
            == before
        )

    def test_an_expired_and_recreated_stream_stops_the_consumer(self) -> None:
        store = _stream(b"a")
        consumer = django_consumer(store, "s", "c")
        consumer.run(lambda _m: None, on_error="halt")

        # Expiry reaps the stream the first time anything looks at it.
        Stream.objects.filter(stream_id="s").update(expires_at="2000-01-01T00:00:00Z")
        assert store.get_generation("s") is None
        store.create("s")
        store.append("s", b"new")

        result = consumer.run(lambda _m: pytest.fail("applied"), on_error="halt")
        assert result.status == "replaced"

    def test_a_cursor_saved_before_stamps_adopts_the_current_one(self) -> None:
        store = _stream(b"a", b"b")
        first = store.read("s")[0][0].offset
        ConsumerCursor.objects.create(consumer_id="c", stream_path="s", offset=first)
        seen: list[bytes] = []

        result = django_consumer(store, "s", "c").run(
            lambda m: seen.append(m.data), on_error="halt"
        )

        assert not result.needs_rebuild and seen == [b"b"]
        row = ConsumerCursor.objects.get(consumer_id="c", stream_path="s")
        assert row.generation == store.get_generation("s")
