"""Contract for a store that implements `TaggedReads` (#365).

Separate from `server_store_contract` so a store that only answers "everything
after this point" keeps passing that suite unchanged. Subclass `TaggedReadsContract`
and provide a `store` fixture returning a fresh, empty store that is also a
`StreamServerStore`::

    class TestMyStore(TaggedReadsContract):
        @pytest.fixture
        def store(self):
            return MyStore()
"""

from __future__ import annotations

import time

from rakaia.types import AppendOptions, StreamNotFound

from ._need_pytest import pytest


def _tagged(store, path: str, *rows: tuple[int, tuple[str, ...]]) -> None:
    """Append one `{"id": n}` event per `(n, tags)` row."""
    for n, tags in rows:
        store.append(path, b'{"id": %d}' % n, AppendOptions(tags=tags))


class TaggedReadsContract:
    """Contract every store that offers `read_tagged` must uphold."""

    def test_satisfies_the_tagged_reads_protocol(self, store):
        from rakaia import TaggedReads

        assert isinstance(store, TaggedReads)

    def test_returns_only_events_carrying_the_tag(self, store):
        store.create("s")
        _tagged(store, "s", (1, ("a",)), (2, ("b",)), (3, ("a", "b")), (4, ()))

        found = store.read_tagged("s", "a")

        assert [m.data for m in found] == [b'{"id": 1}', b'{"id": 3}']

    def test_an_event_must_carry_every_tag_given(self, store):
        store.create("s")
        _tagged(store, "s", (1, ("a",)), (2, ("b",)), (3, ("a", "b")), (4, ("a", "c")))

        found = store.read_tagged("s", "a", "b")

        assert [m.data for m in found] == [b'{"id": 3}']

    def test_the_order_and_repeats_of_tags_do_not_matter(self, store):
        store.create("s")
        _tagged(store, "s", (1, ("a", "b")))

        assert store.read_tagged("s", "b", "a", "b") == store.read_tagged("s", "a", "b")

    def test_no_match_is_an_empty_list(self, store):
        store.create("s")
        _tagged(store, "s", (1, ("a",)))

        assert store.read_tagged("s", "nope") == []

    def test_messages_equal_what_read_returns_for_the_same_events(self, store):
        store.create("s")
        _tagged(store, "s", (1, ("a", "z")), (2, ("b",)), (3, ("a",)))
        everything, _ = store.read("s")

        found = store.read_tagged("s", "a")

        assert found == [everything[0], everything[2]]

    def test_oldest_first_by_default(self, store):
        store.create("s")
        _tagged(store, "s", *[(n, ("a",)) for n in range(5)])

        offsets = [m.offset for m in store.read_tagged("s", "a")]

        assert offsets == sorted(offsets)
        assert len(offsets) == 5

    def test_newest_first_reverses_the_order(self, store):
        store.create("s")
        _tagged(store, "s", (1, ("a",)), (2, ("b",)), (3, ("a",)), (4, ("a",)))

        found = store.read_tagged("s", "a", newest_first=True)

        assert [m.data for m in found] == [
            b'{"id": 4}',
            b'{"id": 3}',
            b'{"id": 1}',
        ]

    def test_limit_keeps_the_oldest_when_oldest_first(self, store):
        store.create("s")
        _tagged(store, "s", *[(n, ("a",)) for n in range(5)])

        found = store.read_tagged("s", "a", limit=2)

        assert [m.data for m in found] == [b'{"id": 0}', b'{"id": 1}']

    def test_limit_keeps_the_newest_when_newest_first(self, store):
        store.create("s")
        _tagged(store, "s", *[(n, ("a",)) for n in range(5)])

        found = store.read_tagged("s", "a", newest_first=True, limit=2)

        assert [m.data for m in found] == [b'{"id": 4}', b'{"id": 3}']

    def test_limit_counts_matches_not_events_scanned(self, store):
        store.create("s")
        _tagged(store, "s", (1, ("a",)), (2, ()), (3, ()), (4, ("a",)), (5, ("a",)))

        found = store.read_tagged("s", "a", limit=2)

        assert [m.data for m in found] == [b'{"id": 1}', b'{"id": 4}']

    def test_a_limit_larger_than_the_matches_returns_them_all(self, store):
        store.create("s")
        _tagged(store, "s", (1, ("a",)))

        assert len(store.read_tagged("s", "a", limit=10)) == 1

    @pytest.mark.parametrize("limit", [0, -1])
    def test_a_limit_below_one_raises(self, store, limit):
        store.create("s")

        with pytest.raises(ValueError):
            store.read_tagged("s", "a", limit=limit)

    def test_no_tags_raises(self, store):
        store.create("s")

        with pytest.raises(ValueError):
            store.read_tagged("s")

    def test_a_tag_no_event_could_carry_raises(self, store):
        store.create("s")

        with pytest.raises(ValueError):
            store.read_tagged("s", "")

    def test_a_missing_stream_raises_stream_not_found(self, store):
        with pytest.raises(StreamNotFound):
            store.read_tagged("nope", "a")

    def test_a_missing_stream_is_not_found_even_with_bad_arguments(self, store):
        with pytest.raises(StreamNotFound):
            store.read_tagged("nope")

    def test_a_closed_stream_can_still_be_read(self, store):
        store.create("s")
        _tagged(store, "s", (1, ("a",)))
        store.close_stream("s")

        assert len(store.read_tagged("s", "a")) == 1

    def test_an_expired_stream_raises_stream_not_found(self, store):
        store.create("s", ttl_seconds=1)
        _tagged(store, "s", (1, ("a",)))
        time.sleep(1.2)

        with pytest.raises(StreamNotFound):
            store.read_tagged("s", "a")

    def test_does_not_extend_the_ttl_window(self, store):
        """Same as `peek`: a lookup by tag is not activity on the stream."""
        store.create("s", ttl_seconds=1)
        _tagged(store, "s", (1, ("a",)))
        time.sleep(0.6)
        store.read_tagged("s", "a")
        time.sleep(0.6)

        assert store.get("s") is None, "read_tagged kept the stream alive"

    def test_a_refused_call_does_not_extend_the_ttl_window(self, store):
        store.create("s", ttl_seconds=1)
        time.sleep(0.6)
        with pytest.raises(ValueError):
            store.read_tagged("s", "a", limit=0)
        time.sleep(0.6)

        assert store.get("s") is None, "a refused read_tagged kept the stream alive"

    def test_does_not_see_another_streams_events(self, store):
        store.create("s")
        store.create("t")
        _tagged(store, "s", (1, ("a",)))
        _tagged(store, "t", (2, ("a",)))

        assert [m.data for m in store.read_tagged("s", "a")] == [b'{"id": 1}']
