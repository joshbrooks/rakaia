"""Tests for rakaia.context: ambient provenance merged into append metadata."""

from __future__ import annotations

from rakaia.context import get_provenance, merge_provenance, provenance
from rakaia.store import StreamStore
from rakaia.types import AppendOptions


class TestProvenance:
    def test_no_provenance_is_empty(self):
        assert get_provenance() == {}
        assert merge_provenance(None) is None
        assert merge_provenance({}) is None

    def test_provenance_block_sets_and_restores(self):
        with provenance(user=5, url="/x"):
            assert get_provenance() == {"user": 5, "url": "/x"}
        assert get_provenance() == {}  # restored on exit

    def test_nested_blocks_merge(self):
        with provenance(user=5):
            with provenance(url="/y"):
                assert get_provenance() == {"user": 5, "url": "/y"}
            assert get_provenance() == {"user": 5}  # inner restored

    def test_explicit_metadata_overrides_ambient(self):
        with provenance(user=5, url="/x"):
            merged = merge_provenance({"user": 99})
        assert merged == {"user": 99, "url": "/x"}  # explicit user wins


class TestAppendPicksUpProvenance:
    def test_append_within_provenance_stamps_metadata(self):
        store = StreamStore()
        store.create("s", content_type="application/octet-stream")
        with provenance(user=7, url="/submit"):
            result = store.append("s", b"x", AppendOptions(label="update"))
        assert result.message is not None
        assert result.message.metadata == {"user": 7, "url": "/submit"}

    def test_append_outside_provenance_has_no_metadata(self):
        store = StreamStore()
        store.create("s", content_type="application/octet-stream")
        result = store.append("s", b"x", AppendOptions(label="update"))
        assert result.message is not None
        assert result.message.metadata is None

    def test_initial_create_is_not_stamped(self):
        # Provenance is merged at the public-append boundary only, so a stream's
        # initial-create message stays envelope-free even inside a block.
        store = StreamStore()
        with provenance(user=7):
            store.create("s", content_type="application/json", initial_data=b'{"a": 1}')
        messages, _ = store.read("s")
        assert messages[0].metadata is None
