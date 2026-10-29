"""GitHub #346: a crash between the log write and meta.json reissues an offset."""

from __future__ import annotations

import pytest

from rakaia.jsonl_store import JsonlStreamStore

PATH = "s"


def test_crash_between_log_and_meta_does_not_reuse_an_offset(tmp_path):
    store = JsonlStreamStore(tmp_path / "streams", fsync=False)
    store.create(PATH)
    store.append(PATH, b"one")
    store.append(PATH, b"two")

    # Simulate the process dying after the log write but before meta.json.
    real_save = store._save_meta

    def die(_path, _meta):
        raise KeyboardInterrupt("killed before meta.json")

    store._save_meta = die  # type: ignore[method-assign]
    with pytest.raises(KeyboardInterrupt):
        store.append(PATH, b"three")
    store._save_meta = real_save  # type: ignore[method-assign]

    # A restarted process appends again.
    restarted = JsonlStreamStore(tmp_path / "streams", fsync=False)
    restarted.append(PATH, b"four")

    offsets = [m.offset for m in restarted.read(PATH)[0]]
    assert len(offsets) == len(set(offsets)), f"duplicate offsets: {offsets}"
