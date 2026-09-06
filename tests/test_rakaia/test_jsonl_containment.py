"""Every file a file-backed store writes lands under its own root (#246).

The property, and the reason this file exists rather than another read-back
test: a stream called `..` used to *round-trip* perfectly while its segments sat
in the store root's parent directory, so a test that writes a name and reads it
back passed and review saw nothing. Containment is what catches it — put a
hostile name in, then look at where the bytes actually went.

Both stores are asked, because the rule they share (`_contained`) is the thing
under test, not either store's copy of it.

The last test in the file is the other half of honesty about that rule: it names
the case the rule does *not* cover — a symlink planted inside the root — as a
strict `xfail`, so the limit is read here rather than found in production.
"""

from __future__ import annotations

import contextlib
from pathlib import Path

import pytest

from rakaia.jsonl_outcomes import JsonlOutcomeStore
from rakaia.jsonl_store import JsonlStreamStore
from rakaia.outcomes import Outcome
from rakaia.types import AppendOptions

#: Names that have to stay inside. `..` and `../escape` traverse outright; `a/b`
#: and `a%2Fb` are the pair that must not collide once slashes are encoded;
#: `%2e%2e` and `%252e%252e` are the singly- and doubly-encoded spellings of
#: `..` that defeat a filter which merely looks for dots; `.` and the empty name
#: are the two that name a directory instead of a child of one.
HOSTILE = [
    "..",
    "../escape",
    "../../escape",
    "a/b",
    "a%2Fb",
    "%2e%2e",
    "%252e%252e",
    ".",
    "",
]


def _files_under(directory: Path) -> list[Path]:
    return [p for p in directory.rglob("*") if p.is_file()]


@pytest.mark.parametrize("name", HOSTILE)
def test_a_stream_writes_only_under_the_store_root(tmp_path: Path, name: str):
    """Nothing the stream store writes for `name` escapes the root.

    `sandbox` is the parent, and it is checked as well as the root: the original
    defect wrote a perfectly valid stream directory, just one level too high, so
    "the root holds the right files" is only half the assertion. Nothing may
    appear beside the root either.
    """
    sandbox = tmp_path / "sandbox"
    root = sandbox / "streams"
    store = JsonlStreamStore(root, fsync=False)

    store.create(name)
    store.append(name, b'{"n": 1}', AppendOptions())

    written = _files_under(sandbox)
    assert written, "the append wrote nothing at all"
    for path in written:
        assert root in path.parents, f"{path} escaped {root}"
        # Directly under the root, one directory deep: a stream is one
        # directory, never a tree.
        assert path.parent.parent == root, f"{path} is not in a stream directory"


#: The same names an outcome can carry. The empty one is missing because
#: `Outcome` refuses it at construction (`test_outcome_validation.py`), so it
#: never reaches a path — a stream path may still be empty, which is why the
#: stream list above keeps it.
HOSTILE_OUTCOME_NAMES = [n for n in HOSTILE if n != ""]


@pytest.mark.parametrize("name", HOSTILE_OUTCOME_NAMES)
@pytest.mark.parametrize("field", ["consumer", "stream_path"])
def test_an_outcome_writes_only_under_the_store_root(
    tmp_path: Path, field: str, name: str
):
    """The same for the outcome store, with the hostile name in either component.

    A consumer id and a stream path are names, not paths: one directory per
    consumer, one file per stream, nothing deeper and nothing above. The last
    assertion is the read-back that is *not* the point — it is here only so a fix
    cannot buy containment by losing the outcome.
    """
    sandbox = tmp_path / "sandbox"
    root = sandbox / "outcomes"
    store = JsonlOutcomeStore(root, fsync=False)
    fixed = {"consumer": "c", "stream_path": "s"}
    scope = {**fixed, field: name}

    store.record(
        Outcome(
            **scope,
            subject="row-1",
            offset="0000000001",
            sequence_key="seq",
            stage="project",
            status="failed",
            reasons=(name,),
        )
    )

    written = _files_under(sandbox)
    assert written, "the record wrote nothing at all"
    for path in written:
        assert root in path.parents, f"{path} escaped {root}"
        assert path.parent.parent == root, f"{path} is not in a consumer directory"

    assert [
        o.reasons for o in store.latest(scope["consumer"], scope["stream_path"])
    ] == [(name,)]


def test_hostile_names_do_not_collide_with_each_other(tmp_path: Path):
    """Containment must not be bought by mapping several names onto one place.

    Two streams sharing a directory would mix their logs, which is a worse bug
    than the one being fixed.
    """
    root = tmp_path / "streams"
    store = JsonlStreamStore(root, fsync=False)
    distinct = [n for n in HOSTILE if n != ""]

    for name in distinct:
        store.create(name)

    directories = {store._dir(name) for name in distinct}
    assert len(directories) == len(distinct)


def test_a_contained_name_still_says_which_stream_it_was(tmp_path: Path):
    """Encoding stays reversible, so `list_paths` reports the names it was given.

    Not the property that catches the bug — that is containment above — but the
    one a fix could quietly break while the containment test went green.
    """
    root = tmp_path / "streams"
    store = JsonlStreamStore(root, fsync=False)
    names = [n for n in HOSTILE if n != ""]

    for name in names:
        store.create(name)

    assert sorted(store.list_paths()) == sorted(names)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Known limit, named rather than discovered (#246 review): `_contained` "
        "checks containment lexically, and a lexical check cannot see a symlink. "
        "A symlink planted inside the root therefore still leads a write out of "
        "the store. `Path.resolve()` would catch it — refusing the name rather "
        "than misplacing it — at the cost of a filesystem walk on every append, "
        "while the root's contents are the deployer's rather than a caller's, so "
        "the trade was taken deliberately. Strict, so the day someone resolves "
        "instead this reports XPASS and the marker comes off."
    ),
)
def test_a_symlink_inside_the_root_does_not_lead_a_write_out_of_it(tmp_path: Path):
    """The strong reading of containment, which the lexical check does not hold.

    Distinct from every case above: the hostile input there is the *name*, which
    a caller supplies. Here the name is ordinary and the *root* has been prepared,
    which takes an actor who can already write inside the store.
    """
    root = tmp_path / "streams"
    root.mkdir()
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    (root / "link").symlink_to(outside)

    store = JsonlStreamStore(root, fsync=False)
    # Refusing the name is as good an answer as containing it, so a `ValueError`
    # counts as holding the property rather than as failing to reach the check.
    with contextlib.suppress(ValueError):
        store.create("link")
        store.append("link", b'{"n": 1}', AppendOptions())

    assert not list(outside.iterdir()), "a symlink in the root led the write outside"
