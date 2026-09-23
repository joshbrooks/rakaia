#!/usr/bin/env python3
"""`formkit_emission` — formkit-ninja decomposes a document, rakaia keeps it.

Every other `partisipa_*` example in this directory invents its own event shape.
This one does not: it drives the real seam that
[`formkit-ninja`](https://github.com/catalpainternational/formkit-ninja) publishes
for exactly this purpose, and appends the result to a rakaia log.

  * `emit_submission` / `emit_reorder` — the walk that turns one submission into
    one emission per row. Pure: it reads `fields`, `form_type` and `pk`, and
    queries nothing.
  * `ContentEvent` — the TypedDict declaring what the decomposition alone can say
    about a row. A consumer extends it by subclassing and never the other way
    round, so the forms library learns none of the consumer's vocabulary.
  * `slug` — the one shared naming rule. Stream *paths* are advisory: the
    application writing to the log owns its stream names, and this example uses a
    consumer's own spelling (`submissions/<form>`, Partisipa's) rather than the
    library's default (`submission/<form>`) to prove that it may.

The property this exists to hold is the one a log cannot recover from getting
wrong. `ContentEvent` distinguishes **an absent key from a key set to `None`**:
`parent_submission=None` says "this row has no parent", while no
`parent_submission` key says only that nobody said. A `TypedDict` cannot express
"present iff supplied", so nothing type-checks it — it has to survive the round
trip through the store, and section [4] is where that is checked rather than
assumed.

Runs as a plain script. No database, no Django settings, no migrations:

    just formkit-emission-demo
    # or: uv run --with 'formkit-ninja>=6.1,<7' python examples/formkit_emission/demo.py

formkit-ninja arrives through `uv run --with` rather than a project extra,
because it pins `Django==4.*` and this project tests against Django 6 — uv
resolves extras together, so an extra would take the whole environment down with
it. Without formkit-ninja the demo fails and names the command; it does not
skip. A gate that quietly passes when it cannot measure is worse than no gate,
and this one exists to break when formkit-ninja changes the shape it publishes.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from rakaia import AppendOptions, StreamStore

try:
    from formkit_ninja.form_submission.emit import (
        Emission,
        emit_reorder,
        emit_submission,
        slug,
    )
    from formkit_ninja.form_submission.wire import (
        CONTENT_EVENT_KEYS,
        REQUIRED_CONTENT_EVENT_KEYS,
        ContentEvent,
    )
except (
    ModuleNotFoundError
) as exc:  # pragma: no cover - exercised by formkit-ninja being absent
    raise SystemExit(
        f"formkit-ninja is not installed ({exc.name}); this demo needs it.\n"
        "  just formkit-emission-demo"
    ) from exc


# A key nobody supplied. `None` is a value a producer may legitimately send, so
# it cannot double as "absent" — the whole distinction this example protects
# would collapse into whichever the last writer happened to mean.
_UNSET: Any = object()


class _Unstated:
    """What a reader gets back for a key nobody supplied.

    `None` already means something — "stated, and there is none" — so a reader
    that folds an absent key into `None` destroys the distinction the log kept.
    A sentinel of its own is the only way to carry all three answers out of a
    read: stated-and-a-value, stated-and-none, and not stated.
    """

    def __repr__(self) -> str:
        return "UNSTATED"


UNSTATED = _Unstated()


class PartisipaContentEvent(ContentEvent, total=False):
    """A consumer's own keys, added the way `ContentEvent` says to add them.

    `project_id` is Partisipa vocabulary and has no business in the forms
    library. `rank` is not: it is the decomposition's own — an `Emission` carries
    it, and an `emit_reorder` emission is *entirely* about it — but `ContentEvent`
    declares no key for it, so a consumer has to mint one here. Section [7] says
    why that is worth reporting upstream rather than quietly living with.
    """

    project_id: int | None
    rank: str | None


@dataclass
class StandInSubmission:
    """What `emit_submission` actually requires of a submission.

    It reads `pk`, `form_type` and `fields` and queries nothing, so the real
    Django model is not needed to exercise the real decomposition. That the walk
    is satisfied by three attributes is the property under test, not a shortcut:
    a producer that consulted the database could not be replayed, because on the
    second run the rows it consulted already say something different.
    """

    pk: str
    form_type: str
    fields: dict[str, Any]


ROOT_UUID = "11111111-1111-1111-1111-111111111111"


def _submission() -> StandInSubmission:
    """One TF 6.1.1 progress form with a two-row repeater."""
    return StandInSubmission(
        pk=ROOT_UUID,
        form_type="TF_6_1_1",
        fields={
            "project_code": "WS-014",
            "suku": "Fatuberliu",
            "repeaterProjectProgress": [
                {"uuid": "aaaa0001", "$rank": "a0", "activity": "Intake", "pct": 100},
                {"uuid": "aaaa0002", "$rank": "a1", "activity": "Pipeline", "pct": 60},
            ],
        },
    )


def _hdr(title: str) -> None:
    print(f"\n{title}")
    print("-" * 74)


# ---------------------------------------------------------------------------
# The consumer's two decisions: what a stream is called, and what an event says.
# ---------------------------------------------------------------------------


def consumer_stream_path(emission: Emission) -> str:
    """This application's stream names, built with the library's spelling rule.

    Partisipa writes `submissions/<form>`, not the library's advisory
    `submission/<form>`. The prefix is ours to choose; `slug` is not, because two
    copies of a naming rule disagree exactly once and then silently write to two
    different streams.
    """
    root, _, repeater = emission.stream_path.partition("/")
    assert root in {"submission", "repeater_reorder"}, root
    prefix = "submissions" if root == "submission" else "reorders"
    return f"{prefix}/{repeater}"


def to_content_event(
    emission: Emission,
    *,
    project_id: Any = _UNSET,
    schema_version: Any = _UNSET,
) -> PartisipaContentEvent:
    """One emission as the event this consumer appends.

    Every optional key defaults to `_UNSET` rather than `None`, so a key reaches
    the event if and only if a caller said something about it. The key is never
    written and then removed: there is no step at which an unsupplied key exists
    in the dict, so there is no step at which it could be mistaken for a supplied
    null, and no tidying pass that could drop a real one.
    """
    event: PartisipaContentEvent = {
        "submission": emission.row_id,
        "form_type": emission.form_type,
        "fields": dict(emission.fields),
    }
    if not emission.is_root:
        # A repeater row always names its parent. A root does not carry the key
        # at all — this consumer has nothing to say about a root's parent, which
        # is different from saying it has none.
        event["parent_submission"] = emission.parent_id
    if emission.rank is not None:
        event["rank"] = emission.rank
    if project_id is not _UNSET:
        event["project_id"] = project_id
    if schema_version is not _UNSET:
        event["schema_version"] = schema_version
    return event


# ---------------------------------------------------------------------------
# [1] Decomposition
# ---------------------------------------------------------------------------


def section_decompose() -> list[Emission]:
    _hdr("[1] emit_submission: one document -> one emission per row")
    emissions = emit_submission(_submission())

    root, *children = emissions
    assert root.is_root and root.parent_id is None
    assert root.row_id == ROOT_UUID, "the root is keyed by the document's own pk"
    assert [c.row_id for c in children] == ["aaaa0002", "aaaa0001"]
    assert all(c.parent_id == ROOT_UUID for c in children)
    # The identities come out of the document, which is what lets a replay
    # reproduce the same primary keys years later.
    assert all(c.row_id in json.dumps(_submission().fields) for c in children)

    for e in emissions:
        kind = "root" if e.is_root else f"repeater[{e.repeater_key}]"
        print(
            f"    {kind:32} row={e.row_id:36} rank={e.rank or '-':4} {dict(e.fields)}"
        )
    print(
        f"    root first, then children -> {len(emissions)} emissions, parent before child ✓"
    )

    # Position and identity travel outside the answers, so moving a row is not a
    # content change. `$rank` is the reserved spelling: FormKit reserves `$` for
    # schema expressions, so no form input can collide with it.
    assert all("$rank" not in e.fields and "uuid" not in e.fields for e in emissions)
    print(
        "    `$rank` and `uuid` are outside `fields` — a re-order is not a content change ✓"
    )
    return emissions


# ---------------------------------------------------------------------------
# [2] The declared shape
# ---------------------------------------------------------------------------


def section_wire(emissions: list[Emission]) -> list[PartisipaContentEvent]:
    _hdr("[2] ContentEvent: the shape formkit-ninja vouches for, plus ours")
    events = [
        to_content_event(e, project_id=24336 if e.is_root else _UNSET)
        for e in emissions
    ]

    ours = {"project_id", "rank"}
    for event in events:
        keys = set(event)
        assert keys >= REQUIRED_CONTENT_EVENT_KEYS, (
            f"missing required: {REQUIRED_CONTENT_EVENT_KEYS - keys}"
        )
        # Nothing outside the library's declared set except keys we minted.
        assert keys <= (CONTENT_EVENT_KEYS | ours), (
            f"undeclared: {keys - CONTENT_EVENT_KEYS - ours}"
        )

    print(f"    library declares : {sorted(CONTENT_EVENT_KEYS)}")
    print(f"    required         : {sorted(REQUIRED_CONTENT_EVENT_KEYS)}")
    print(f"    this consumer adds: {sorted(ours)}")
    print(f"    {len(events)} events, every one within the declared set ✓")

    root = events[0]
    assert "parent_submission" not in root, "a root says nothing about its parent"
    assert all("parent_submission" in e for e in events[1:])
    print("    root carries no `parent_submission` key; every repeater row does ✓")
    return events


# ---------------------------------------------------------------------------
# [3] Append
# ---------------------------------------------------------------------------


def section_append(
    events: list[PartisipaContentEvent], emissions: list[Emission]
) -> StreamStore:
    _hdr("[3] Append to the log, under this consumer's own stream names")
    store = StreamStore()

    for event, emission in zip(events, emissions, strict=True):
        path = consumer_stream_path(emission)
        if not store.has(path):
            store.create(path, content_type="application/json")
        store.append(
            path,
            json.dumps(event).encode(),
            AppendOptions(label="submission.saved"),
        )

    paths = sorted({consumer_stream_path(e) for e in emissions})
    for path in paths:
        messages, up_to_date = store.read(path)
        print(f"    {path:48} {len(messages)} events  up_to_date={up_to_date}")

    assert paths == ["submissions/tf611", "submissions/tf611/repeaterprojectprogress"]
    # The library would have said `submission/tf611`. The prefix is ours; the
    # `tf611` spelling is the library's, and that is the part that must not drift.
    assert slug("TF_6_1_1") == "tf611"
    assert all(slug("TF_6_1_1") in p for p in paths)
    print("    stream names are ours, the form's spelling is `slug`'s ✓")
    return store


# ---------------------------------------------------------------------------
# [4] The property a log cannot recover from getting wrong
# ---------------------------------------------------------------------------


def section_absent_versus_null(store: StreamStore) -> None:
    _hdr("[4] An absent key and an explicit null survive the store as different events")

    # Three events about the same shape of thing, saying three different things
    # about a parent: one says there is none, one says nothing at all, one names
    # it. If the store collapses the first two, the log has lost what it recorded.
    path = "submissions/contrived"
    store.create(path, content_type="application/json")
    said_none: dict[str, Any] = {
        "submission": "row-1",
        "form_type": "TF_6_1_1",
        "fields": {},
        "parent_submission": None,
    }
    said_nothing: dict[str, Any] = {
        "submission": "row-2",
        "form_type": "TF_6_1_1",
        "fields": {},
    }
    named_one: dict[str, Any] = {
        "submission": "row-3",
        "form_type": "TF_6_1_1",
        "fields": {},
        "parent_submission": ROOT_UUID,
    }
    for event in (said_none, said_nothing, named_one):
        store.append(path, json.dumps(event).encode(), AppendOptions())

    messages, _ = store.read(path)
    read_back = [json.loads(m.data) for m in messages]

    assert read_back == [said_none, said_nothing, named_one], (
        "the store changed what was written"
    )
    assert (
        "parent_submission" in read_back[0]
        and read_back[0]["parent_submission"] is None
    )
    assert "parent_submission" not in read_back[1]
    assert read_back[2]["parent_submission"] == ROOT_UUID

    for label, event in (("said none", read_back[0]), ("said nothing", read_back[1])):
        print(
            f"    {label:13} -> key present: {'parent_submission' in event:<5} "
            f"  .get() returns: {event.get('parent_submission')!r}"
        )
    # This is the trap the distinction exists to name: `.get` answers `None` for
    # both, so a consumer reading with `.get` cannot tell them apart and the
    # difference the log faithfully kept is lost at the last step.
    assert (
        read_back[0].get("parent_submission")
        == read_back[1].get("parent_submission")
        is None
    )
    print("    the store keeps them apart; `.get()` cannot — read with `in` ✓")


# ---------------------------------------------------------------------------
# [5] schema_version
# ---------------------------------------------------------------------------


def section_schema_version(store: StreamStore) -> None:
    _hdr("[5] schema_version: absent means 'recorded before versions existed'")
    path = "submissions/versioned"
    store.create(path, content_type="application/json")

    before = to_content_event(emit_submission(_submission())[0])
    after = to_content_event(emit_submission(_submission())[0], schema_version=3)
    for event in (before, after):
        store.append(path, json.dumps(event).encode(), AppendOptions())

    messages, _ = store.read(path)
    old, new = (json.loads(m.data) for m in messages)

    assert "schema_version" not in old, (
        "nothing may fabricate a version for an old event"
    )
    assert new["schema_version"] == 3
    print("    pre-versions event : no `schema_version` key — not a zero, not a null ✓")
    print("    later event        : schema_version=3 ✓")
    # Reading it is the consumer's decision, and it has to be made where the
    # absence is still visible. Defaulting it at read time is the same mistake as
    # fabricating it at write time, one step later.
    reading = {
        True: "answered against version",
        False: "recorded before versions existed",
    }
    for event in (old, new):
        print(f"    {reading['schema_version' in event]:<38} {event['submission'][:8]}")


# ---------------------------------------------------------------------------
# [6] Fold the log back into rows
# ---------------------------------------------------------------------------


def section_fold(store: StreamStore) -> None:
    _hdr("[6] Read the log back and rebuild the rows — the seam, end to end")

    rows: dict[str, dict[str, Any]] = {}
    for path in ("submissions/tf611", "submissions/tf611/repeaterprojectprogress"):
        messages, _ = store.read(path)
        for message in messages:
            event: Mapping[str, Any] = json.loads(message.data)
            rows[event["submission"]] = {
                "form_type": event["form_type"],
                # `.get` is safe here, and only because the default is a
                # sentinel rather than `None`. A bare `.get("parent_submission")`
                # answers `None` both to a row stated to have no parent and to a
                # row nobody said anything about, folding two different records
                # into one — the loss section [4] measured. What decides it is
                # the default, not the method.
                "parent": event.get("parent_submission", UNSTATED),
                "rank": event.get("rank"),
                **event["fields"],
            }

    assert len(rows) == 3
    root = rows[ROOT_UUID]
    assert root["form_type"] == "TF_6_1_1" and root["project_code"] == "WS-014"
    assert root["parent"] is UNSTATED, (
        "a root's parent was never stated, and is not null"
    )
    children = {k: v for k, v in rows.items() if v["parent"] == ROOT_UUID}
    assert len(children) == 2
    # The repeater rows come back in the order their ranks put them, not the
    # order they were appended in — `aaaa0002` was emitted first.
    assert sorted(children, key=lambda k: children[k]["rank"]) == [
        "aaaa0001",
        "aaaa0002",
    ]

    def _parent(value: Any) -> str:
        if value is UNSTATED:
            return "unstated"
        return "none" if value is None else str(value)[:8]

    print(f"    {'row':10} {'rank':6} {'parent':10} answers")
    for row_id in sorted(rows, key=lambda k: (rows[k]["rank"] or "", k)):
        row = rows[row_id]
        answers = {
            k: v for k, v in row.items() if k not in {"form_type", "parent", "rank"}
        }
        print(
            f"    {row_id[:8]:10} {row['rank'] or '-':6} {_parent(row['parent']):10} {answers}"
        )
    print("    3 rows rebuilt from the log alone, ordered by rank ✓")
    print("    the root's parent reads `unstated`, which is not `none` ✓")


# ---------------------------------------------------------------------------
# [7] Re-order, and the one key that has nowhere to go
# ---------------------------------------------------------------------------


def section_reorder() -> None:
    _hdr("[7] emit_reorder: one event per moved row — and the gap it exposes")
    sub = _submission()

    unchanged = emit_reorder(sub, prior={"aaaa0001": "a0", "aaaa0002": "a1"})
    assert unchanged == [], "a group that did not move must cost nothing"
    print("    nothing moved              -> 0 events ✓")

    moved = emit_reorder(sub, prior={"aaaa0001": "a0", "aaaa0002": "a9"})
    assert len(moved) == 1 and moved[0].row_id == "aaaa0002"
    print(
        f"    one row of two moved       -> {len(moved)} event  (row {moved[0].row_id}, rank {moved[0].rank}) ✓"
    )

    # The finding this example was built to surface, asserted so it goes red if
    # the library closes it: a re-order emission's entire content is its rank,
    # and `ContentEvent` declares no key for a rank. `ordinality` is the row's
    # index within the split, not its fractional position, so it is not the same
    # key wearing another name.
    assert "rank" not in CONTENT_EVENT_KEYS
    assert "ordinality" in CONTENT_EVENT_KEYS
    event = to_content_event(moved[0])
    assert event["rank"] == "a1" and "rank" not in CONTENT_EVENT_KEYS
    print()
    print("    REPORTED UPSTREAM: `rank` is the decomposition's own vocabulary, but")
    print("    `ContentEvent` declares no key for it, so every consumer mints one")
    print("    privately — which is the divergence the shared TypedDict exists to")
    print("    prevent. `repeater_key` is in the same position.")


def main() -> None:
    print("=" * 74)
    print("  formkit-ninja decomposes a submission; rakaia keeps what it said")
    print("=" * 74)
    emissions = section_decompose()
    events = section_wire(emissions)
    store = section_append(events, emissions)
    section_absent_versus_null(store)
    section_schema_version(store)
    section_fold(store)
    section_reorder()
    print("\nAll formkit-ninja/rakaia seam checks passed ✓")


if __name__ == "__main__":
    main()
