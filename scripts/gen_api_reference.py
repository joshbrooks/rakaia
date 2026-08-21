"""Generate `docs/api-reference.md` from what the packages actually export.

A hand-written list of 130-odd names goes stale the first time someone adds one.
This reads `rakaia.__all__` and `django_rakaia.__all__` at runtime, so the
reference cannot drift from the code — the worst it can do is be regenerated.

Run it with `just api-reference` (or `uv run python scripts/gen_api_reference.py`)
and commit the result. CI checks the committed file is up to date.

Grouping is the one editorial choice here: `GROUPS` below assigns names to
sections by hand, because the module a name happens to live in is an
implementation detail the public API deliberately does not promise. Anything
unassigned lands in "Everything else", which is the signal to come and group it.
"""

from __future__ import annotations

import inspect
import os
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.test_django_rakaia.settings")
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import django

django.setup()

import django_rakaia  # noqa: E402
import rakaia  # noqa: E402

GROUPS: dict[str, tuple[str, ...]] = {
    "Running a server": (
        "app",
        "create_app",
        "get_asgi_app",
        "ServerOptions",
        "get_store",
    ),
    "Reading and writing streams": (
        "Stream",
        "StreamStore",
        "DjangoStreamStore",
        "ReadableStore",
        "WritableStore",
        "StreamServerStore",
        "append_event",
        "append_if_changed",
        "seed_stream",
        "create_stream_event",
        "AppendOptions",
        "AppendResult",
        "CloseResult",
        "ClosedBy",
        "StreamMessage",
        "Poll",
        "PollStatus",
        "poll",
        "poll_consumer",
    ),
    "Rebuilding tables (replay)": (
        "replay",
        "replay_stream",
        "merge_replay",
        "ReplayResult",
        "DriftLedger",
        "fold_events",
        "project_latest",
    ),
    "Describing changes (effects)": (
        "Effect",
        "AnyEffect",
        "Upsert",
        "Update",
        "Delete",
        "Retire",
        "Exclude",
        "RowEffect",
        "ExternalEffect",
        "Ref",
        "RefResolver",
        "UnresolvedRefError",
        "Transition",
        "TouchedSubject",
    ),
    "Registering rules (handlers, reducers, upcasters)": (
        "register_handler",
        "register_reducer",
        "register_simple",
        "register_upcaster",
        "upcast",
        "HandlerRegistry",
        "UpcasterRegistry",
        "HandlerVersion",
        "ReducerVersion",
        "UpcasterVersion",
        "get_default_registry",
        "get_default_upcaster_registry",
        "reset_default_registries",
        "check_disjoint_defaults",
    ),
    "Applying changes (executors and readers)": (
        "Executor",
        "CollectingExecutor",
        "DjangoExecutor",
        "InMemoryProjections",
        "ProjectionReader",
        "DjangoProjectionReader",
        "PreloadedProjectionReader",
        "ModelStreamReader",
    ),
    "Rehearsing a rebuild safely": (
        "rebuild_and_verify",
        "GuardNotArmed",
        "ScratchAliasNotEmpty",
        "deny_database_access",
        "assert_no_live_writes",
        "AmbientDatabaseAccess",
        "LiveWriteLeaked",
        "diff_effects_against_rows",
        "DiffReport",
        "RowDiff",
        "FieldDiff",
        "snapshots_equal",
        "GREEN",
        "RED",
        "VACUOUS",
        "VacuousVerification",
        "VerificationError",
        "PreloadMismatch",
    ),
    "Audit trails and provenance": (
        "provenance",
        "get_provenance",
        "ProvenanceMiddleware",
        "envelope_actor",
        "label_marker",
        "materialize_history",
        "history_effects",
        "ENVELOPE_TS",
    ),
    "Keeping child rows in step": (
        "reconcile_children",
        "reconcile_tree",
        "reconcile_aggregate",
        "reconcile_by_key",
    ),
    "Consumer cursors": (
        "CursorStore",
        "CursorOptions",
        "calculate_cursor",
        "generate_response_cursor",
        "commit_cursor",
        "load_cursor",
    ),
    "Producer fencing": (
        "ProducerState",
        "ProducerAccepted",
        "ProducerDuplicate",
        "ProducerStaleEpoch",
        "ProducerSequenceGap",
        "ProducerInvalidEpochSeq",
        "ProducerStreamClosed",
        "ProducerValidationResult",
    ),
    "Django model integration": (
        "stream_model",
        "register_stream_event_admin",
        "canonical_value",
        "DEFAULT_NORMALIZERS",
        "Normalizer",
    ),
    "Constants": (
        "__version__",
        "HANDLERS_META_STREAM",
        "REDUCERS_META_STREAM",
        "UPCASTERS_META_STREAM",
        "SCRATCH_PATH",
    ),
    "Errors": (
        "StreamError",
        "StreamNotFound",
        "SequenceConflict",
        "ContentTypeMismatch",
        "InvalidJson",
        "InvalidOffset",
        "ForeignOffset",
        "EmptyJsonArray",
        "SpareKeys",
        "StreamConfigConflict",
        "HandlerDriftError",
        "HandlerGapError",
        "HandlerOverlapError",
        "EffectCollisionError",
        "DuplicateProducesError",
        "UpcasterChainError",
        "UpcasterConflictError",
    ),
}

INTRO = """---
icon: lucide/list
---

# Python API reference

Every name the two packages export, with its signature. This page is generated
from `rakaia.__all__` and `django_rakaia.__all__`, so it lists exactly what is
importable and nothing else.

For *what these names promise* — which are stable, which may change, and how to
pin a version — see [the public API](public-api.md). This page is the index; that
page is the contract.

!!! note "Generated file"

    Do not edit by hand. Run `just api-reference` and commit the result.

"""


def signature_of(obj: object) -> str:
    if not (inspect.isfunction(obj) or inspect.isclass(obj) or inspect.ismethod(obj)):
        return ""  # constants have no signature; str/tuple instances would report their type's
    try:
        sig = str(inspect.signature(obj))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return ""
    # Default values that repr as `<function f at 0x7f…>` embed a memory address,
    # which changes every run and would churn the committed diff for no reason.
    return re.sub(r" at 0x[0-9a-f]+", "", sig)


def summary_of(obj: object) -> str:
    doc = inspect.getdoc(obj)
    if not doc:
        return ""
    # `inspect.getdoc` falls back to the *type's* docstring for instances, so a
    # module-level `FOO: str = "…"` would otherwise be documented as `str(...)`.
    is_callable = (
        inspect.isfunction(obj) or inspect.isclass(obj) or inspect.ismethod(obj)
    )
    if not is_callable and doc == inspect.getdoc(type(obj)):
        return ""
    first = doc.strip().split("\n\n")[0].replace("\n", " ").strip()
    return first if len(first) < 200 else first[:197].rsplit(" ", 1)[0] + "…"


def main() -> int:
    home: dict[str, list[str]] = {}
    for pkg_name, pkg in (("rakaia", rakaia), ("django_rakaia", django_rakaia)):
        for name in pkg.__all__:
            home.setdefault(name, []).append(pkg_name)

    grouped = {n for names in GROUPS.values() for n in names}
    ungrouped = sorted(n for n in home if n not in grouped)

    out = [INTRO]
    sections = list(GROUPS.items())
    if ungrouped:
        sections.append(("Everything else", tuple(ungrouped)))

    for title, names in sections:
        present = [n for n in names if n in home]
        if not present:
            continue
        out.append(f"## {title}\n")
        out.append("| Name | Import from | Signature | What it does |")
        out.append("|---|---|---|---|")
        for name in present:
            pkgs = home[name]
            obj = getattr(rakaia if "rakaia" in pkgs else django_rakaia, name)
            imports = " / ".join(f"`{p}`" for p in pkgs)
            sig = signature_of(obj).replace("|", "\\|")
            sig = f"`{sig}`" if sig else "—"
            summary = summary_of(obj).replace("|", "\\|") or "—"
            out.append(f"| `{name}` | {imports} | {sig} | {summary} |")
        out.append("")

    missing = sorted(
        n
        for n in home
        if not summary_of(getattr(rakaia if "rakaia" in home[n] else django_rakaia, n))
    )
    out.append("---\n")
    out.append("## Appendix — coverage\n")
    out.append(
        f"{len(home)} exported names across {len(sections)} sections. "
        f"{len(home) - len(missing)} carry a docstring; {len(missing)} do not "
        "and show `—` above.\n"
    )
    if missing:
        out.append("Undocumented: " + ", ".join(f"`{n}`" for n in missing) + ".\n")

    target = (
        pathlib.Path(__file__).resolve().parent.parent / "docs" / "api-reference.md"
    )
    target.write_text("\n".join(out))
    print(f"wrote {target} — {len(home)} names, {len(ungrouped)} ungrouped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
