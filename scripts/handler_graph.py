"""Prototype: draw the handlers in one or more registries as a mermaid graph.

Nodes are the routes events arrive on, the handlers and reducers, and the
tables they write. Edges are inferred from each handler's source: a
`model_label=` (or the first argument of `Upsert`/`Update`/`Delete`/`Retire`)
is a write, and `reader.get/filter/query("app.Model", ...)` is a read. Calls
to other functions defined in the same package are followed a few levels deep,
so helpers count. A label built at runtime (an f-string, a dict lookup) cannot
be resolved and is reported instead of guessed.

Prototype only: not exported, not tested, and the inference is a heuristic.
"""

from __future__ import annotations

import ast
import contextlib
import inspect
import re
import textwrap
from collections.abc import Iterable
from dataclasses import dataclass, field
from types import FunctionType
from typing import Any

EFFECT_NAMES = {"Upsert", "Update", "Delete", "Retire"}
READ_METHODS = {"get", "filter", "query"}
MODEL_LABEL = re.compile(r"^[A-Za-z_][\w]*\.[A-Z][\w]*$")


@dataclass
class Found:
    writes: set[str] = field(default_factory=set)
    reads: set[str] = field(default_factory=set)
    unresolved: set[str] = field(default_factory=set)


def _namespace(fn: FunctionType) -> dict[str, Any]:
    names = dict(fn.__globals__)
    if fn.__closure__:
        for name, cell in zip(fn.__code__.co_freevars, fn.__closure__, strict=True):
            with contextlib.suppress(ValueError):  # an empty cell
                names[name] = cell.cell_contents
    return names


def _resolve(node: ast.AST, names: dict[str, Any]) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and isinstance(names.get(node.id), str):
        return names[node.id]
    if isinstance(node, ast.Attribute):
        base = _resolve_obj(node.value, names)
        value = getattr(base, node.attr, None)
        if isinstance(value, str):
            return value
    return None


def _resolve_obj(node: ast.AST, names: dict[str, Any]) -> Any:
    if isinstance(node, ast.Name):
        return names.get(node.id)
    if isinstance(node, ast.Attribute):
        return getattr(_resolve_obj(node.value, names), node.attr, None)
    return None


def _scan(fn: Any, package: str, found: Found, seen: set[Any], depth: int) -> None:
    fn = inspect.unwrap(fn)
    if not isinstance(fn, FunctionType) or fn in seen or depth < 0:
        return
    seen.add(fn)
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    except (OSError, TypeError, SyntaxError):
        found.unresolved.add(f"{fn.__qualname__}: source unavailable")
        return
    names = _namespace(fn)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        callee_name = (
            func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        )
        for kw in node.keywords:
            if kw.arg == "model_label":
                _record(kw.value, names, found.writes, found, fn, "write")
        if callee_name in EFFECT_NAMES and node.args:
            _record(node.args[0], names, found.writes, found, fn, "write")
        if isinstance(func, ast.Attribute) and func.attr in READ_METHODS and node.args:
            label = _resolve(node.args[0], names)
            if label and MODEL_LABEL.match(label):
                found.reads.add(label)
        # Follow helpers that live in the consumer's own package.
        callee = _resolve_obj(func, names)
        if isinstance(callee, FunctionType) and callee.__module__.startswith(package):
            _scan(callee, package, found, seen, depth - 1)


def _record(node, names, into, found, fn, kind) -> None:
    label = _resolve(node, names)
    if label:
        into.add(label)
    else:
        found.unresolved.add(f"{fn.__qualname__}: {kind} `{ast.unparse(node)}`")


def scan(fn: Any, depth: int = 3) -> Found:
    fn = inspect.unwrap(fn)
    package = (getattr(fn, "__module__", "") or "").split(".")[0]
    found = Found()
    _scan(fn, package, found, set(), depth)
    return found


def _id(*parts: str) -> str:
    return re.sub(r"\W", "_", "__".join(parts))


def _route(match: Any, match_field: str | None) -> str:
    pats = sorted(match) if isinstance(match, frozenset) else [match]
    shown = ", ".join(pats[:4]) + (f" +{len(pats) - 4}" if len(pats) > 4 else "")
    return f"{match_field}={shown}" if match_field else shown


def to_mermaid(
    registries: Iterable[tuple[str, Any]], hint: Any = None, condense: int = 0
) -> tuple[str, list[str]]:
    """Return (mermaid source, list of things that could not be resolved).

    One box per handler name in a group, however many routes feed it; a group
    may appear more than once. `hint(group, version)` may return
    ``{"writes": {...}, "reads": {...}}`` for a handler or reducer whose tables
    are chosen at runtime, replacing what the source scan could not resolve.

    With `condense` > 0, a handler with more than that many routes gets one
    route box, and tables it alone touches fold into one counted box, so a
    wide fan-out does not bury the joins between handlers.
    """
    groups: dict[str, dict[str, str]] = {}  # group -> node id -> label
    routes: dict[str, dict[str, str]] = {}  # node id -> route id -> label
    writes: dict[str, set[str]] = {}
    reads: dict[str, set[str]] = {}
    problems: list[str] = []

    def connect(node: str, group: str, version: Any) -> None:
        found = scan(version.fn)
        extra = hint(group, version) if hint else None
        if extra:
            found.writes |= set(extra.get("writes", ()))
            found.reads |= set(extra.get("reads", ()))
            found.unresolved.clear()
        writes.setdefault(node, set()).update(found.writes)
        reads.setdefault(node, set()).update(found.reads)
        problems.extend(
            f"{group}/{version.name}: {p}" for p in sorted(found.unresolved)
        )
        if not found.writes:
            problems.append(f"{group}/{version.name}: no table writes found")

    for group, reg in registries:
        nodes = groups.setdefault(group, {})
        for v in reg.all_versions():
            if v.effective_to is not None:
                continue  # only the version in force today
            hid = _id(group, v.name)
            nodes[hid] = f'["{v.name}<br/><small>stage {v.stage}</small>"]'
            rid = _id("route", group, str(v.event_match))
            routes.setdefault(hid, {})[rid] = _route(v.event_match, v.match_field)
            connect(hid, group, v)
        for r in reg.all_reducers():
            rid = _id(group, "reducer", r.name)
            nodes[rid] = (
                f'{{{{"{r.name}<br/><small>reducer, stage {r.stage}</small>"}}}}'
            )
            connect(rid, group, r)

    touchers: dict[str, set[str]] = {}
    for node in writes:
        for t in writes[node] | reads[node]:
            touchers.setdefault(t, set()).add(node)
    folded: dict[str, str] = {}  # table -> folded box id
    tables: dict[str, str] = {}  # box id -> label
    for node, ts in writes.items():
        private = sorted(t for t in ts if touchers[t] == {node})
        if condense and len(private) > condense:
            box = _id("t", node, "folded")
            tables[box] = f"{len(private)} tables only this handler touches"
            for t in private:
                folded[t] = box
    for t in touchers:
        if t not in folded:
            tables[_id("t", t)] = t

    lines = ["flowchart LR"]
    edges: list[str] = []
    for group, nodes in groups.items():
        lines.append(f'  subgraph {_id("g", group)}["{group}"]')
        for node, label in nodes.items():
            rs = routes.get(node, {})
            if condense and len(rs) > condense:
                field = rs[next(iter(rs))].split("=")[0]
                rid = _id("route", node, "many")
                lines.append(f'    {rid}(["{field}: {len(rs)} kinds"])')
                edges.append(f"  {rid} --> {node}")
            else:
                for rid, rlabel in rs.items():
                    lines.append(f'    {rid}(["{rlabel}"])')
                    edges.append(f"  {rid} --> {node}")
            lines.append(f"    {node}{label}")
        lines.append("  end")
    for box, label in sorted(tables.items()):
        lines.append(f'  {box}[("{label}")]')
    for node in writes:
        edges.extend(
            f"  {node} --> {folded.get(t, _id('t', t))}" for t in sorted(writes[node])
        )
        edges.extend(
            f"  {folded.get(t, _id('t', t))} -. reads .-> {node}"
            for t in sorted(reads[node])
        )
    lines += list(dict.fromkeys(edges))
    return "\n".join(lines), list(dict.fromkeys(problems))


if __name__ == "__main__":  # tiny self-demo on rakaia's own README shape
    from rakaia import HandlerRegistry, Upsert

    reg = HandlerRegistry()
    reg.register(
        "project",
        "TF_6_1_1",
        lambda _event: Upsert(model_label="ida.Project", lookup={}),
        0,
        match_field="form_type",
    )

    def sf12(event, refs):
        refs.get("ida.Project", suku=event["suku"])
        return Upsert(model_label="ida_forms.Sf_1_2", lookup={})

    reg.register("sf12", "SF_1_2", sf12, 0, match_field="form_type", stage=1)
    src, problems = to_mermaid([("submissions", reg)])
    print(src)
    print("\n".join(problems))
