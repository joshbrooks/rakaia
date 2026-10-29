"""Properties of the handler fingerprint (GitHub #352).

Modules are written to a temp dir and imported under a unique name each time, so
the properties also prove the fingerprint does not depend on the module's name.
"""

from __future__ import annotations

import importlib.util
import itertools
import tempfile
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rakaia.source_hash import hash_function_source

# Kept alive for the whole run: `inspect.getsource` re-reads the file later.
_DIR = tempfile.TemporaryDirectory()
_counter = itertools.count()

_HELPER_BODIES = [
    "return x + {n}",
    "return x * {n}",
    "return str(x) + {s}",
    "return [x] * ({n} % 5)",
]

_TEMPLATE = """\
LIMIT = {limit!r}


def helper(x):
    {body}


def handler(event):
    return helper(event), LIMIT
"""


def _load(source: str):
    name = f"_fp_prop_{next(_counter)}"
    path = Path(_DIR.name) / f"{name}.py"
    path.write_text(source)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.handler


def _module_source(limit, template: int, n: int, s: str) -> str:
    body = _HELPER_BODIES[template].format(n=n, s=repr(s))
    return _TEMPLATE.format(limit=limit, body=body)


limits = st.one_of(st.integers(-1000, 1000), st.text(max_size=8))
templates = st.integers(0, len(_HELPER_BODIES) - 1)
numbers = st.integers(0, 50)
strings = st.text(max_size=5)


@settings(max_examples=40, deadline=None)
@given(limits, limits, templates, numbers, strings)
def test_changing_the_constant_changes_the_fingerprint(a, b, template, n, s):
    if repr(a) == repr(b):
        return
    before = _load(_module_source(a, template, n, s))
    after = _load(_module_source(b, template, n, s))
    assert hash_function_source(before) != hash_function_source(after)


@settings(max_examples=40, deadline=None)
@given(limits, templates, templates, numbers, numbers, strings, strings)
def test_changing_the_helper_changes_the_fingerprint(limit, t1, t2, n1, n2, s1, s2):
    body1 = _HELPER_BODIES[t1].format(n=n1, s=repr(s1))
    body2 = _HELPER_BODIES[t2].format(n=n2, s=repr(s2))
    if body1 == body2:
        return
    before = _load(_module_source(limit, t1, n1, s1))
    after = _load(_module_source(limit, t2, n2, s2))
    assert hash_function_source(before) != hash_function_source(after)


@settings(max_examples=40, deadline=None)
@given(limits, templates, numbers, strings)
def test_reimporting_unchanged_code_gives_the_same_fingerprint(limit, template, n, s):
    source = _module_source(limit, template, n, s)
    assert hash_function_source(_load(source)) == hash_function_source(_load(source))


_IMPURE = {
    "mutable list": "STATE = [1, 2]\n\n\ndef handler(event):\n    return STATE\n",
    "mutable dict": "STATE = {{}}\n\n\ndef handler(event):\n    return STATE\n",
    "import inside": "def handler(event):\n    import json\n    return json\n",
    "eval": "def handler(event):\n    return eval('1')\n",
}


@settings(max_examples=10, deadline=None)
@given(st.sampled_from(sorted(_IMPURE)))
def test_mutable_globals_and_inner_imports_are_flagged_impure(kind):
    from rakaia.source_hash import fingerprint_handler

    source = _IMPURE[kind].replace("{{}}", "{}")
    result = fingerprint_handler(_load(source))
    assert result.impure, kind


@settings(max_examples=20, deadline=None)
@given(limits, templates, numbers, strings)
def test_pure_handlers_are_not_flagged(limit, template, n, s):
    from rakaia.source_hash import fingerprint_handler

    result = fingerprint_handler(_load(_module_source(limit, template, n, s)))
    assert result.impure == ()
