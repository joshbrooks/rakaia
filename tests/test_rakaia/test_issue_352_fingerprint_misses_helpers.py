"""GitHub #352: the drift fingerprint covers only the handler's own text."""

from __future__ import annotations

import importlib.util

from rakaia.source_hash import hash_function_source

_TEMPLATE = """\
THRESHOLD = {threshold}


def _helper(x):
    return x * {factor}


def handler(event):
    return _helper(event) if event > THRESHOLD else 0
"""


def _load(tmp_path, name, **values):
    f = tmp_path / f"{name}.py"
    f.write_text(_TEMPLATE.format(**values))
    spec = importlib.util.spec_from_file_location(name, f)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.handler


def test_changing_a_helper_or_constant_changes_the_fingerprint(tmp_path):
    before = _load(tmp_path, "before", threshold=1, factor=2)
    after = _load(tmp_path, "after", threshold=100, factor=3)

    # Behaviour differs: the handler now computes something else.
    assert before(5) != after(5)

    assert hash_function_source(before) != hash_function_source(after), (
        "handler behaviour changed via helper/constant but the fingerprint is identical"
    )
