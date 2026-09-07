"""Tests for the runnable examples, which are not on the import path by default.

Each example is a self-contained Django project rather than a package under
`src/`, so importing one means saying where it lives. Doing that here rather than
in each test module keeps the imports at the top of those files, where ruff and a
reader both expect them.

Only the model-free half of an example is importable this way. An example's
models belong to its own project's settings and this suite runs under the test
settings in `tests/test_django_rakaia`, so anything touching a projection table
is covered by the example's own ``demo_*`` command, which `just demos` runs.
"""

from __future__ import annotations

import sys
from pathlib import Path

_EXAMPLES = Path(__file__).resolve().parents[2] / "examples"

for _project in ("partisipa_intake",):
    _path = str(_EXAMPLES / _project)
    if _path not in sys.path:
        sys.path.insert(0, _path)
