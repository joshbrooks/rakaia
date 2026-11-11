"""The contract suites a store, executor or reader is held to.

Subclass a suite in your own tests and provide the fixture it names; pytest then
runs every check against your implementation, the same ones the built-in stores
and executors pass. The suites are `store_contract` (`WritableStore`),
`server_store_contract` (`StreamServerStore`), `executor_contract` (`Executor`),
`outcome_store_contract` (`OutcomeStore`) and `projection_reader_contract`
(`ProjectionReader`).

Importing this package needs nothing. Importing a suite needs pytest (all but
`outcome_store_contract`), and says so if it is missing, so installing rakaia never
installs pytest.
"""

from __future__ import annotations

_SUITES = (
    "store_contract",
    "server_store_contract",
    "executor_contract",
    "outcome_store_contract",
    "projection_reader_contract",
)

try:
    import pytest
except ImportError:
    # No pytest, so nothing here can run; importing a suite will say so.
    pass
else:
    # pytest only explains a failed `assert` in test files, conftests and
    # plugins. The suites are none of those, so without this a failure would
    # read `AssertionError` with no values.
    pytest.register_assert_rewrite(*(f"{__name__}.{name}" for name in _SUITES))
