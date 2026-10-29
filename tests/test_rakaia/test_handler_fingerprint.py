"""Handler fingerprint: what counts as impure, the strict setting, old hashes (#352)."""

from __future__ import annotations

import dataclasses
import enum
import sys
import warnings

import pytest

from rakaia import registry as registry_module
from rakaia.drift import DriftLedger
from rakaia.registry import HandlerRegistry
from rakaia.source_hash import (
    Fingerprint,
    ImpureHandlerError,
    ImpureHandlerWarning,
    fingerprint_handler,
    set_strict_handler_fingerprints,
)
from rakaia.store import StreamStore

MUTABLE = [1]
LIMIT = 3


@dataclasses.dataclass(frozen=True)
class Made:
    n: int


class Colour(enum.Enum):
    RED = "red"


class Service:
    def run(self) -> int:
        return 1


def uses_constant(event):
    return event if event > LIMIT else 0


def uses_mutable(event):  # noqa: ARG001
    return MUTABLE


def builds_a_dataclass(event):
    return Made(event), Colour.RED


def uses_a_class(event):  # noqa: ARG001
    return Service().run()


def uses_default(event, factor=2):
    return event * factor


@pytest.fixture(autouse=True)
def _reset_strict():
    set_strict_handler_fingerprints(None)
    yield
    set_strict_handler_fingerprints(None)


def test_constants_and_dataclasses_are_pure_but_mutables_and_plain_classes_are_not():
    assert fingerprint_handler(uses_constant).impure == ()
    assert fingerprint_handler(builds_a_dataclass).impure == ()
    assert fingerprint_handler(uses_default).impure == ()
    assert any("MUTABLE" in line for line in fingerprint_handler(uses_mutable).impure)
    assert any("Service" in line for line in fingerprint_handler(uses_a_class).impure)


def test_a_dataclass_the_handler_builds_is_part_of_the_fingerprint():
    # Same handler text, different dataclass body: the digest must differ.
    import importlib.util
    import tempfile
    from pathlib import Path

    tmp = tempfile.mkdtemp()
    digests = []
    for i, field in enumerate(("n: int", "n: str")):
        path = Path(tmp) / f"dc{i}.py"
        path.write_text(
            "from dataclasses import dataclass\n\n\n@dataclass\nclass Made:\n"
            f"    {field}\n\n\ndef handler(event):\n    return Made(event)\n"
        )
        spec = importlib.util.spec_from_file_location(f"dc{i}", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"dc{i}"] = module  # inspect finds a class's source here
        spec.loader.exec_module(module)
        digests.append(fingerprint_handler(module.handler).digest)
    assert digests[0] != digests[1]


def test_registering_an_impure_handler_warns_naming_it():
    reg = HandlerRegistry()
    with pytest.warns(ImpureHandlerWarning, match="MUTABLE") as seen:
        reg.register("h", "s", uses_mutable, 0)
    assert "uses_mutable" in str(seen[0].message)
    assert "future release" in str(seen[0].message)


def test_a_pure_handler_does_not_warn():
    reg = HandlerRegistry()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        reg.register("h", "s", uses_constant, 0)


@pytest.mark.parametrize("how", ["flag", "env"])
def test_strict_makes_an_impure_handler_an_error(how, monkeypatch):
    if how == "flag":
        set_strict_handler_fingerprints(True)
    else:
        monkeypatch.setenv("RAKAIA_STRICT_HANDLER_FINGERPRINTS", "1")
    with pytest.raises(ImpureHandlerError, match="MUTABLE"):
        HandlerRegistry().register("h", "s", uses_mutable, 0)


def test_the_django_setting_reaches_core(settings):
    from django.apps import apps

    settings.RAKAIA_STRICT_HANDLER_FINGERPRINTS = True
    apps.get_app_config("django_rakaia").ready()
    with pytest.raises(ImpureHandlerError):
        HandlerRegistry().register("h", "s", uses_mutable, 0)


def test_an_old_format_stored_hash_is_not_drift():
    ledger = DriftLedger()
    assert (
        ledger.check(kind="handler", name="h", stored_hash="a" * 64, fn=uses_constant)
        is False
    )
    assert ledger.warnings == []


def test_an_old_format_record_is_re_recorded_not_used_as_the_reference(monkeypatch):
    store = StreamStore()
    with monkeypatch.context() as m:
        m.setattr(
            registry_module,
            "fingerprint_handler",
            lambda _fn: Fingerprint("a" * 64),
        )
        HandlerRegistry(store=store).register("h", "s", uses_constant, 0)

    version = HandlerRegistry(store=store).register("h", "s", uses_constant, 0)
    assert version.reference_hash is None
    ledger = DriftLedger()
    assert not ledger.check(
        kind="handler", name="h", stored_hash=version.hash_to_check, fn=uses_constant
    )
    # The new-format record is now the reference for later edits.
    again = HandlerRegistry(store=store).register("h", "s", uses_constant, 0)
    assert again.hash_to_check == version.source_hash
