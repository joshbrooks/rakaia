"""Tests for the `manage.py replay` management command."""

from __future__ import annotations

import io

import pytest
from django.core.management import call_command

from django_rakaia.store import get_store
from rakaia.registry import HandlerDriftError
from rakaia.seed import seed_stream

# Importing handlers for the side effect of registering the autodiscovered
# handler that the management command tests dispatch against.
from .handlers import AUTODISCOVERED_HANDLER_NAME  # noqa: F401
from .models import Area


def _seed(stream_path: str, events: list[dict]) -> None:
    # The store is a process-wide singleton, and seeding is additive: `create()`
    # is idempotent and cannot truncate, so an existing stream needs no guard.
    seed_stream(stream_path, events, store=get_store())


@pytest.fixture(autouse=True)
def _clear_store():
    """Reset the singleton store between tests so streams don't leak."""
    get_store().clear()
    yield
    get_store().clear()


@pytest.mark.django_db
class TestReplayCommand:
    def test_basic_replay_applies_effects(self):
        _seed(
            "test_autodiscover_stream",
            [{"name": "Alpha"}, {"name": "Beta"}],
        )

        out = io.StringIO()
        call_command(
            "replay",
            "test_autodiscover_stream",
            stdout=out,
        )

        assert Area.objects.filter(name="Alpha").exists()
        assert Area.objects.filter(name="Beta").exists()
        assert "APPLIED" in out.getvalue()
        assert "events=2" in out.getvalue()
        assert "effects=2" in out.getvalue()

    def test_dry_run_does_not_apply(self):
        _seed("test_autodiscover_stream", [{"name": "Gamma"}])

        out = io.StringIO()
        call_command(
            "replay",
            "test_autodiscover_stream",
            "--dry-run",
            stdout=out,
        )

        assert not Area.objects.filter(name="Gamma").exists()
        assert "DRY RUN" in out.getvalue()
        assert "events=1" in out.getvalue()

    def test_dry_run_lists_effects(self):
        _seed("test_autodiscover_stream", [{"name": "Delta"}, {"name": "Epsilon"}])

        out = io.StringIO()
        call_command(
            "replay",
            "test_autodiscover_stream",
            "--dry-run",
            stdout=out,
        )

        text = out.getvalue()
        # Nothing applied ...
        assert not Area.objects.filter(name__in=["Delta", "Epsilon"]).exists()
        # ... but the effects that WOULD be applied are listed.
        assert "DRY RUN" in text
        assert "upsert" in text
        assert "test_django_rakaia.Area" in text

    def test_range_bounds(self):
        _seed(
            "test_autodiscover_stream",
            [{"name": f"Row{i}"} for i in range(5)],
        )

        out = io.StringIO()
        call_command(
            "replay",
            "test_autodiscover_stream",
            "--from",
            "1",
            "--to",
            "4",
            stdout=out,
        )

        # Indices 1, 2, 3 only
        assert Area.objects.filter(name="Row1").exists()
        assert Area.objects.filter(name="Row3").exists()
        assert not Area.objects.filter(name="Row0").exists()
        assert not Area.objects.filter(name="Row4").exists()
        assert "events=3" in out.getvalue()

    def test_strict_drift_raises(self):
        from rakaia.registry import get_default_registry

        _seed("test_autodiscover_stream", [{"name": "Drifted"}])

        # Tamper with the autodiscovered handler's stored hash to simulate drift
        registry = get_default_registry()
        version = next(
            v
            for v in registry.all_versions()
            if v.name == "test_autodiscovered_handler"
        )
        original_hash = version.source_hash
        object.__setattr__(version, "source_hash", "deadbeef" * 8)
        try:
            with pytest.raises(HandlerDriftError):
                call_command(
                    "replay",
                    "test_autodiscover_stream",
                    "--strict-drift",
                    stdout=io.StringIO(),
                )
        finally:
            object.__setattr__(version, "source_hash", original_hash)
