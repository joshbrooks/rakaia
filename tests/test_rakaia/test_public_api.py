"""The public surface is pinned, so changing it is a decision rather than a diff.

`docs/public-api.md` promises that Tier 1 — the names in `rakaia.__all__` and
`django_rakaia.__all__` — does not change without a major bump and an
`UPGRADING.md` entry. A promise nothing checks is a promise that gets broken by
an unrelated refactor, so the full list is written out below.

**When this test fails, that is the test working.** Adding a name is fine: add it
here too. Removing or renaming one is a breaking change — do it deliberately,
with the changelog and upgrade note that go with it.
"""

from __future__ import annotations

import pytest

import rakaia

RAKAIA_PUBLIC = {
    # app factory
    "app",
    "create_app",
    # store
    "StreamStore",
    "seed_stream",
    # extension protocols
    "CursorStore",
    "ProjectionReader",
    "ReadableStore",
    "StreamServerStore",
    "WritableStore",
    # envelope / provenance
    "append_if_changed",
    "envelope_actor",
    "get_provenance",
    "history_effects",
    "label_marker",
    "provenance",
    "snapshots_equal",
    # options
    "CursorOptions",
    "ServerOptions",
    # types
    "AppendOptions",
    "AppendResult",
    "ClosedBy",
    "CloseResult",
    "ProducerState",
    "Stream",
    "StreamMessage",
    # producer validation
    "ProducerAccepted",
    "ProducerDuplicate",
    "ProducerInvalidEpochSeq",
    "ProducerSequenceGap",
    "ProducerStaleEpoch",
    "ProducerStreamClosed",
    "ProducerValidationResult",
    # named store failures
    "ContentTypeMismatch",
    "EmptyJsonArray",
    "InvalidJson",
    "InvalidOffset",
    "SequenceConflict",
    "StreamConfigConflict",
    "StreamError",
    "StreamNotFound",
    # cursors
    "calculate_cursor",
    "generate_response_cursor",
    # effects
    "CollectingExecutor",
    "InMemoryProjections",
    "DuplicateProducesError",
    "Effect",
    "AnyEffect",
    "RowEffect",
    "Upsert",
    "Update",
    "Delete",
    "Retire",
    "ExternalEffect",
    "Exclude",
    "SpareKeys",
    "Transition",
    "EffectCollisionError",
    "Executor",
    "Ref",
    "RefResolver",
    "UnresolvedRefError",
    "check_disjoint_defaults",
    # projections
    "project_latest",
    "reconcile_aggregate",
    "reconcile_by_key",
    "reconcile_children",
    "reconcile_tree",
    # registry
    "HANDLERS_META_STREAM",
    "REDUCERS_META_STREAM",
    "UPCASTERS_META_STREAM",
    "HandlerDriftError",
    "HandlerGapError",
    "HandlerOverlapError",
    "HandlerRegistry",
    "HandlerVersion",
    "ReducerVersion",
    "UpcasterChainError",
    "UpcasterConflictError",
    "UpcasterRegistry",
    "UpcasterVersion",
    "get_default_registry",
    "get_default_upcaster_registry",
    "register_handler",
    "register_reducer",
    "register_simple",
    "register_upcaster",
    "reset_default_registries",
    "upcast",
    # replay
    "ENVELOPE_TS",
    "ReplayResult",
    "TouchedSubject",
    "merge_replay",
    "replay",
    # subscriptions
    "Poll",
    "PollStatus",
    "poll",
    # version
    "__version__",
}

DJANGO_RAKAIA_PUBLIC = {
    "AmbientDatabaseAccess",
    "DEFAULT_NORMALIZERS",
    "DiffReport",
    "DjangoExecutor",
    "DjangoProjectionReader",
    "DjangoStreamStore",
    "FieldDiff",
    "GREEN",
    "LiveWriteLeaked",
    "ModelStreamReader",
    # Added when `DjangoExecutor` gained `normalizers=` (#160): both public entry
    # points taking a normalizer sequence needed the element type to be nameable
    # without importing a submodule.
    "Normalizer",
    "PreloadedProjectionReader",
    # Added with `diff_effects_against_rows(preload=True)` (#190): the diff
    # refuses a hand-built preloaded reader that covers a different batch, and a
    # consumer catching that needs to name the error without a submodule import.
    "PreloadMismatch",
    "GuardNotArmed",
    "ProvenanceMiddleware",
    "RED",
    "RowDiff",
    "SCRATCH_PATH",
    "ScratchAliasNotEmpty",
    "VACUOUS",
    "VacuousVerification",
    "VerificationError",
    "append_event",
    "assert_no_live_writes",
    "canonical_value",
    "commit_cursor",
    "create_stream_event",
    "deny_database_access",
    "diff_effects_against_rows",
    "fold_events",
    "get_asgi_app",
    "get_store",
    "load_cursor",
    "materialize_history",
    "poll_consumer",
    "rebuild_and_verify",
    "register_stream_event_admin",
    "replay_stream",
    "stream_model",
}


class TestRakaiaSurface:
    def test_all_is_exactly_the_pinned_set(self):
        assert set(rakaia.__all__) == RAKAIA_PUBLIC

    def test_every_exported_name_resolves(self):
        """`__all__` listing a name that isn't there breaks `import *` and every
        documentation example that uses it."""
        for name in rakaia.__all__:
            assert hasattr(rakaia, name), f"rakaia.__all__ lists missing {name!r}"

    def test_no_private_name_is_exported(self):
        assert [n for n in rakaia.__all__ if n.startswith("_")] == ["__version__"]


class TestDjangoRakaiaSurface:
    def test_all_is_exactly_the_pinned_set(self):
        import django_rakaia

        assert set(django_rakaia.__all__) == DJANGO_RAKAIA_PUBLIC

    def test_every_exported_name_resolves(self):
        import django_rakaia

        for name in django_rakaia.__all__:
            assert getattr(django_rakaia, name) is not None

    def test_an_unknown_name_raises_attribute_error(self):
        """The lazy `__getattr__` must not turn a typo into an ImportError from
        somewhere unrelated."""
        import django_rakaia

        with pytest.raises(AttributeError, match="no attribute"):
            _ = django_rakaia.definitely_not_exported

    def test_dir_includes_the_public_names(self):
        """So tab-completion and `help()` show the surface."""
        import django_rakaia

        assert set(dir(django_rakaia)) >= DJANGO_RAKAIA_PUBLIC


class TestImportingTheDjangoPackageStaysCheap:
    """The reason this package exported nothing for so long: eager imports pull
    the ORM in at package-import time and raise `AppRegistryNotReady` during
    Django's own startup. Lazy resolution is what makes a declared surface
    possible at all, so the laziness is part of the contract."""

    def test_the_models_module_is_not_imported_as_a_side_effect(self):
        import subprocess
        import sys

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys, django_rakaia; "
                "print('django_rakaia.models' in sys.modules)",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        assert result.stdout.strip() == "False", (
            "importing django_rakaia pulled in the ORM — that raises "
            "AppRegistryNotReady during Django startup"
        )


class TestTierTwoIsDeliberatelyNotExported:
    """The ORM models are usable but weaker than Tier 1 (`docs/public-api.md`).
    Keeping them out of `__all__` is what makes the weaker guarantee visible at
    the import site."""

    @pytest.mark.parametrize(
        "model",
        [
            "Stream",
            "StreamEvent",
            "StreamEntry",
            "StreamProducer",
            "StreamOffsetWatermark",
            "ConsumerCursor",
        ],
    )
    def test_a_model_is_not_in_the_stable_surface(self, model):
        import django_rakaia

        assert model not in django_rakaia.__all__

    def test_models_remain_importable_from_their_module(self):
        """Not exported is not the same as not supported — Tier 2 is usable."""
        from django_rakaia.models import Stream, StreamEntry, StreamEvent

        assert Stream and StreamEntry and StreamEvent


class TestTheReplayModuleIsShadowed:
    """`rakaia.replay` the attribute is the function, not the module (#161 item 1).

    The package root does `from .replay import … replay`, rebinding the package
    attribute. Harmless for imports, but a `monkeypatch.setattr` aimed at
    `"rakaia.replay.<name>"` lands on the function object and patches nothing —
    a call-count assertion then reads zero and passes. That cost a wrong
    measurement while verifying #156.

    Renaming either would break `rakaia.__all__`, so the sharp edge is documented
    in `replay.py` rather than removed. These two cases pin the *shape* — that the
    attribute is the function and the module is still importable — so a future
    change that quietly fixed or worsened the shadowing shows up here.

    There was a third case asserting the word "monkeypatch" appeared in
    `replay.py`'s docstring. It is gone: a substring check on module source is
    satisfied by a passing mention in a comment, which is the exact reason the
    closed-outcome assertions were deleted from `test_producer.py` in the same
    change. Keeping one while deleting the other was the inconsistency.
    """

    def test_the_package_attribute_is_the_function(self):
        import rakaia

        assert callable(rakaia.replay)
        assert not hasattr(rakaia.replay, "build_pipeline")

    def test_the_module_is_still_reachable_by_import(self):
        import sys

        from rakaia import replay as replay_module_attr  # the function
        from rakaia.replay import replay as replay_fn

        assert replay_module_attr is replay_fn
        assert sys.modules["rakaia.replay"].__name__ == "rakaia.replay"
        assert hasattr(sys.modules["rakaia.replay"], "build_pipeline")
