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
    "JsonlStreamStore",
    "seed_stream",
    # moving a log between backends
    "Migration",
    "migrate_all",
    "migrate_stream",
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
    "ForeignOffset",
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
    "DriftLedger",
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
    "reset_store_cache",
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


class TestTheLazyRootResolvesOnce:
    """`__getattr__` caches into `globals()`, and that caching is the contract.

    Deleting the cache line left the whole suite green while making every
    `rakaia.app` access mint a fresh app and a fresh `StreamStore` — so the
    property the module root is *for* had no test at all. These are those tests.
    """

    def test_a_resolved_name_becomes_a_real_module_attribute(self):
        """The cache is what makes the *second* access free, and identity alone
        cannot see it.

        `rakaia.StreamStore is rakaia.StreamStore` passes with the caching line
        deleted — re-resolving imports an already-imported module and returns
        the same class object, so the assertion is satisfied by `importlib`
        being idempotent rather than by anything this module does. What the
        cache actually promises is that the name stops going through
        `__getattr__` at all, which is visible in the module dict and nowhere
        else.

        A subprocess because the check is about the transition from absent to
        present, and any earlier test in the session may already have resolved
        the name.
        """
        import subprocess
        import sys
        import textwrap

        program = textwrap.dedent("""
            import rakaia
            print("before" if "StreamStore" not in vars(rakaia) else "PRESENT")
            rakaia.StreamStore
            print("after" if "StreamStore" in vars(rakaia) else "MISSING")
        """)
        result = subprocess.run(
            [sys.executable, "-c", program], capture_output=True, text=True, timeout=60
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.split() == ["before", "after"], result.stdout

    def test_a_resolved_name_is_the_submodule_s_own_object(self):
        """Lazy resolution must hand back the real thing, not a copy or proxy."""
        from rakaia import store

        assert rakaia.StreamStore is store.StreamStore

    def test_the_app_is_built_once(self):
        assert rakaia.app is rakaia.app

    def test_the_app_is_built_once_under_concurrent_first_touch(self):
        """The regression this guards is invisible after the first access.

        `app` is *constructed*, not imported, so before the lock every thread
        that raced past the cache check built its own — 16 threads gave 16
        stores, 15 of them orphaned and holding their own log. The eager
        `app = create_app()` this replaced was serialised by the import lock, so
        this asserts a property that used to come for free.

        A fresh interpreter per run, because `app` is cached after first touch
        and the race exists only on the way in.
        """
        import subprocess
        import sys
        import textwrap

        program = textwrap.dedent("""
            import threading
            import rakaia

            start = threading.Barrier(16)
            seen = []

            def touch():
                start.wait()
                seen.append(id(rakaia.app))

            threads = [threading.Thread(target=touch) for _ in range(16)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            print(len(set(seen)))
        """)
        result = subprocess.run(
            [sys.executable, "-c", program], capture_output=True, text=True, timeout=60
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "1", (
            f"16 threads saw {result.stdout.strip()} distinct apps; each extra "
            "one is an orphaned StreamStore holding its own log"
        )


class TestTheLazyRootRefusesWhatItDoesNotExport:
    """A module `__getattr__` that returns instead of raising makes `hasattr`
    unconditionally true, which is worse than a missing name — it turns a typo
    into `None` at the call site rather than an error at the import."""

    def test_an_unknown_name_raises(self):
        with pytest.raises(AttributeError):
            rakaia.not_a_real_export  # noqa: B018

    def test_the_error_names_the_module_and_the_attribute(self):
        with pytest.raises(AttributeError) as exc:
            rakaia.not_a_real_export  # noqa: B018

        assert "rakaia" in str(exc.value)
        assert "not_a_real_export" in str(exc.value)

    def test_hasattr_is_false_for_a_name_that_is_not_exported(self):
        assert not hasattr(rakaia, "not_a_real_export")

    # Deliberately not asserted: that an unexported *submodule* — `read_decision`
    # and friends — is unreachable as `rakaia.<name>`. It is reachable, once
    # anything in the process has imported it, because the import system binds a
    # submodule onto its parent package. That is ordinary Python and is true of
    # the eager version too (checked on both), so an assertion here would pin
    # import order rather than this module's surface, and would pass or fail
    # depending on what else the test session had touched.


class TestTheLazyRootAndItsTypeCheckingBlockAgree:
    """The package root resolves exports lazily, so it carries the same list
    twice: `_EXPORTS`, which the runtime uses, and a `TYPE_CHECKING` block, which
    is the only thing pyright can follow. Two lists drift.

    Drift here is silent in the worst way. A name missing from the block still
    *works* — `__getattr__` returns it — but pyright types it as `Any`, so every
    annotation checked against it stops being checked and nothing goes red. That
    is also what `pyproject.toml`'s `F401` exemption for this file leans on: ruff
    can no longer see the block's imports as used, so this is what keeps them
    honest instead.
    """

    @staticmethod
    def _type_checking_imports() -> set[str]:
        import ast
        import pathlib

        source = pathlib.Path(rakaia.__file__).read_text()
        for node in ast.walk(ast.parse(source)):
            is_guard = (
                isinstance(node, ast.If)
                and isinstance(node.test, ast.Name)
                and node.test.id == "TYPE_CHECKING"
            )
            if not is_guard:
                continue
            return {
                alias.asname or alias.name
                for child in ast.walk(node)
                if isinstance(child, ast.ImportFrom)
                for alias in child.names
            }
        raise AssertionError("no `if TYPE_CHECKING:` block in rakaia/__init__.py")

    def test_the_block_covers_every_lazily_resolved_name(self):
        lazily_resolved = set(rakaia._EXPORTS) - {"replay"}

        missing = lazily_resolved - self._type_checking_imports()

        assert not missing, (
            f"in `_EXPORTS` but not the TYPE_CHECKING block: {sorted(missing)}. "
            "These still import at runtime, and pyright silently types them "
            "`Any` — add them to the block."
        )

    def test_the_block_imports_nothing_that_is_not_exported(self):
        """The other direction, which ruff used to catch and no longer can."""
        extra = self._type_checking_imports() - set(rakaia._EXPORTS)

        assert not extra, (
            f"imported for type checkers but not exported: {sorted(extra)}. "
            "An unused import in that block is invisible to ruff now that the "
            "file is F401-exempt."
        )

    def test_replay_is_bound_eagerly_rather_than_listed_in_the_block(self):
        """`replay` is the one name that cannot be lazy, so it must not be in
        the block: it is imported unconditionally, and a second import under
        `TYPE_CHECKING` would be a redefinition that hides which one wins."""
        assert "replay" not in self._type_checking_imports()
        assert "replay" in rakaia._EXPORTS


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
                (
                    "import sys, django_rakaia; "
                    "print('django_rakaia.models' in sys.modules)"
                ),
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
            "ConsumerOutcome",
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
