"""`manage.py demo_hermeticity` — which handlers can be trusted to stay put.

It answers: *a handler's drift fingerprint is only as good as what it can see.
What does it refuse, and what does it silently miss?* Three checks, each
asserted hard (a failure raises CommandError):

  [1] WARNED  — a handler that reads the database, and one that reads a clock
                object, cannot be fingerprinted. Registration warns, naming each
                and saying an edit behind it will not read as drift.
  [2] STRICT  — with `RAKAIA_STRICT_HANDLER_FINGERPRINTS` set, the same
                registration raises `ImpureHandlerError` instead.
  [3] WRAPPER — the same dependency and the same wrapped function registered
                with `functools.partial` and with a closure. Editing the wrapped
                function drifts the partial; the closure's hash never saw it.
"""

from __future__ import annotations

import functools
import os
import warnings
from decimal import Decimal
from typing import Any

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from orders import rules
from orders.hermeticity import make_closure, reads_clock, reads_database
from rakaia import (
    DriftLedger,
    HandlerRegistry,
    ImpureHandlerError,
    ImpureHandlerWarning,
)
from rakaia.source_hash import hash_function_source

RATES = {"EUR": Decimal("1.0"), "USD": Decimal("0.9")}
STRICT_ENV = "RAKAIA_STRICT_HANDLER_FINGERPRINTS"


class Command(BaseCommand):
    help = "Show impure handlers being refused, and a closure hiding an edit."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        call_command("migrate", verbosity=0, interactive=False)

        self._check_warned()
        self._check_strict()
        self._check_wrapper()

        self.stdout.write(
            self.style.SUCCESS("\nAll handler-hermeticity checks passed ✓")
        )

    def _expect(self, condition: bool, message: str) -> None:
        if not condition:
            raise CommandError(message)

    def _impure_warnings(self, caught: list[Any]) -> list[str]:
        return [
            str(item.message)
            for item in caught
            if issubclass(item.category, ImpureHandlerWarning)
        ]

    # -- [1] warned ---------------------------------------------------------

    def _check_warned(self) -> None:
        registry = HandlerRegistry()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            registry.register(
                name="reads_database",
                event_match="orders",
                fn=reads_database,
                effective_from=0,
            )
            registry.register(
                name="reads_clock",
                event_match="orders",
                fn=reads_clock,
                effective_from=0,
            )

        messages = self._impure_warnings(caught)
        self.stdout.write("[1] WARNED — a handler that reaches outside its event:")
        for message in messages:
            self.stdout.write(f"    {message}")

        self._expect(
            len(messages) == 2,
            f"expected two impure-handler warnings, got {len(messages)}",
        )
        self._expect(
            any("reads_database" in m and "OrderSummary" in m for m in messages),
            f"the database read should be named: {messages}",
        )
        self._expect(
            any("reads_clock" in m and "SystemClock" in m for m in messages),
            f"the clock object should be named: {messages}",
        )
        self.stdout.write(
            self.style.SUCCESS(
                "    → both are registered, both warn once, and an edit behind "
                "either would go unnoticed ✓"
            )
        )

    # -- [2] strict ---------------------------------------------------------

    def _check_strict(self) -> None:
        previous = os.environ.get(STRICT_ENV)
        os.environ[STRICT_ENV] = "1"
        try:
            raised: ImpureHandlerError | None = None
            try:
                HandlerRegistry().register(
                    name="reads_database_strict",
                    event_match="orders",
                    fn=reads_database,
                    effective_from=0,
                )
            except ImpureHandlerError as exc:
                raised = exc
        finally:
            if previous is None:
                os.environ.pop(STRICT_ENV, None)
            else:
                os.environ[STRICT_ENV] = previous

        self.stdout.write(f"\n[2] STRICT — {STRICT_ENV}=1:")
        if raised is None:
            raise CommandError(
                "an impure handler should raise under the strict setting"
            )
        self.stdout.write(f"    ImpureHandlerError: {str(raised).splitlines()[0]}")
        self._expect(
            "OrderSummary" in str(raised),
            f"the error should name what could not be fingerprinted: {raised}",
        )
        self.stdout.write(
            self.style.SUCCESS(
                "    → the warning is promoted to an error, so the bad shape "
                "cannot reach a deployment that sets it ✓"
            )
        )

    # -- [3] wrapper --------------------------------------------------------

    def _check_wrapper(self) -> None:
        registry = HandlerRegistry()
        bound = functools.partial(rules.rate_value, rates=RATES)
        closure = make_closure(bound)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            partial_version = registry.register(
                name="rates_partial",
                event_match="orders",
                fn=bound,
                effective_from=0,
            )
            closure_version = registry.register(
                name="rates_closure",
                event_match="orders",
                fn=closure,
                effective_from=0,
            )
        closure_warnings = [
            message
            for message in self._impure_warnings(caught)
            if "rates_closure" in message
        ]

        self.stdout.write("\n[3] WRAPPER — the same dependency, two wirings:")
        self.stdout.write(
            f"    partial  dotted_path={partial_version.dotted_path} "
            f"impure=no  hash==wrapped function: "
            f"{hash_function_source(bound) == hash_function_source(rules.rate_value)}"
        )
        self.stdout.write(
            f"    closure  dotted_path={closure_version.dotted_path} "
            f"impure={'yes' if closure_warnings else 'no'}  "
            f"hash==wrapped function: "
            f"{hash_function_source(closure) == hash_function_source(rules.rate_value)}"
        )
        self._expect(
            hash_function_source(bound) == hash_function_source(rules.rate_value),
            "a partial must hash as the function it wraps",
        )
        self._expect(
            hash_function_source(closure) != hash_function_source(rules.rate_value),
            "a closure hashes its own wrapper, not the function it calls",
        )
        self._expect(
            bool(closure_warnings),
            "the closure should warn that the captured callable is impure",
        )

        # Edit the wrapped function: change the policy constant it reads. Nothing
        # about the registration objects changes, so this is exactly the edit a
        # fingerprint exists to notice.
        before = rules.MINOR_UNITS
        rules.MINOR_UNITS = Decimal(200)
        try:
            ledger = DriftLedger(on_drift="warn")
            partial_drifted = ledger.check(
                kind="handler",
                name="rates_partial",
                stored_hash=partial_version.hash_to_check,
                fn=partial_version.fn,
            )
            closure_drifted = ledger.check(
                kind="handler",
                name="rates_closure",
                stored_hash=closure_version.hash_to_check,
                fn=closure_version.fn,
            )
        finally:
            rules.MINOR_UNITS = before

        self.stdout.write(
            f"    after editing MINOR_UNITS: partial drift={partial_drifted}, "
            f"closure drift={closure_drifted}"
        )
        self._expect(
            partial_drifted,
            "the partial should drift when the function it wraps is edited",
        )
        self._expect(
            not closure_drifted,
            "the closure should not notice an edit to the function it calls",
        )
        self._expect(
            ledger.drifted == ["rates_partial"],
            f"only the partial should be reported: {ledger.drifted}",
        )
        self.stdout.write(
            self.style.SUCCESS(
                "    → bind the dependency with functools.partial: the hash "
                "describes the logic, so an edit is drift ✓"
            )
        )
