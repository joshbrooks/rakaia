---
icon: lucide/graduation-cap
---

# Tutorial: your first rebuilt table

By the end of this you will have a Django table that is built from a record of
changes rather than written to directly — and you will have fixed a bug in that
table by correcting the code and re-running, without touching the data.

It takes about ten minutes. Everything here was run start to finish against
`rakaia-streams` 0.2.0 and Django 6.1; the output shown is the real output.

You need Python 3.10 or newer. You don't need to know any event-sourcing
vocabulary — see the [glossary](glossary.md) if a word here is unfamiliar.

## 1. Install

```bash
mkdir billing-tutorial && cd billing-tutorial
python -m venv .venv && source .venv/bin/activate
pip install "rakaia-streams[django]"
```

## 2. Make a Django project

```bash
django-admin startproject billing .
python manage.py startapp invoices
```

Add both apps to `INSTALLED_APPS` in `billing/settings.py`:

```python
INSTALLED_APPS = [
    "django_rakaia",
    "invoices",
    # ...the rest, unchanged
]
```

## 3. Describe the table you want

This is an ordinary Django model. The only thing unusual is the rule you're
adopting: **nothing writes to this table by hand.** It gets built for you.

```python title="invoices/models.py"
from django.db import models


class InvoiceSummary(models.Model):
    """One row per invoice. Never edited by hand — rebuilt from the log."""

    invoice_id = models.CharField(max_length=64, unique=True)
    customer = models.CharField(max_length=100, default="")
    total = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    class Meta:
        ordering = ["invoice_id"]
```

## 4. Write the rule that fills it in

A **handler** is a plain function. It takes one recorded change and returns a
description of the row that change implies. It does not touch the database
itself — it just says what should be true.

`Upsert` means "make sure a row with this `lookup` exists, and give it these
`defaults`".

```python title="invoices/handlers.py"
from decimal import Decimal

from rakaia import Effect, Upsert, register_handler


@register_handler(name="invoice_total", event_match="invoices", effective_from=0)
def invoice_total(event: dict) -> Effect | None:
    total = Decimal("0")
    for item in event["items"]:
        total += Decimal(str(item["price"]))
    return Upsert(
        model_label="invoices.InvoiceSummary",
        lookup={"invoice_id": event["invoice_id"]},
        defaults={"customer": event["customer"], "total": total},
    )
```

That code has a deliberate bug. Leave it in — fixing it is the point of step 7.

Handlers have to be loaded at startup, so import the module when the app is
ready:

```python title="invoices/apps.py"
from django.apps import AppConfig


class InvoicesConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "invoices"

    def ready(self):
        from . import handlers  # noqa: F401
```

## 5. Add some changes, and build the table

Create `invoices/management/commands/rebuild.py` (with empty `__init__.py` files
in `invoices/management/` and `invoices/management/commands/`):

```python title="invoices/management/commands/rebuild.py"
from django.core.management.base import BaseCommand

from django_rakaia import DjangoExecutor, get_store
from invoices.models import InvoiceSummary
from rakaia import CollectingExecutor, replay, seed_stream

STREAM = "invoices"

EVENTS = [
    {
        "invoice_id": "INV-1",
        "customer": "Ana",
        "items": [{"price": "10.00", "quantity": 3}],
    },
    {
        "invoice_id": "INV-2",
        "customer": "Bo",
        "items": [{"price": "5.00", "quantity": 2}, {"price": "1.50", "quantity": 4}],
    },
]


class Command(BaseCommand):
    def handle(self, *args, **opts):
        store = get_store()
        store.delete(STREAM)
        seed_stream(STREAM, EVENTS, store=store)

        # Rehearsal: work out every change, write none of them.
        preview = CollectingExecutor()
        replay(store=store, stream_path=STREAM, executor=preview)
        self.stdout.write(
            f"Dry run: would apply {len(preview.effects)} changes, wrote nothing."
        )

        # For real this time.
        replay(store=store, stream_path=STREAM, executor=DjangoExecutor())
        for row in InvoiceSummary.objects.all():
            self.stdout.write(f"  {row.invoice_id}  {row.customer:6}  {row.total}")
```

Create the table and run it:

```bash
python manage.py makemigrations invoices
python manage.py migrate
python manage.py rebuild
```

```text
Dry run: would apply 2 changes, wrote nothing.
  INV-1  Ana     10.00
  INV-2  Bo      6.50
```

## 6. Notice the numbers are wrong

INV-1 is three items at $10.00. It should be $30.00, not $10.00. The handler
adds up prices and ignores quantities.

In an ordinary Django app this is now a data problem: the wrong totals are in
your table, and getting them right means a migration or a repair script that has
to work out what each row *should* have been.

Here it isn't a data problem, because the table isn't the original. The record of
what happened still has the quantities in it.

## 7. Fix the code, rebuild the table

Change one line in `invoices/handlers.py`:

```python
        total += Decimal(str(item["price"])) * item["quantity"]
```

Run the same command again:

```bash
python manage.py rebuild
```

```text
Dry run: would apply 2 changes, wrote nothing.
  INV-1  Ana     30.00
  INV-2  Bo      16.00
```

The table is correct. You wrote no migration and no repair script — you fixed the
rule and the rows were rebuilt from the record.

Run it a third time and you get the same two rows, not four. Rebuilding is safe
to repeat.

## What you just relied on

- **The record is the original; the table is derived.** That is what made the fix
  a code change rather than a data change.
- **The rehearsal is real.** The `CollectingExecutor` run worked out all the
  changes and wrote none of them. On a real database you would combine it with
  the guards in [preview a replay with no writes](dry-run-and-executors.md),
  which make writing during a rehearsal an error rather than a matter of trust.
- **Rebuilding is repeatable.** Each row is matched by its `lookup`, so replaying
  the same record converges instead of duplicating.

## Where next

- [Add streams to a Django model](django-integration.md) — emit these changes
  automatically on `save()` instead of writing them out by hand.
- [Why Rakaia exists](why-rakaia.md) — what this buys you beyond the toy case.
- [Versioned handlers](versioned-handlers.md) — for when the rule *should*
  change over time rather than being fixed.

---

## Appendix — details deliberately skipped above

**Fixing a bug vs. changing a rule.** In step 7 you replaced the handler
outright, so the correction applied to all of history. That is right for a bug:
the old totals were never correct. It is wrong for a *rule change* — if tax rises
next year, rebuilding must not retroactively tax last year's invoices. That case
uses `effective_from` / `effective_to` to bracket each version of the rule to the
stretch of the record it governed. See [versioned handlers](versioned-handlers.md).

**Why `effective_from=0`.** It marks this handler as applying from the very start
of the record. With no `effective_to`, it applies to everything after, too — one
rule, all of history, which is what a bug fix wants.

**Where the record is stored.** `get_store()` returned the in-memory store, which
is process-local and disappears when the command exits. That is why `rebuild`
seeds and replays in the same run. For a real project the record lives in your
database — see [Django integration](django-integration.md) for switching to the
durable store.

**`Upsert` vs `Update`.** `Upsert` creates the row if it is missing. `Update`
only touches rows that already exist, which is what you want for a handler that
decorates someone else's row and must never bring a half-built one into being.

**`store.delete()` at the top.** Only so the tutorial command is re-runnable
against the in-memory store. Do not do this to a real record — it is the one
thing in this file that destroys data rather than deriving it.
