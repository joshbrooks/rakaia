---
type: Example
title: "orders — versioned handlers, upcasters & replay"
description: "An e-commerce projection showing time-correctness: the sales-tax rule changed on a date, and orders placed before it must keep the old tax."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/orders
tags: [example, django, versioned-handlers]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-orders-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

An e-commerce projection showing time-correctness: the sales-tax rule changed
on a date, and orders placed before it must keep the old tax. Demonstrates
`register_handler` with `effective_from`/`effective_to` ranges, an upcaster
(`register_upcaster`) normalising an old field name, a `CollectingExecutor`
dry-run, `ExternalEffect` receipt effects returned rather than applied, `Update`
(update-if-exists) for a loyalty bonus, and drift detection via
`on_drift="raise"`.

# Run

```sh
just orders-demo
just hermeticity-demo
```

`hermeticity-demo` exercises the fingerprint's limits: a handler that reads the
projection table, and one that reads a clock object, both warn at registration and
raise `ImpureHandlerError` under `RAKAIA_STRICT_HANDLER_FINGERPRINTS`. It also
registers the same wrapped function with `functools.partial` and inside a closure,
edits the policy constant the function reads, and shows drift in the partial but
not the closure.

# Concepts demonstrated

* [Versioned handlers & replay](../concepts/versioned-handlers-and-replay.md)
* [Effects & executors](../concepts/effects-and-executors.md)
* [Django integration](../concepts/django-integration.md)
