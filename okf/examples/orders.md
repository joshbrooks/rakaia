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
dry-run, `op="external"` receipt effects skipped on replay, `op="update"`
(update-if-exists) for a loyalty bonus, and drift detection via
`on_drift="raise"`.

# Run

```sh
just orders-demo
```

# Concepts demonstrated

* [Versioned handlers & replay](../concepts/versioned-handlers-and-replay.md)
* [Effects & executors](../concepts/effects-and-executors.md)
