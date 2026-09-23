---
type: Example
title: "formkit_emission — formkit-ninja's real decomposition into a rakaia log"
description: "Drives formkit-ninja's published emit/wire seam rather than a model of it, and checks that an absent key and an explicit null come back as different events."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/formkit_emission
tags: [example, standalone, interop, event-envelope]
status: stable
generated: { by: claude-code/opus-5, at: 2026-09-23T00:00:00Z }
verified:
  - { by: process:just-formkit-emission-demo, at: 2026-09-23T00:00:00Z }
---

# What it proves

Every other `partisipa_*` example models a formkit submission; this one imports
`formkit_ninja.form_submission.emit` and `wire` and appends what they produce.
`emit_submission` yields the root first and repeater rows parent-before-child,
keyed by identities taken from the document, with `$rank` and `uuid` outside
`fields` so a re-order is not a content change. Each emission becomes a
`ContentEvent` subclass carrying the consumer's own keys, checked against
`REQUIRED_CONTENT_EVENT_KEYS` and `CONTENT_EVENT_KEYS`. Streams are named with the
consumer's own prefix (`submissions/tf611`, not the library's advisory
`submission/tf611`) and the library's `slug`.

The property it exists for is that **an absent key and a key set to `None`
come back as different events** — `parent_submission=None` says a row has
no parent, no key says only that nobody said. A `TypedDict` cannot express
"present iff supplied", so nothing type-checks it. The demo runs against the
in-memory store, where payloads are opaque bytes; the durable store, which decodes
into a `JSONField`, is held to the same property by `TestAnAbsentKeyIsNotANullOne`
in `tests/test_django_rakaia/test_django_store.py`. The demo prints the trap no
store can prevent: `.get()` answers `None` for both, so a consumer must read
with `in`. `schema_version` is never fabricated for an event recorded before
versions existed, `emit_reorder` costs one event per moved row and nothing for a
group that did not move, and the log folds back into three rows ordered by rank.

It also asserts a gap it found: `rank` has no key on `ContentEvent` though it is
the decomposition's own vocabulary, so the assertion goes red when formkit-ninja
closes it.

# Run

```sh
just formkit-emission-demo
```

Standalone — no Django settings, no database. formkit-ninja comes in through
`uv run --with`, not a project extra: it pins `Django==4.*` where this project
tests against Django 6, and uv resolves extras together. Without it the demo
fails and names the command rather than skipping.

# Concepts demonstrated

* [Event envelope & provenance](../concepts/event-envelope-and-provenance.md)
* [Protocol layer & streams](../concepts/protocol-and-streams.md)
