---
type: Example
title: "stream_coverage — a stream that fell behind its table"
description: "Opens a gap between a table and the stream written from it, finds it with check_stream_coverage, and repairs it."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/stream_coverage
tags: [example, django, coverage, operations]
status: stable
generated: { by: claude-code/opus-5, at: 2026-09-22T00:00:00Z }
verified:
  - { by: process:just-coverage-demo, at: 2026-09-22T00:00:00Z }
---

# What it proves

A `Submission` model decorated with `@stream_model` writes one event per save to
the `submissions` stream, and `RAKAIA_COVERAGE_CHECKS` holds the table against
that stream with `changed_field="updated_at"`. `manage.py check_stream_coverage`
passes on 20 saved rows; reports 5 missing rows, by id, after a `bulk_create`
that wrote no events, and fails; reports 2 stale rows after a `QuerySet.update`
that wrote none either; and passes again once the rows it named are saved through
the producer. Each outcome is asserted, including that the command raises
`CommandError` (a non-zero exit under `manage.py`) exactly when there is a gap.

# Run

```sh
just coverage-demo
```

# Concepts demonstrated

* [Django integration](../concepts/django-integration.md)
