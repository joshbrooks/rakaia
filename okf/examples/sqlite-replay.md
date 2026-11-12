---
type: Example
title: "sqlite_replay — replay into a SQLite file (no Django)"
description: "A standard-library script that replays an order stream into a SQLite database with `SqliteExecutor` and reads it back with `SqliteProjectionReader`."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/sqlite_replay
tags: [example, standalone, executors, sqlite]
status: stable
generated: { by: claude-code/sonnet-5-5, at: 2026-11-12T00:00:00Z }
verified:
  - { by: process:just-sqlite-replay-demo, at: 2026-11-12T00:00:00Z }
---

# What it proves

Replay into a real database needs no Django. `SqliteExecutor` writes a replayed
order stream to a SQLite file; replaying twice leaves the same rows; the file is
readable through `SqliteProjectionReader` and through a plain `sqlite3`
connection; a batch that fails part way is rolled back whole; and nothing named
`django` is imported.

# Run

```sh
just sqlite-replay-demo
```

# Concepts demonstrated

* [Effects & executors](../concepts/effects-and-executors.md)
