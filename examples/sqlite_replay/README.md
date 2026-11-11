# sqlite_replay — replay into a real database with no Django

Replay does not need Django, but until `SqliteExecutor` the only executor that
wrote to an actual database was the Django one. This replays a small order stream
into a SQLite file using only the standard library, and reads it back through
`SqliteProjectionReader` and through a plain `sqlite3` connection.

## Run

```sh
just sqlite-replay-demo
```

No database to create, no settings, no migrations. The demo checks four things
and fails loudly if any breaks: replaying twice leaves the same rows; the file
holds what was written; a batch that fails part way leaves none of itself behind;
and nothing named `django` was imported.

## How rows are stored

Each model label is a table named exactly as the label, so the example reads
`SELECT pk, row FROM "shop.Order"`. A table is an integer `pk` and one `row`
column of JSON, created on first use. There is nothing to migrate, and a value
must be JSON-serialisable: the example keeps money as whole cents for that reason.
