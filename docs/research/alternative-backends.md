# Three alternative stream backends: JSONL, DuckDB, DuckLake

**Status:** research notes, 2026-08-23. Not a decision — no ADR is implied by this
file, and nothing here changes source.

**Where this lives and why.** Same reasoning as
[`handler-types.md`](handler-types.md): `docs/` holds prose listed in
`zensical.toml`'s explicit `nav`, `docs/adr/` holds decisions, `okf/` holds the
machine-readable bundle. A scoping note is none of those, so it sits in
`docs/research/`, versioned with the code it argues about and deliberately not in
the nav.

---

## The short answer

Build **two** implementations, not three.

1. **JSONL first.** A file-per-id-range log with a sidecar `meta.json`. It is the
   cheapest of the three, needs no dependency at all, and it is the one that
   proves the storage seam is a real seam rather than a description of Django.
   Everything today either runs in RAM or runs on the ORM; nothing has ever
   tested the claim that a third backend can plug in.
2. **DuckDB and DuckLake as one backend with two attach modes**, not two
   backends. DuckLake is reached with `ATTACH 'ducklake:…'` and is then queried
   as an ordinary DuckDB catalogue. If the store speaks SQL against whatever is
   attached, "local single-file DuckDB" and "shared Parquet-on-object-storage
   DuckLake" are a connection string apart. Writing them as two independent
   `StreamServerStore`s would mean two copies of producer fencing, expiry,
   batching and offset allocation, and the fence is exactly the code that has
   already been got wrong twice (#181, #214).

Ranked by value-per-effort: JSONL, then the DuckDB/DuckLake pair. Plain DuckDB on
its own — a single-file local store with no sharing — earns the least: it is a
worse JSONL for simple records and a worse DuckLake for shared ones. It is worth
having only as the cheap local mode of the shared backend, and as the thing that
makes the log queryable with SQL.

---

## What a backend actually has to implement

The seam is `rakaia.protocols`, and it has two levels. A backend can stop at the
first.

**Level 1 — `WritableStore` + `CursorStore`.** What `replay()`, producers and the
meta-stream registry need. Five methods: `read`, `has`, `create(path)`, `append`,
`append_many`, plus `get_current_offset`. Conformance suite:
`tests/store_contract.py` (240 lines, ~20 tests).

**Level 2 — `StreamServerStore`.** What `rakaia.handler.create_app` needs to serve
the Durable Streams protocol on top of the backend. Adds eleven more: `run_sync`,
the widened `create(path, *, content_type, ttl_seconds, expires_at, initial_data,
closed)`, `get`, `touch`, `delete`, `format_response`, `append_with_producer`,
`close_stream`, `close_stream_with_producer`, `wait_for_messages`. Conformance
suite: `tests/server_store_contract.py` (1,087 lines).

Those two suites are the largest single asset here. A new backend's test file is
about fifteen lines — subclass both contracts, supply a `store` fixture — and
1,300 lines of behaviour then apply to it for free, including the shape checks
that assert every protocol method's signature actually binds. **Definition of
done for each backend below is: both contracts green.**

Most of the hard logic is already backend-neutral and gets reused rather than
rewritten: `rakaia.append_decision` (admission ordering, and the all-or-nothing
batch pre-flight), `rakaia.producer` (fencing state expiry), `rakaia.json_mode`
(JSON-mode flattening and response framing), `rakaia.offsets` (offset shape,
padding, ordering), `rakaia.context` (provenance merge). What a new backend has
to supply is genuinely only four things:

- **persistence** — write a message, read a window back;
- **offset allocation** — issue strictly increasing, lexicographically sortable
  tokens, including across a delete-and-recreate (#34);
- **serialisation** — some equivalent of `DjangoStreamStore._locked_write`, so a
  writer's closed / content-type / fencing checks and its write are one
  indivisible step;
- **long-poll** — `wait_for_messages`.

## Three constraints that apply to all of them

**1. The offset registry only distinguishes two formats, and both are taken.**
`rakaia.offsets.FORMATS` holds `COMPOUND` (two fields, in-memory) and `PLAIN` (one
field, durable). `format_of` picks between them purely by field count, and the
docstring notes the patterns are disjoint "one field versus two" — which stops
being true the moment a third format arrives. A third one-field format would be
indistinguishable from `PLAIN`, and `after()` would silently compare a JSONL
cursor against a Django head instead of raising `ForeignOffset` — the exact wrong
answer that module exists to refuse.

Two ways out, and they should be decided before any code is written:

- *Reuse `PLAIN`.* Both new backends have a natural single ascending entry id, so
  their tokens are already `PLAIN`-shaped. Zero work, but cross-store cursor
  confusion between Django and JSONL/DuckDB is then undetectable.
- *Give `OffsetFormat` a discriminator* — a short name prefix, so a token reads
  `jsonl:0000…` — and widen `format_of`. That is a change to a load-bearing shared
  module, and the padding/ordering guarantees have to survive it.

Reusing `PLAIN` is the right first move, with the caveat written down. The
mismatched-pair failure only arises from a test built against the wrong store or a
hand-edited cursor (see the `rakaia.offsets` module docstring), and that is a
narrower risk than editing the ordering rule for all four stores at once.

**2. Long-poll cannot use an in-process event.** The in-memory store notifies via
`asyncio.Event`; that only works because appends happen in the same process.
Every out-of-process backend has to do what `DjangoStreamStore.wait_for_messages`
does — poll on an interval until the deadline, extending the TTL window once on
the way out rather than on every tick. Copy that shape, including its two subtle
behaviours: an absent stream on the *first* pass raises `StreamNotFound`, but a
stream that expires *mid-wait* is reported as an ordinary timeout, not a 404.

**3. `append_many` is all-or-nothing.** A refusal anywhere in a batch must refuse
the whole batch before anything is written — a fence, a `Stream-Seq` conflict, or
a body the content type cannot hold. `decide_append_batch` gives every backend the
pre-flight; what each backend still owes is that the write itself cannot leave a
prefix behind. That is what #214 and #222 were about, and it is the first thing to
break in a file-backed store.

---

## Backend 1 — JSONL, file per id range

**Shape** (as chosen):

```
data/<stream>/
  meta.json              head, closed, closed_by, content_type, ttl, expires_at,
                         last_seq, producer states
  000000000000.jsonl     entry ids 1..9999
  000000010000.jsonl     entry ids 10000..19999
```

One JSON object per line: entry id, offset token, label, metadata, `event_ts`,
and the payload. A segment seals at N entries and a new one opens, so old
segments are immutable, a read can skip whole files by name, and retention
becomes "delete a file".

**Where the difficulty actually is.** Not in appending lines — in the four
things above.

- *Serialisation.* There is no `select_for_update`. An exclusive advisory lock on
  the stream directory (`fcntl.flock` on a lockfile) around the whole
  check-then-write is the equivalent, and it must cover the `meta.json` update
  too. This is POSIX-only; Windows needs `msvcrt.locking` or the backend
  documents itself as POSIX-only.
- *Crash atomicity.* An append and a `meta.json` update are two files, so they
  cannot be made atomic against each other. **Make the log authoritative and
  `meta.json` a cache**: head is derived from the last complete line of the last
  segment, and a torn trailing line (a crash mid-write) is truncated on open.
  Then a lost `meta.json` costs a scan, not the stream. The fields that are *not*
  derivable from the log — `closed_by`, producer states, TTL — still need
  write-temp-then-`os.replace` to be crash-safe.
- *Batch atomicity.* Serialise the whole batch into one buffer and issue one
  `write()` under the lock, so a refusal happens before the buffer exists.
- *Reads.* `read(path, offset)` must not scan from zero on a long stream; the
  segment filename is the index, so resolve the offset to a segment first.

**Effort:** one new module of roughly the size of `rakaia/store.py`, plus a test
file. Call it 2–3 days to both contracts green, most of it in crash-safety and
the lock, not in the JSON.

**What it is good for:** single-node deployments, embedded and CLI use, fixtures
and demos, and anything where the log wants to be readable with `less` and
diffable in git. **What it is not good for:** many writers, or querying.

---

## Backend 2/3 — DuckDB, and DuckLake as its shared mode

**Shape.** Three tables mirroring the durable model closely enough that the
migration story is obvious: `streams` (one row of metadata per path),
`stream_entries` (id, path, offset, label, metadata, `event_ts`, payload), and
`stream_producers` (path, producer id, epoch, last seq, last updated). Offsets are
the entry id, `PLAIN`-rendered.

Local mode attaches a `.duckdb` file. Shared mode attaches a DuckLake catalogue —
metadata in a SQL database, data as Parquet — and the same SQL runs against both.
That is the whole argument for treating these as one backend.

**The sharpest risk: there is no row lock to take.** DuckDB gives snapshot
isolation with optimistic concurrency, not `SELECT … FOR UPDATE`. Every place the
Django store relies on `_locked_write` — offset allocation, the fence commit, the
close — becomes a read-modify-write that must either run under a single writer or
retry on conflict. Given the answer to start single-writer, the first version
should:

- take a process-level lock and document the single-writer constraint honestly;
- **still write the retry path**, because the point of DuckLake is that the
  constraint lifts later, and retry-on-conflict retrofitted into fencing code is
  how #181-shaped defects happen;
- assert the constraint in a test, not just in prose.

Note that `tests/test_locking.py` and `tests/test_concurrent_appends.py` are the
two files the project has designated for lock coverage, and CLAUDE.md's rule
applies: a test that exercises a lock must be shown to fail with the lock removed.
That rule is more important here than it is on Postgres, because there is no
database error to catch when it is missing — just a lost update.

**The second risk: file explosion.** A DuckLake commit materialises new Parquet
files. An event log that commits one row per append will produce an enormous
number of tiny files. Mitigations, in order: route bulk writes through
`append_many` so a batch is one transaction; run compaction
(`ducklake_merge_adjacent_files`-style) on a schedule; and be explicit in the docs
that per-event appends against a shared DuckLake are the slow path. This is the
thing most likely to make the shared backend look bad in a first demo.

**The payoff, which neither other backend has:** the log becomes a SQL table.
Projections, drift checks, `docs/history-read-model.md`-style reads and ad-hoc
analytics all become queries over Parquet instead of replays through Python, and
DuckLake snapshots give time travel over the log for free.

**Effort:** 3–5 days for the local DuckDB mode to both contracts green. The
DuckLake mode is then mostly a connection string, a compaction story, and a second
run of the same two contract suites in CI against an attached catalogue — call it
another 2–3 days, plus whatever the concurrency work costs when single-writer is
lifted.

**Version caution.** DuckDB's concurrency story and DuckLake's API have both moved
quickly. Every version-specific claim above — what a DuckDB 2.0 release does about
multi-process writers, the exact compaction function name, DuckLake's conflict
semantics — should be re-checked against current docs before anything is
committed, not taken from this note.

---

## Packaging and CI

Both land as optional extras, keeping the zero-dependency core intact:

```toml
jsonl  = []                  # stdlib only; an extra purely as a marker
duckdb = ["duckdb>=…"]
```

CI gains one job per backend running the two contract suites. `just check` stays
as it is; the new suites are skipped when the extra is not installed, the same way
the Postgres leg is opt-in. Each backend also needs an entry in
`rakaia.__all__` and therefore a `just api-reference` run — the generated
`docs/api-reference.md` is a CI gate.

## Open questions

1. **Offset format** — reuse `PLAIN` for both new backends, or add a
   discriminator to `OffsetFormat`? (Recommendation above: reuse, and write down
   the caveat.)
2. **Segment size N for JSONL** — a knob, or fixed? A knob is a compatibility
   surface: segment filenames encode it.
3. **Does the JSONL backend implement level 2 at all?** Long-poll and producer
   fencing over files are most of its cost. If it only ever backs `replay()` and
   fixtures, stopping at `WritableStore` halves the work — but then it cannot
   back a protocol server, which is arguably the point of proving the seam.
4. **Does DuckDB replace or complement the Django store** for projections? They
   overlap heavily, and two durable answers with no stated preference is how a
   library gets confusing.
