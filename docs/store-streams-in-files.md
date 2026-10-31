# Store streams in files

Rakaia can keep a stream in three places. In the memory of the running process,
which is fast and vanishes when the process stops. In a Django database, which
survives anything but needs a database. Or in a folder of ordinary text files,
which is what this page is about.

The file-backed store needs no database and no extra packages. Each stream is a
directory, each event is one line of JSON, and the whole log can be read with
`less`, searched with `grep` and diffed in git. Point it at a memory-backed
folder — `/dev/shm` on Linux, or any tmpfs mount — and it runs at memory speed
while still being readable by every other process on the machine, which the
in-memory store cannot do at any speed.

## Using it

```python
from rakaia import JsonlStreamStore

store = JsonlStreamStore("/var/lib/rakaia/streams")
store.create("submissions")
store.append("submissions", b'{"id": 1, "name": "kea"}')
```

From Django, name it in settings instead:

```python
RAKAIA_STORE = "jsonl"
RAKAIA_JSONL_ROOT = "/var/lib/rakaia/streams"
```

There is no default location on purpose. A guessed one would accept every
append and put the log somewhere the next deployment does not look, so a missing
`RAKAIA_JSONL_ROOT` is reported by `manage.py check` rather than discovered
later.

Two more settings are optional. `RAKAIA_JSONL_SEGMENT_SIZE` sets how many events
go in a file before a new one starts (10,000 by default).
`RAKAIA_JSONL_FSYNC` decides whether an append waits for the disk; leave it on
unless the folder is memory-backed, where there is no disk to wait for.

## What it looks like on disk

```
/var/lib/rakaia/streams/
    submissions/
        meta.json              is it closed, when does it expire, who wrote last
        000000000000.jsonl     events 1 to 9,999
        000000010000.jsonl     events 10,000 to 19,999
```

Naming each file after the events it holds is what keeps reading cheap. A reader
catching up from event 19,000 opens the last file and ignores the rest, so
resuming costs what is new rather than what came before it. It also makes
retention a matter of deleting whole files.

## Moving a log between backends

Changing the `RAKAIA_STORE` setting does not move anything. The application
starts reading a different, empty log, while every consumer still holds a
position that looks perfectly valid — so the move is a copy, and there is a
function for it:

```python
from rakaia import migrate_stream, JsonlStreamStore
from django_rakaia.django_store import DjangoStreamStore

result = migrate_stream(
    DjangoStreamStore(), JsonlStreamStore("/var/lib/rakaia"), "submissions"
)
```

The copy carries the events, their labels and metadata, their logical
timestamps, the content type, the expiry and whether the stream was closed. What
it will not do is *promise* that saved consumer positions still work, because
that depends on both stores numbering events the same way. Instead it copies
first and then checks, and tells you:

```python
if result.cursors_valid:
    ...  # consumers resume where they left off
else:
    ...  # reset consumers before starting them again
for note in result.notes:
    print(note)  # anything the copy could not carry
```

Between the database-backed and file-backed stores, positions normally survive:
both number events one, two, three. Coming from the in-memory store they never
do, because it counts bytes instead. `migrate_all` copies every stream a store
can list.

When positions survive, so does the stream's stamp. A consumer keeps that stamp
beside its position and stops with `replaced` if it changes, which is how it
learns its stream was deleted and recreated; a copy that kept every offset is the
same log, so a consumer that has run before carries on instead of being told to
rebuild. When positions do not survive the copy keeps a stamp of its own, and
consumers stop, which is what you want.

One rule surprises people. A stream that has been deleted can never get its old
numbering back, even by copying the same events in again — a store must not
reissue a position a consumer may already have read past. So exporting a log,
deleting it, and importing it into the same place gives you the events but not
the positions, and the report says exactly that.

## When to use which

Use the **in-memory** store for tests and demos, where nothing needs to outlive
the process.

Use the **file-backed** store when you want the log to survive restarts without
running a database — single-machine deployments, embedded and command-line
tools, development against real data, or a tmpfs folder when you want speed and
still need other processes to read along.

Use the **Django** store when you already have a database, want the log in the
same transaction as your other writes, or need many machines writing at once.
The file-backed store coordinates its writers through the filesystem, which
works between processes on one machine and does not stretch across a network
share.

## Appendix

**Concurrency.** Writers take an exclusive `flock` on a lock file in the stream's
directory, held across the whole check-then-write, which is the file-backed
counterpart of the durable store's `select_for_update()` on the stream row. This
serialises appends, closes and batches between processes on one machine. It is
not safe over NFS or another network filesystem, where `flock` semantics are not
dependable. The store is POSIX-only: without `fcntl` it refuses to start rather
than run unlocked, because an unlocked writer hands out an offset another writer
has already taken and the loss is invisible until the log is read back.

Readers take no lock. A read can miss a record that is being written at that
instant, and will see it on the next read; it can never see a partial one,
because an incomplete trailing line is skipped.

**The TTL window is not in `meta.json`.** Extending a sliding expiry window is
something a *reader* does, and metadata here is a file replaced whole — so a
reader that wrote the window back would roll the head backwards over any append
that landed while it was reading. The window lives in its own `activity` file,
which is the file-level equivalent of the durable store's single-column update.

**Crash recovery.** The log is authoritative and `meta.json` is a cache. The head
of a stream can always be rebuilt by reading the last complete line of the last
segment, so a lost or truncated `meta.json` costs a scan rather than the stream.
A partial trailing line — a process killed mid-append — is ignored when reading
and truncated before the next append, so the log heals itself rather than
needing a repair pass. What `meta.json` alone holds, and what a loss of it does
cost, is the TTL configuration, the producer fencing state and the record of
which producer closed the stream: the log does not record them, so a close or a
producer sequence that was only in a lost write is lost with it.

**Batches.** Each write to a segment file is atomic, and a batch is one write per
segment it touches. A batch that crosses a segment boundary is therefore two
writes, and a crash between them can leave the first without the second;
recovery keeps that prefix.

**Segment size.** The size is recorded in `meta.json` when a stream is created,
because the segment a record goes in is worked out from it. Opening a stream with
a different `segment_size` raises `SegmentSizeMismatch` naming both values rather
than quietly writing into segments that are already full. A stream whose
`meta.json` is lost, or predates the recorded size, takes the size from its segment
filenames. With only one segment there is nothing to read, so the store's own size is
used only if every entry already fits that size's first segment; otherwise the stream
was written with a larger size, the open is refused, and you must open it with the size
it was written with, which nobody recorded.

**Delete and create.** Both take the stream's lock, so a delete waits for a writer
that is mid-append instead of pulling the directory out from under it, and an
append that was queued behind a delete fails with the store's own stream-not-found
error. A delete moves the directory into `%retired` and then removes it, so a crash
between the two leaves a `%trash-*` directory there; the next delete sweeps any that
are over a minute old.

**Durability.** With `fsync` on (the default) an append that has returned is on
the disk, and a new segment's directory entry is synced too — syncing a file
does not commit the name that points at it. A batch pays one sync per segment
rather than one per event, which is what makes the default affordable for bulk
writes. With `fsync` off, an append survives the death of the process that wrote
it, because the page cache outlives the process, but not a power cut.

**What it does not do.** The Django store broadcasts every append over channels
as it writes it, so live consumers hear about an event the moment it lands. The
file-backed store cannot: it lives in the framework-independent package and has
no way to reach Django. Live consumers must poll instead — the protocol server's
long-poll and SSE reads work normally over it, including for appends made by a
different process. `manage.py check` warns (`rakaia.W002`) if you select this
backend in a project that has channels installed, because consumers going quiet
is otherwise a silent change.

**Offsets.** This store issues the same plain, zero-padded entry-id offsets as
the durable store. They are opaque tokens: pass one back to `read()` to resume,
and do not parse it. Because both stores issue the same *format*, a cursor saved
against one and replayed against the other cannot be detected as foreign — the
usual mismatch guard in `rakaia.offsets` distinguishes formats, not stores.
Clear saved cursors when switching backends.
