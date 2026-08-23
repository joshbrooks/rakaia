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
which producer closed the stream.

**Durability.** With `fsync` on (the default) an append that has returned is on
the disk, and a new segment's directory entry is synced too — syncing a file
does not commit the name that points at it. A batch pays one sync per segment
rather than one per event, which is what makes the default affordable for bulk
writes. With `fsync` off, an append survives the death of the process that wrote
it, because the page cache outlives the process, but not a power cut.

**Offsets.** This store issues the same plain, zero-padded entry-id offsets as
the durable store. They are opaque tokens: pass one back to `read()` to resume,
and do not parse it. Because both stores issue the same *format*, a cursor saved
against one and replayed against the other cannot be detected as foreign — the
usual mismatch guard in `rakaia.offsets` distinguishes formats, not stores.
Clear saved cursors when switching backends.
