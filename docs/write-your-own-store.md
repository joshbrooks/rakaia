# Write your own store

A store is where streams are kept. rakaia ships three: one in memory, one in
files, one in a Django database. If your events have to live somewhere else, you
write the fourth, and it only has to do the storing. The rules that say whether an
append is allowed, what it becomes, and how a read is cut into pages are plain
functions exported from `rakaia`, the same ones the built-in stores call, so your
store cannot disagree with them about what an append means.

When it is written, you hold it to the same checks as the built-in ones. The check
is a ready-made set of tests that ships inside the package: you point it at your
store and run pytest.

## A small store

This one keeps everything in Python lists. Swap the lists for your database and
the shape stays the same: ask the rules, then save what they say. It is a store
for `replay()` and producers only: it never closes a stream (an append with
`close=True` is stored and the close ignored) and keeps no producer state, so
producer fencing is not supported. The appendix shows what to add.

```python
import time

from rakaia import (
    AppendResult,
    StreamFacts,
    StreamMessage,
    StreamNotFound,
    page_of,
    plan_appends,
)


class ListStore:
    def __init__(self):
        self._messages = {}  # path -> list of StreamMessage
        self._generation = {}  # path -> text that changes when it is recreated
        self._last_stamp = {}  # path -> the last event time this store gave out
        self._offset = 0  # never goes back, even when a stream is recreated
        self._created = 0

    def has(self, path):
        return path in self._messages

    def create(self, path):
        if path not in self._messages:  # creating twice changes nothing
            self._created += 1
            self._messages[path] = []
            self._generation[path] = str(self._created)

    def delete(self, path):
        self._generation.pop(path, None)
        return self._messages.pop(path, None) is not None

    def get_generation(self, path):
        return self._generation.get(path)

    def append(self, path, data, options=None):
        return self.append_many(path, [(data, options)])[0]

    def append_many(self, path, events):
        events = list(events)
        if path not in self._messages:
            raise StreamNotFound(path)

        # Ask the rules. A refusal raises here, before anything is stored.
        plan = plan_appends(
            StreamFacts(),  # this store has no closed streams or sequences
            [options for _data, options in events],
            payloads=[data for data, _options in events],
            now=time.time(),
        )

        # Give each write its event time, then save what the plan says.
        writes, stamp = plan.stamped(self._last_stamp.get(path), time.time())
        if stamp is not None:
            self._last_stamp[path] = stamp
        results = []
        for verdict, write in zip(plan.verdicts, writes, strict=True):
            message = None
            for payload in write.payloads if write else ():
                self._offset += 1
                message = StreamMessage(
                    data=payload,
                    offset=f"{self._offset:016d}",
                    timestamp=time.time(),
                    event_ts=write.event_ts,
                    label=write.label,
                    metadata=write.metadata,
                    tags=write.tags,
                    correlation_id=write.correlation_id,
                )
                self._messages[path].append(message)
            results.append(
                AppendResult(message=message, producer_result=verdict.producer_result)
            )
        return results

    def read(self, path, offset=None, *, limit=None):
        if path not in self._messages:
            raise StreamNotFound(path)
        after = [
            m
            for m in self._messages[path]
            if not offset or offset == "-1" or m.offset > offset
        ]
        return page_of(after, limit)

    def get_current_offset(self, path):
        if path not in self._messages:
            return None
        messages = self._messages[path]
        return messages[-1].offset if messages else f"{self._offset:016d}"
```

Three things are worth knowing before you change it:

- **`plan_appends` does the thinking.** It checks the body, the tags, the
  content type and the `Stream-Seq`, and answers for the whole batch at once: if
  any of those is wrong it raises and nothing is stored. An item the producer fence
  refuses, or one sent to a closed stream, does not raise; its verdict says not to
  write it and the rest of the batch is still written. Your code never inspects an
  `AppendOptions` itself.
- **An offset is a token you invent.** Readers hand it back to you and compare it
  with `>` as text, so it must sort the way the messages were written. Zero-padded
  numbers do; plain numbers do not (`"9" > "10"`).
- **`read` takes a page size.** `page_of` refuses a size below one, cuts the
  page and says whether any messages were left over.

## Hold it to the same checks

Write one test file. It names your store and nothing else:

```python
import pytest
from my_store import ListStore

from rakaia.testing.store_contract import StoreContract


class TestListStore(StoreContract):
    @pytest.fixture
    def store(self):
        return ListStore()
```

Install pytest (`pip install pytest`; rakaia does not install it for you) and run
`pytest`. Every check the in-memory and database stores pass now runs against
yours, and a failure names the rule you broke. If the store is more than a
framework store, there is a suite for each of the other jobs:

| Your class does this | Run this suite | It holds you to |
|---|---|---|
| Keeps streams for `replay()` and producers | `rakaia.testing.store_contract.StoreContract` | `WritableStore` and `CursorStore` |
| Backs the protocol server | `rakaia.testing.server_store_contract.ServerStoreContract` | `StreamServerStore` |
| Applies effects to tables | `rakaia.testing.executor_contract.ExecutorContract` | `Executor` |
| Records what a consumer did with each event | `rakaia.testing.outcome_store_contract.OutcomeStoreContract` | `OutcomeStore` |
| Answers a handler's lookups | `rakaia.testing.projection_reader_contract.ProjectionReaderContract` | `ProjectionReader` |
| Can return the events carrying given tags | `rakaia.testing.tagged_reads_contract.TaggedReadsContract` | `TaggedReads` |

`TaggedReads` is optional: a protocol server store does not need it, and
`ServerStoreContract` does not ask for it. Give your store `read_tagged(path, *tags,
newest_first=False, limit=None)` to let a caller fetch the events carrying every one
of the tags. It must not extend the stream's TTL
window, and `check_tagged_read` and `pick_tagged` (both exported from `rakaia`) hold the
argument checks and the matching the built-in stores share.

One check sets the clock by replacing `time.time`, so a store that reads the
time any other way (`from time import time`, `datetime.now()`) fails the
event-time checks for no visible reason: call `time.time()`.

The module paths under `rakaia.testing` are part of the supported surface, like the
names in `rakaia.__all__`. See [the public API](public-api.md).

## Appendix

### Which names to build from

All of these import from `rakaia`; the signatures are in the
[API reference](api-reference.md#writing-your-own-store-or-executor).

| Name | What a store or executor does with it |
|---|---|
| `plan_appends`, `StreamFacts` | Decide a batch of appends. `StreamFacts` is what you know about the stream: whether it is closed and by which producer, its content type, and its last `Stream-Seq`. |
| `AppendPlan`, `AppendVerdict`, `PlannedWrite` | What `plan_appends` returns: one verdict and one write (or `None`) per item, plus what the stream becomes afterwards (`last_seq`, `close`, `closed_by`, `producer_commits`). A verdict's `producer_result` is a `ProducerAccepted` when a producer-fenced append is written, and its `proposed_state` is what to save. |
| `plan_initial_write` | The same, for the body of a create. |
| `page_of`, `check_page_size` | Cut a page for `read(limit=...)`. `page_of` checks the size itself; call `check_page_size` alone to refuse a bad size before any side effect, such as extending a TTL. |
| `check_tagged_read`, `pick_tagged` | For `read_tagged`: `check_tagged_read(tags, limit)` refuses no tags, a bad tag or a limit below 1 and returns the cleaned tags; `pick_tagged(messages, tags, newest_first=..., limit=...)` keeps the messages carrying every tag, in the order asked, cut to the limit. |
| `apply_effects` | An executor supplies four writes (an upsert, a run of updates, a delete, a retire) and this runs them in the right order with `Ref` values filled in. |
| `UpsertOutcome`, `RowAccessor`, `ApplyReport` | The types `apply_effects` takes and returns. |
| `OnDriftPolicy` | The type of `replay(on_drift=...)`, for a wrapper that forwards it. |

`InMemoryProjections` is a complete executor and reader built on `apply_effects`,
and the closest thing to a worked example of one.

### A store with closed streams and producers

`plan_appends` already handles a closed stream, a producer's epoch and sequence, and
`Stream-Seq`. To use them, fill in `StreamFacts` from what you stored and pass the
saved state of each producer id in the batch as `producer_states`. Afterwards store
`plan.last_seq`; if `plan.close` is set, mark the stream closed with `plan.closed_by`;
and save the `proposed_state` of each `ProducerAccepted` in `plan.producer_commits`.
For each item, return an `AppendResult` whose `producer_result` is the verdict's and
whose `stream_closed` is `write.close` when the item was written and
`verdict.stream_closed` when it was not. The `ServerStoreContract` suite checks all
of it.

!!! note "The lifecycle rules are not exported yet"

    A store for the protocol server also has to decide expiry, re-creating with a
    different configuration, closing and deleting. The built-in stores take those
    from a module that is not part of the supported surface, so a store written
    outside this repository has to write those rules itself and let
    `ServerStoreContract` say whether it got them right.

### Running a suite without pytest installed

`import rakaia` and `import rakaia.testing` never need pytest. Importing a suite
does (all but `outcome_store_contract`), and if pytest is missing it fails with a
message saying to install it.
