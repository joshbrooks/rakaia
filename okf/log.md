# Directory Update Log

## 2026-09-23

* **Creation**: [formkit_emission](examples/formkit-emission.md) — formkit-ninja's own
  `emit`/`wire` decomposition appended to a rakaia log, and the check that an absent key
  and an explicit null come back as different events. The durable store is held to the
  same property by `TestAnAbsentKeyIsNotANullOne`; the demo's own store keeps payloads as
  opaque bytes and could not lose it.
* **Update**: [Django integration](concepts/django-integration.md) — `RAKAIA_PERMANENT_STREAMS`
  now refuses `DjangoStreamStore.delete()` from Python unless it is called with
  `force=True`. The switch previously refused only a client's protocol DELETE, which left
  the hand-run management command — the likeliest way to lose a stream — outside the guard
  it looked like it covered.

## 2026-09-22

* **Creation**: [stream_coverage](examples/stream-coverage.md) — a stream that fell behind its
  table, found by `check_stream_coverage` and repaired.
* **Update**: [Django integration](concepts/django-integration.md) — `stream_coverage` and the
  command, demonstrated by the new example.
* **Update**: [Protocol layer & streams](concepts/protocol-and-streams.md) — paged catch-up
  reads (`ServerOptions(read_page_size=…)`, `read(limit=…)`), the two permanent-stream
  refusals (`ExpiryNotAllowed`, `DeleteNotAllowed`), a delete that takes the stream's own
  events, and the stamp that now agrees with an event's position.
* **Update**: [Django integration](concepts/django-integration.md) — the settings and
  operator commands the bundle had never listed: `RAKAIA_READ_PAGE_SIZE`,
  `RAKAIA_PERMANENT_STREAMS`, `manage.py prune_orphan_events`, and the stream lock order a
  model save now shares with a protocol append.
* **Note**: everything in the two entries above shipped in 0.6.0, 0.6.1 and 0.7.0, and none
  of it reached this bundle before its release. Only examples are gated — a change that adds
  a setting or a command without adding an example walks past the test that would have
  caught it.

## 2026-09-08

* **Creation**: [Consuming a stream & failure records](concepts/consuming-and-outcomes.md) — a seventh
  concept group, for the loop that reads a stream and the record of what it could not apply
  (`poll`/`consume`, `Consumer`, `Outcome` and the three outcome stores, the published reason codes,
  the retention command and the screen).
* **Update**: [Effects & executors](concepts/effects-and-executors.md) — `RecordingExecutor`.
* **Creation**: [partisipa_intake](examples/partisipa-intake.md) — the worked example of the whole
  consuming loop (added with the example itself, logged here now).
* **Note**: a test now fails if an `examples/` directory has no page in this bundle, and the names
  in these pages are swept for resolution along with the rest of the documentation. Until today
  nothing checked either, and this log had not been written to since the bundle was created.

## 2026-07-28

* **Creation**: Rakaia knowledge bundle ([index](index.md)) — initial OKF catalog of rakaia's concepts and examples.
* **Creation**: [Protocol layer & streams](concepts/protocol-and-streams.md), [Versioned handlers & replay](concepts/versioned-handlers-and-replay.md), [Effects & executors](concepts/effects-and-executors.md), [Projections & fan-out](concepts/projections-and-fan-out.md), [Event envelope & provenance](concepts/event-envelope-and-provenance.md), [Django integration](concepts/django-integration.md) — the six concept groups.
* **Creation**: 13 example concepts under [examples/](examples/index.md), each cross-linked to the concepts it demonstrates; demos run green are marked `verified`.
