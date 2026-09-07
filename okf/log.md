# Directory Update Log

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
