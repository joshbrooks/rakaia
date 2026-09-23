# formkit_emission — the real formkit-ninja seam, into a rakaia log

Every other `partisipa_*` example here models a formkit submission. This one
does not model anything: it imports
[`formkit-ninja`](https://github.com/catalpainternational/formkit-ninja)'s own
decomposition and appends what it produces to a rakaia stream.

That matters because nothing in **this** repository exercised that seam before.
Every other example here invents an event shape and asserts against its own
invention, so a change to what formkit-ninja publishes could not make any of
them fail. This one imports `emit_submission`, `emit_reorder`, `slug` and
`ContentEvent` and asserts against them, so it breaks when they move.

Whether anything *else* depends on that seam is a question about other
repositories, and this file deliberately does not answer it. Three drafts of a
sentence doing so were wrong in three different ways — a file that does not
exist, a scope word covering a hundredfold more files than intended, and a claim
this example itself falsified by being written. Nothing in rakaia's CI can check
a statement about another project's source, so the statement does not belong
here. `just formkit-emission-demo` is the check that does run.

## Run

```sh
just formkit-emission-demo
```

No database, no Django settings, no migrations. `emit_submission` reads `pk`,
`form_type` and `fields` and queries nothing, so a three-attribute stand-in
drives the real walk. Run directly, the demo **fails and names the command**
rather than skipping when formkit-ninja is absent: it exists to break when the
published shape changes, and a gate that quietly passes when it cannot measure
is worse than no gate.

### Why `--with` and not an extra

formkit-ninja pins `Django==4.*`. This project tests against Django 6, and uv
resolves every extra together, so declaring a `formkit` extra silently drags the
whole environment — CI included — down to Django 4.2. That was tried here and
turned five unrelated tests red. The recipe uses `uv run --with`, which resolves
in an overlay that touches neither `.venv` nor `uv.lock`.

## What it proves

Sections [1], [2], [3] and [7] are assertions about **formkit-ninja** — they
would still pass if rakaia's store started mangling payloads. [4], [5] and [6]
are the ones that go through the log and back.

| Section | Claim | About |
|---|---|---|
| [1] | `emit_submission` yields the root first, then repeater rows parent-before-child, keyed by identities that came out of the document — which is what lets a replay reproduce the same primary keys. `$rank` and `uuid` stay outside `fields`, so re-ordering a row is not a content change. | formkit-ninja |
| [2] | Every event carries `REQUIRED_CONTENT_EVENT_KEYS` and nothing outside `CONTENT_EVENT_KEYS` except keys this consumer minted by subclassing `ContentEvent`. | formkit-ninja |
| [3] | Stream names are the consumer's (`submissions/tf611`, Partisipa's spelling — not the library's advisory `submission/tf611`), while the form's spelling comes from `slug`.  half each |
| [4] | An absent key and an explicit `null` come back as different events. | the log |
| [5] | `schema_version` absent means "recorded before versions existed" — never fabricated at write time, never defaulted at read time. | the log |
| [6] | Three rows rebuilt from the log alone, ordered by rank. | the log |
| [7] | `emit_reorder` costs one event per moved row and nothing for a group that did not move. | formkit-ninja |

## The one property worth the example on its own

`ContentEvent` distinguishes a key that is **absent** from a key set to
**`None`**. `parent_submission=None` says "this row has no parent";
no `parent_submission` key says only that nobody said. A `TypedDict` cannot
express "present iff supplied", so nothing type-checks it — it has to survive
the round trip, and section [4] checks that rather than assuming it.

**This demo runs against the in-memory store, where the property holds
trivially** — that store keeps payloads as opaque bytes and never parses them,
so it could not collapse the two if it tried. The store that could is
`DjangoStreamStore`, which decodes into a `JSONField` and re-encodes on the way
out. That one is held by `TestAnAbsentKeyIsNotANullOne` in
`tests/test_django_rakaia/test_django_store.py`, whose tests were shown to fail
against a store that drops `None` values.

What the demo adds is the trap at the last step, which no store protects you
from:

```
said none     -> key present: 1       .get() returns: None
said nothing  -> key present: 0       .get() returns: None
```

`.get()` answers `None` for both, so a consumer reading with `.get` loses a
distinction the log faithfully kept. Read with `in`.

## What it found

`rank` is the decomposition's own vocabulary — an `Emission` carries it, and an
`emit_reorder` emission is *entirely* about it — but `ContentEvent` declares no
key for it, so every consumer has to mint one privately. That is the divergence
a shared TypedDict exists to prevent. `repeater_key` is in the same position, and
`ordinality` is not the same key under another name: it is the row's index within
the split, not its fractional position.

Section [7] asserts `"rank" not in CONTENT_EVENT_KEYS`, so this example goes red
when formkit-ninja closes the gap — which is the signal to delete the private
key from `PartisipaContentEvent` here and stop working around it.

## Reading further

* The seam's own account: `formkit_ninja/form_submission/emit.py` and
  `wire.py`, and `docs/public-api.md` in that repo for which tier each sits at.
* How a real consumer wires it: Partisipa's `docs/content/streams/adopting-rakaia.md`.
