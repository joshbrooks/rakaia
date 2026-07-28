# Examples

Runnable demos in `examples/`. Each is assertion-backed: it prints what it proves
and fails loudly on a regression. Standalone demos need no database; Django demos
migrate themselves.

## Standalone (no Django)

* [protocol_streams](protocol-streams.md) - `StreamStore`, producer fencing, close, `poll` cursors.
* [multi_owner](multi-owner.md) - `Ref`/`RefResolver`, `reconcile_aggregate(owns=)`, `reconcile_by_key`.

## Headless event-sourcing (Django)

* [orders](orders.md) - versioned handlers, `effective_from/to`, upcasters, dry-run.
* [formkit_submissions](formkit-submissions.md) - `reconcile_children`, migration parity.
* [formkit_submissions (stream)](formkit-submission-stream.md) - `project_latest`, append log as source of truth.
* [projection_cookbook](projection-cookbook.md) - staged replay, reader, verification.
* [partisipa_history](partisipa-history.md) - pghistory-parity audit + recovery.
* [partisipa_staged](partisipa-staged.md) - staged replay for late-arriving links.
* [partisipa_close](partisipa-close.md) - close-precondition state machine.
* [partisipa_merge](partisipa-merge.md) - `merge_replay` deterministic order.
* [partisipa_repeaters](partisipa-repeaters.md) - nested-repeater tree reconcile.

## Live SSE (Django, browser)

* [chat](chat.md) - `@stream_model`, multi-stream events, live SSE.
* [polyglot](polyglot.md) - language-scoped streams, live-editable translations.
