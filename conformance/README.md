# Durable Streams conformance tests

This directory runs the upstream, language-agnostic protocol compliance suite —
[`@durable-streams/server-conformance-tests`](https://github.com/durable-streams/durable-streams/tree/main/packages/server-conformance-tests)
— against a running rakaia server. It gives us an independent signal that
rakaia implements the [Durable Streams protocol](../docs/protocol.md) correctly,
separate from rakaia's own pytest suite.

## Running locally

From the repo root:

```bash
just conformance          # starts rakaia on :4437, runs the suite, tears it down
just conformance 4500     # use a different port
```

Or drive it directly:

```bash
conformance/run.sh 4437
```

To run against a server you started yourself:

```bash
cd conformance
npm ci
CONFORMANCE_TEST_URL=http://127.0.0.1:4437 npm test
```

## Regression detection (expected-failures baseline)

Because rakaia doesn't yet pass the whole protocol surface, a raw pass/fail
count can't tell a real regression apart from the known gap. So the run diffs
results against a committed baseline of expected failures:

- `expected-failures.txt` — one vitest `fullName` per line for every test rakaia
  is known not to pass yet (currently the entire stream-forking family). `#`
  comments and blank lines are ignored.
- `check-regressions.mjs` — reads the suite's JSON report
  (`conformance-results.json`, written by `npm run test:ci`) and the baseline,
  then reports three sets:
  - **NEW failures** (failed, not in the baseline) → regressions. In CI these
    become `::error::` annotations and a step-summary section.
  - **Newly passing** (in the baseline, now passing) → remove them from
    `expected-failures.txt` to shrink the gap.
  - **Expected** (failed, in the baseline) → the known gap; kept quiet.

`conformance/run.sh` (and `just conformance`) runs the suite via `test:ci` and
then `check-regressions.mjs` automatically. The check **exits 0** so the job
stays informational; set `CONFORMANCE_FAIL_ON_REGRESSION=1` to make a regression
exit non-zero.

**Regenerating the baseline** (after fork lands, or a suite-version bump):

```bash
just conformance-baseline    # runs the suite, then rewrites expected-failures.txt
git diff conformance/expected-failures.txt   # review before committing
```

## Version pinning

The suite version is pinned in `package.json` (and `package-lock.json`) for
reproducibility. Dependabot (`.github/dependabot.yml`) opens PRs when a newer
`@durable-streams/server-conformance-tests` is released; on such a PR the
regression check shows any newly-added protocol tests as NEW failures, so you
can see exactly what the newer revision requires. You can also bump it manually
and re-run `npm install`.

## Status

The suite runs in CI as a **non-blocking / informational** check
(`.github/workflows/conformance.yml`) while rakaia's coverage matures. rakaia
currently passes the full protocol surface **except the stream forking family**
(`Stream-Forked-From` / `Stream-Fork-Offset` / `Stream-Fork-Sub-Offset`,
sub-offset prefix materialization, cascade GC, and fork TTL inheritance), which
is not yet implemented. See the tracking issue for details.
