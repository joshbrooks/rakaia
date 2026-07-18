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

## Version pinning

The suite version is pinned in `package.json` (and `package-lock.json`) for
reproducibility. Bump `@durable-streams/server-conformance-tests` there and
re-run `npm install` to pick up a newer protocol revision.

## Status

The suite runs in CI as a **non-blocking / informational** check
(`.github/workflows/conformance.yml`) while rakaia's coverage matures. rakaia
currently passes the full protocol surface **except the stream forking family**
(`Stream-Forked-From` / `Stream-Fork-Offset` / `Stream-Fork-Sub-Offset`,
sub-offset prefix materialization, cascade GC, and fork TTL inheritance), which
is not yet implemented. See the tracking issue for details.
