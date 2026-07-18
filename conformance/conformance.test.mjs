// Vitest entry point for the Durable Streams server conformance suite.
//
// The upstream package registers its `describe`/`test` blocks when
// `runConformanceTests()` is called, so this file simply invokes it against
// the server URL provided via CONFORMANCE_TEST_URL. Point it at a running
// rakaia instance:
//
//   CONFORMANCE_TEST_URL=http://127.0.0.1:4437 npm test
//
// or use `../conformance/run.sh` / `just conformance`, which start the server
// for you.

import { runConformanceTests } from "@durable-streams/server-conformance-tests"

const baseUrl = process.env.CONFORMANCE_TEST_URL
if (!baseUrl) {
  throw new Error(
    "CONFORMANCE_TEST_URL is required (e.g. http://127.0.0.1:4437). " +
      "Use `just conformance` or conformance/run.sh to start the server automatically.",
  )
}

runConformanceTests({ baseUrl })
