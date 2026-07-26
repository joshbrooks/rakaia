#!/usr/bin/env node
// Compare a vitest JSON run against the committed expected-failures baseline and
// report regressions in a way that's loud on a PR but does NOT block by default.
//
// The Durable Streams conformance job is informational: rakaia does not yet pass
// the full protocol surface (the stream-forking family is unimplemented). A plain
// pass/fail count therefore can't tell a genuine regression apart from that known
// gap. This script makes the distinction explicit:
//
//   NEW failures  — failed, and NOT in the baseline  → regressions (loud)
//   Newly passing — in the baseline, but now passing → shrink the baseline
//   Expected      — failed, and in the baseline      → the known gap (quiet)
//
// Output: a human summary on stdout, GitHub `::error::`/`::warning::` workflow
// annotations, and (when $GITHUB_STEP_SUMMARY is set) a Markdown step summary.
//
// Exit code: 0 by default so the job stays non-blocking. Set
// CONFORMANCE_FAIL_ON_REGRESSION=1 to exit non-zero when regressions are found
// (one-line switch to make regressions a required check later).
//
// Usage:
//   node check-regressions.mjs [results.json] [expected-failures.txt]

import { readFileSync } from "node:fs"
import { resolve, dirname } from "node:path"
import { fileURLToPath } from "node:url"

const here = dirname(fileURLToPath(import.meta.url))
const resultsPath = resolve(process.argv[2] ?? resolve(here, "conformance-results.json"))
const baselinePath = resolve(process.argv[3] ?? resolve(here, "expected-failures.txt"))

function fail(msg) {
  console.error(`check-regressions: ${msg}`)
  process.exit(2)
}

let report
try {
  report = JSON.parse(readFileSync(resultsPath, "utf8"))
} catch (err) {
  fail(`could not read vitest JSON results at ${resultsPath}: ${err.message}`)
}

let baseline
try {
  baseline = new Set(
    readFileSync(baselinePath, "utf8")
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => l && !l.startsWith("#")),
  )
} catch (err) {
  fail(`could not read baseline at ${baselinePath}: ${err.message}`)
}

// Collect statuses keyed by the stable vitest `fullName`.
const failed = new Set()
const passed = new Set()
for (const file of report.testResults ?? []) {
  for (const a of file.assertionResults ?? []) {
    if (a.status === "failed") failed.add(a.fullName)
    else if (a.status === "passed") passed.add(a.fullName)
  }
}

// `--write-baseline`: overwrite expected-failures.txt with the current failures.
// Run after the suite when the accepted gap changes (e.g. fork lands, or the
// suite version bumps). Review the diff before committing.
if (process.argv.includes("--write-baseline")) {
  const { writeFileSync } = await import("node:fs")
  const header = [
    "# Durable Streams conformance — expected failures (baseline)",
    "#",
    "# One vitest `fullName` (ancestorTitles + title) per line for each test that",
    "# rakaia is KNOWN not to pass yet. check-regressions.mjs treats these as the",
    "# accepted protocol gap: they do NOT count as regressions. Anything failing",
    "# that is NOT listed here is a NEW regression; anything listed here that now",
    "# PASSES should be removed to shrink the baseline.",
    "#",
    "# Current gap: the entire stream-forking family (not yet implemented).",
    "# Regenerate with: just conformance-baseline   (see conformance/README.md)",
    "# Lines starting with # and blank lines are ignored.",
    "#",
  ]
  const body = [...failed].sort()
  writeFileSync(baselinePath, header.concat(body).join("\n") + "\n")
  console.log(`Wrote ${body.length} expected-failure entries to ${baselinePath}`)
  process.exit(0)
}

const regressions = [...failed].filter((n) => !baseline.has(n)).sort()
const nowPassing = [...baseline].filter((n) => passed.has(n)).sort()
const stillExpected = [...failed].filter((n) => baseline.has(n)).sort()
// Baseline entries that neither failed nor passed this run (renamed/removed upstream).
const staleBaseline = [...baseline]
  .filter((n) => !failed.has(n) && !passed.has(n))
  .sort()

// --- GitHub workflow annotations (show inline on the PR checks) ---
const isCI = Boolean(process.env.GITHUB_ACTIONS)
if (isCI) {
  for (const n of regressions) {
    console.log(`::error title=Conformance regression::${n}`)
  }
  for (const n of nowPassing) {
    console.log(`::warning title=Conformance test now passing (shrink baseline)::${n}`)
  }
  if (staleBaseline.length) {
    console.log(
      `::warning title=Stale baseline entries (not in this run)::${staleBaseline.length} baselined test(s) were neither run nor found — the suite version may have changed`,
    )
  }
}

// --- Human-readable console summary ---
const totals = {
  total: report.numTotalTests ?? passed.size + failed.size,
  passed: report.numPassedTests ?? passed.size,
  failed: report.numFailedTests ?? failed.size,
  skipped: report.numPendingTests ?? 0,
}
console.log("")
console.log("Durable Streams conformance — regression check")
console.log(
  `  ${totals.passed} passed, ${totals.failed} failed, ${totals.skipped} skipped (of ${totals.total})`,
)
console.log(
  `  ${regressions.length} NEW failure(s) · ${nowPassing.length} newly passing · ${stillExpected.length} expected (known gap)`,
)
if (regressions.length) {
  console.log("\n  NEW failures (regressions):")
  for (const n of regressions) console.log(`    ✗ ${n}`)
}
if (nowPassing.length) {
  console.log("\n  Now passing — remove from expected-failures.txt:")
  for (const n of nowPassing) console.log(`    ✓ ${n}`)
}
if (staleBaseline.length) {
  console.log("\n  Stale baseline entries (not seen this run):")
  for (const n of staleBaseline) console.log(`    ? ${n}`)
}

// --- GitHub step summary (Markdown) ---
const summaryPath = process.env.GITHUB_STEP_SUMMARY
if (summaryPath) {
  const lines = []
  const verdict = regressions.length
    ? "🔴 **Regressions detected**"
    : nowPassing.length
      ? "🟡 **No regressions — baseline can shrink**"
      : "🟢 **No regressions**"
  lines.push("### Durable Streams conformance results", "")
  lines.push(verdict, "")
  lines.push(
    `| Passed | Failed | Skipped | New failures | Newly passing | Expected gap |`,
    `| ---: | ---: | ---: | ---: | ---: | ---: |`,
    `| ${totals.passed} | ${totals.failed} | ${totals.skipped} | ${regressions.length} | ${nowPassing.length} | ${stillExpected.length} |`,
    "",
  )
  const list = (title, items, mark) => {
    if (!items.length) return
    lines.push(`<details><summary>${title} (${items.length})</summary>`, "")
    for (const n of items) lines.push(`- ${mark} \`${n}\``)
    lines.push("", "</details>", "")
  }
  list("NEW failures (regressions)", regressions, "✗")
  list("Now passing — shrink the baseline", nowPassing, "✓")
  list("Stale baseline entries (not seen this run)", staleBaseline, "?")
  lines.push(
    "",
    "_Informational check. Expected failures are the unimplemented stream-forking family; see `conformance/expected-failures.txt`._",
  )
  try {
    // eslint-disable-next-line no-undef
    ;(await import("node:fs")).appendFileSync(summaryPath, lines.join("\n") + "\n")
  } catch (err) {
    console.error(`check-regressions: could not write step summary: ${err.message}`)
  }
}

const failOnRegression = process.env.CONFORMANCE_FAIL_ON_REGRESSION === "1"
if (regressions.length && failOnRegression) {
  console.log(
    `\nExiting non-zero: ${regressions.length} regression(s) and CONFORMANCE_FAIL_ON_REGRESSION=1.`,
  )
  process.exit(1)
}
process.exit(0)
