#!/usr/bin/env bash
#
# Start the standalone rakaia server, run the Durable Streams conformance
# suite against it, then tear the server down. Exits with the suite's status.
#
# Usage:
#   conformance/run.sh [port]
#
# Requires: uv (with the Python env synced), node/npm. The JS dependencies are
# installed on first run if conformance/node_modules is missing.
set -uo pipefail

PORT="${1:-4437}"
BASE_URL="http://127.0.0.1:${PORT}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"

# Install the JS conformance dependencies if needed.
if [ ! -d "${HERE}/node_modules" ]; then
  echo "==> Installing conformance dependencies (npm ci)"
  (cd "${HERE}" && npm ci)
fi

# Start the standalone rakaia server in the background.
echo "==> Starting rakaia on ${BASE_URL}"
(
  cd "${ROOT}" &&
    exec uv run uvicorn rakaia:app --host 127.0.0.1 --port "${PORT}" --log-level warning
) &
SERVER_PID=$!

cleanup() {
  # Stop the uvicorn worker (child of the uv launcher) and the launcher
  # itself, addressed by PID so we never match an unrelated process. Avoid a
  # blocking `wait` so teardown can't hang the job if SIGTERM is slow.
  pkill -P "${SERVER_PID}" 2>/dev/null || true
  kill "${SERVER_PID}" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for readiness (a PUT creates a throwaway stream; 2xx means we're up).
echo "==> Waiting for the server to accept requests"
ready=""
for _ in $(seq 1 60); do
  if curl -fsS -o /dev/null -X PUT -H "content-type: text/plain" \
      "${BASE_URL}/__conformance_probe" 2>/dev/null; then
    ready="yes"
    break
  fi
  sleep 0.5
done
if [ -z "${ready}" ]; then
  echo "!! Server did not become ready on ${BASE_URL}" >&2
  exit 1
fi

# Run the conformance suite.
echo "==> Running Durable Streams conformance suite against ${BASE_URL}"
CONFORMANCE_TEST_URL="${BASE_URL}" npm --prefix "${HERE}" test
exit $?
