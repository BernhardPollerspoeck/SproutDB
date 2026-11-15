#!/usr/bin/env bash
# Runs all automated tests of SproutDB, one suite after the other:
#   1. unit     xUnit unit and integration tests (dotnet test SproutDB.slnx, as the CI does)
#   2. client   the end-to-end test client (sandbox/SproutDB.TestClient) against a started sandbox:
#               "features", then "stream" + hard kill of the sandbox + restart + "verify" (WAL replay)
#   3. e2e      Playwright: HTTP smoke tests and Admin UI tests in headless Chromium (tests/e2e)
# Every suite runs even if an earlier one failed; any failure makes the exit code non-zero.
# Results: tests/results (report.md, screenshots by feature, TRX, client logs, videos/traces of failures)
set -uo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$(pwd)"
RESULTS="$REPO_ROOT/tests/results"
SANDBOX_DIR="$REPO_ROOT/sandbox/SproutDB.Sandbox"
CLIENT_PORT="${SPROUTDB_CLIENT_PORT:-5392}"
CLIENT_URL="http://127.0.0.1:$CLIENT_PORT"

WORK="$(mktemp -d)"
SANDBOX_PID=""
cleanup() {
  if [ -n "$SANDBOX_PID" ]; then kill "$SANDBOX_PID" 2>/dev/null; wait "$SANDBOX_PID" 2>/dev/null; fi
  rm -rf "$WORK"
}
trap cleanup EXIT

rm -rf "$RESULTS"
mkdir -p "$RESULTS/unit" "$RESULTS/client"

echo "== Build"
if ! dotnet build SproutDB.slnx --configuration Release --nologo -v q; then
  echo "# SproutDB automated tests" > "$RESULTS/report.md"
  echo "" >> "$RESULTS/report.md"
  echo "The build failed, no tests ran." >> "$RESULTS/report.md"
  exit 1
fi
# The build output paths come from MSBuild, so a change of the target framework does not break them.
target_path() { dotnet msbuild "$1" -getProperty:TargetPath -p:Configuration=Release; }
SANDBOX_DLL="$(target_path "$SANDBOX_DIR/SproutDB.Sandbox.csproj")"
CLIENT_DLL="$(target_path "$REPO_ROOT/sandbox/SproutDB.TestClient/SproutDB.TestClient.csproj")"
export SPROUTDB_SANDBOX_DLL="$SANDBOX_DLL"

echo "== Suite unit: dotnet test"
unit_status=0
dotnet test SproutDB.slnx --configuration Release --no-build --logger trx --results-directory "$RESULTS/unit" || unit_status=$?

echo "== Suite client: end-to-end test client against the sandbox"
# Starts the sandbox on a data directory; a long flush interval leaves the writes in the WAL,
# so a hard kill is followed by a WAL replay on the next start.
start_sandbox() {
  (cd "$SANDBOX_DIR" && exec env ASPNETCORE_ENVIRONMENT=Development dotnet "$SANDBOX_DLL" \
    --urls="$CLIENT_URL" --data="$1" --flushSeconds=3600) >> "$RESULTS/client/sandbox.log" 2>&1 &
  SANDBOX_PID=$!
  for _ in $(seq 1 60); do
    curl -sf -o /dev/null "$CLIENT_URL/sproutdb/admin" && return 0
    kill -0 "$SANDBOX_PID" 2>/dev/null || break
    sleep 1
  done
  echo "sandbox did not start on $CLIENT_URL" | tee -a "$RESULTS/client/output.txt"
  return 1
}
stop_sandbox() {
  kill "-${1:-TERM}" "$SANDBOX_PID" 2>/dev/null
  wait "$SANDBOX_PID" 2>/dev/null
  SANDBOX_PID=""
}
client_status=0
run_client() {
  local data="$WORK/client-data" acked="$WORK/acked.txt"
  start_sandbox "$data" || return 1
  echo "-- features" | tee -a "$RESULTS/client/output.txt"
  dotnet "$CLIENT_DLL" features --url="$CLIENT_URL/" 2>&1 | tee -a "$RESULTS/client/output.txt" || client_status=1
  stop_sandbox

  rm -rf "$data"
  start_sandbox "$data" || return 1
  echo "-- stream, kill -9 after 3 s, restart, verify" | tee -a "$RESULTS/client/output.txt"
  dotnet "$CLIENT_DLL" stream --url="$CLIENT_URL/" --acked="$acked" >> "$RESULTS/client/output.txt" 2>&1 &
  local stream_pid=$!
  sleep 3
  stop_sandbox KILL
  wait "$stream_pid" || client_status=1
  start_sandbox "$data" || return 1
  dotnet "$CLIENT_DLL" verify --url="$CLIENT_URL/" --acked="$acked" 2>&1 | tee -a "$RESULTS/client/output.txt" || client_status=1
  stop_sandbox
}
run_client || client_status=1

echo "== Suite e2e: Playwright"
e2e_status=0
(
  cd tests/e2e &&
  npm ci --no-audit --no-fund &&
  npx playwright install chromium &&
  SPROUTDB_E2E_RESULTS="$RESULTS" SPROUTDB_E2E_DATA="$WORK/e2e-data" npx playwright test
) || e2e_status=$?

node tests/write-report.mjs "$RESULTS" "$unit_status" "$client_status" "$e2e_status" || true
cat "$RESULTS/report.md"

[ "$unit_status" -eq 0 ] && [ "$client_status" -eq 0 ] && [ "$e2e_status" -eq 0 ]
