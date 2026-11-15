#!/usr/bin/env bash
# Runs the automated tests of SproutDB: builds the sandbox, starts it with a fresh
# data directory (Playwright's webServer), runs the HTTP smoke tests and the Admin UI
# tests in headless Chromium and stops the sandbox again.
# Results (screenshots, report.md, videos and traces of failed tests): tests/e2e/results
set -euo pipefail

cd "$(dirname "$0")"
E2E_DIR="$(pwd)"
REPO_ROOT="$(cd ../.. && pwd)"

export SPROUTDB_E2E_RESULTS="$E2E_DIR/results"
export SPROUTDB_E2E_PORT="${SPROUTDB_E2E_PORT:-5391}"
export SPROUTDB_E2E_DATA="$(mktemp -d)"
trap 'rm -rf "$SPROUTDB_E2E_DATA"' EXIT

rm -rf "$SPROUTDB_E2E_RESULTS"
mkdir -p "$SPROUTDB_E2E_RESULTS"

dotnet build "$REPO_ROOT/sandbox/SproutDB.Sandbox/SproutDB.Sandbox.csproj" --configuration Release --nologo -v q

npm ci --no-audit --no-fund
npx playwright install chromium

status=0
npx playwright test "$@" || status=$?

node "$E2E_DIR/write-report.mjs" "$SPROUTDB_E2E_RESULTS/results.json" "$SPROUTDB_E2E_RESULTS/report.md" || true
exit $status
