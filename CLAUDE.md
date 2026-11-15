# SproutDB - Claude Instructions

## Code Quality Rules

- **Keine null-forgiving operator (`!`)** - Immer saubere null-checks, guard clauses oder pattern matching statt `!`. Der `!` Operator versteckt Probleme statt sie zu lösen.
- Nullable reference types sind aktiviert (`<Nullable>enable</Nullable>`) - nutze sie korrekt.

## Automated Tests

`bash tests/run.sh` runs all automated tests (also the command in `.connbridge/autotest.json`). It builds `SproutDB.slnx` in Release and runs three suites one after the other; every suite runs even if an earlier one failed, and any failed test makes the exit code non-zero. No Docker, no root; needs .NET 9 SDK, Node.js and curl.

1. **Unit and integration tests** (xUnit, `tests/SproutDB.Core.Tests`): `dotnet test SproutDB.slnx`, the same as the CI. Alone: `dotnet test SproutDB.slnx`.
2. **End-to-end test client** (`sandbox/SproutDB.TestClient`) against the sandbox, started on `http://127.0.0.1:5392` (`SPROUTDB_CLIENT_PORT`): scenario `features`, then `stream` with a hard kill of the sandbox after 3 s, a restart on the same data and `verify` (WAL replay keeps every acknowledged write).
3. **HTTP smoke and Admin UI tests** (Playwright, headless Chromium, `tests/e2e`): starts the sandbox on `http://127.0.0.1:5391` (`SPROUTDB_E2E_PORT`) with a fresh data directory. `tests/e2e/tests/api.spec.ts` tests `POST /sproutdb/query`, the other specs the Admin UI at `/sproutdb/admin`. Alone, after a Release build of the sandbox: `cd tests/e2e && npm ci && npx playwright install chromium && npx playwright test` (a single file: `npx playwright test tests/query.spec.ts`).

Results in `tests/results`: `report.md` (passed, failed and skipped tests per suite), screenshots grouped by feature (`query/`, `schema/`, `sidebar/`, `monitoring/`, `admin/`), the TRX files under `unit/`, the client log under `client/`, video and trace of failed Playwright tests under `test-output/`.

The Admin UI keeps query tabs on the server, shared by all browser sessions: a test must not rely on the tabs other tests left behind (open a new tab when it needs an empty one).

**Every ticket that changes behaviour adds or updates tests:**
- **Unit tests**: every behaviour change in the engine, parser, storage, server or client adds or updates xUnit tests in `tests/SproutDB.Core.Tests`.
- **Automated tests with screenshots**: a visible change in the Admin UI adds or updates a Playwright test in `tests/e2e/tests` that saves a screenshot of the changed page or state with `screenshot(page, '<feature>', '<n>-<state>')`. One screenshot per page or state a person has to see, not one per click. A change of the HTTP behaviour adds or updates a smoke test in `tests/e2e/tests/api.spec.ts`.
