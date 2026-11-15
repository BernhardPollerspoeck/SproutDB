# SproutDB - Claude Instructions

## Code Quality Rules

- **Keine null-forgiving operator (`!`)** - Immer saubere null-checks, guard clauses oder pattern matching statt `!`. Der `!` Operator versteckt Probleme statt sie zu lösen.
- Nullable reference types sind aktiviert (`<Nullable>enable</Nullable>`) - nutze sie korrekt.

## Automated Tests

- Unit and integration tests: `dotnet test SproutDB.slnx` (xUnit, `tests/SproutDB.Core.Tests`).
- Automated smoke and UI tests: `bash tests/e2e/run.sh` (also the command in `.connbridge/autotest.json`).
  - Builds `sandbox/SproutDB.Sandbox`, starts it on `http://127.0.0.1:5391` (override with `SPROUTDB_E2E_PORT`) with a fresh temporary data directory, runs the Playwright tests (headless Chromium) and stops the sandbox again. No Docker, no root.
  - Needs .NET 9 SDK and Node.js; the script runs `npm ci` and `npx playwright install chromium` itself.
  - `tests/e2e/tests/api.spec.ts`: smoke tests of `POST /sproutdb/query`. The other specs test the Admin UI at `/sproutdb/admin`.
  - Results in `tests/e2e/results`: `report.md`, screenshots grouped by feature (`query/`, `schema/`, `sidebar/`, `monitoring/`, `admin/`), and video + trace of failed tests under `test-output/`.
  - Extra Playwright arguments are passed through, e.g. `bash tests/e2e/run.sh tests/query.spec.ts`.
  - The Admin UI keeps query tabs on the server, shared by all browser sessions: a test must not rely on the tabs other tests left behind (open a new tab when it needs an empty one).
- **Every ticket that changes behaviour adds or updates automated tests.** A visible change in the Admin UI adds or updates a Playwright test in `tests/e2e/tests` that saves a screenshot of the changed page or state with `screenshot(page, '<feature>', '<n>-<state>')`; engine and API changes add xUnit tests (and an API smoke test where the HTTP behaviour changes).
