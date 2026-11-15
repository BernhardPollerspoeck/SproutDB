import { defineConfig, devices } from '@playwright/test';
import { mkdtempSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// Started by tests/run.sh, which builds the sandbox first and passes the results and data directories.
const repoRoot = path.resolve(__dirname, '../..');
const port = process.env.SPROUTDB_E2E_PORT ?? '5391';
const baseURL = `http://127.0.0.1:${port}`;
const resultsDir = process.env.SPROUTDB_E2E_RESULTS ?? path.join(repoRoot, 'tests/results');
// The sandbox seeds its garden database on a fresh data directory.
const dataDir = process.env.SPROUTDB_E2E_DATA ?? mkdtempSync(path.join(os.tmpdir(), 'sproutdb-e2e-'));

export default defineConfig({
  testDir: './tests',
  outputDir: path.join(resultsDir, 'test-output'),
  // One sandbox instance with shared state: run the tests one after another.
  workers: 1,
  fullyParallel: false,
  retries: 0,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: [['list'], ['json', { outputFile: path.join(resultsDir, 'results.json') }]],
  use: {
    baseURL,
    viewport: { width: 1440, height: 900 },
    video: 'retain-on-failure',
    trace: 'retain-on-failure',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 900 } } }],
  webServer: {
    command: `dotnet "${path.join(repoRoot, 'sandbox/SproutDB.Sandbox/bin/Release/net9.0/SproutDB.Sandbox.dll')}" --urls=${baseURL} --data="${dataDir}"`,
    cwd: path.join(repoRoot, 'sandbox/SproutDB.Sandbox'),
    // Development serves the Admin UI's static web assets (CSS, JS) from the build output.
    env: { ASPNETCORE_ENVIRONMENT: 'Development' },
    url: `${baseURL}/sproutdb/admin`,
    reuseExistingServer: false,
    timeout: 120_000,
    stdout: 'ignore',
    stderr: 'pipe',
  },
});
