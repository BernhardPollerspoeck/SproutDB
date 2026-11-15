import { defineConfig, devices } from '@playwright/test';
import { mkdtempSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

// The Admin UI in an app that hosts SproutDB from the NuGet package (tests/package-app).
// Started by tests/run.sh after tests/package-app/build.sh, which passes the app's work directory.
const repoRoot = path.resolve(__dirname, '../..');
const resultsDir = process.env.SPROUTDB_E2E_RESULTS ?? path.join(repoRoot, 'tests/results');
const appDir = process.env.SPROUTDB_PACKAGE_APP_DIR;
if (!appDir) {
  throw new Error('SPROUTDB_PACKAGE_APP_DIR is not set: run bash tests/package-app/build.sh <dir> and pass <dir>.');
}

const developmentURL = `http://127.0.0.1:${process.env.SPROUTDB_PACKAGE_DEV_PORT ?? '5394'}`;
const productionURL = `http://127.0.0.1:${process.env.SPROUTDB_PACKAGE_PROD_PORT ?? '5395'}`;

// Every app starts on a fresh data directory; the config is loaded again in the worker,
// so the directories are created once and passed on in the environment.
const developmentData = (process.env.SPROUTDB_PACKAGE_DEV_DATA ??= mkdtempSync(path.join(os.tmpdir(), 'sproutdb-package-dev-')));
const productionData = (process.env.SPROUTDB_PACKAGE_PROD_DATA ??= mkdtempSync(path.join(os.tmpdir(), 'sproutdb-package-prod-')));

export default defineConfig({
  testDir: './package',
  outputDir: path.join(resultsDir, 'test-output', 'package'),
  workers: 1,
  fullyParallel: false,
  retries: 0,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: [['list'], ['json', { outputFile: path.join(resultsDir, 'package-results.json') }]],
  use: {
    viewport: { width: 1440, height: 900 },
    video: 'retain-on-failure',
    trace: 'retain-on-failure',
  },
  projects: [
    // The build output, as `dotnet run` starts it: static web assets come from the build manifest.
    { name: 'development', use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 900 }, baseURL: developmentURL } },
    // The published app: static web assets come from its wwwroot.
    { name: 'production', use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 900 }, baseURL: productionURL } },
  ],
  webServer: [
    {
      command: `dotnet "${path.join(appDir, 'build', 'PackageApp.dll')}" --urls=${developmentURL} --data="${developmentData}"`,
      cwd: path.join(appDir, 'app'),
      env: { ASPNETCORE_ENVIRONMENT: 'Development' },
      url: `${developmentURL}/sproutdb/admin`,
      reuseExistingServer: false,
      timeout: 120_000,
      stdout: 'ignore',
      stderr: 'pipe',
    },
    {
      command: `dotnet "${path.join(appDir, 'publish', 'PackageApp.dll')}" --urls=${productionURL} --data="${productionData}"`,
      cwd: path.join(appDir, 'publish'),
      env: { ASPNETCORE_ENVIRONMENT: 'Production' },
      url: `${productionURL}/sproutdb/admin`,
      reuseExistingServer: false,
      timeout: 120_000,
      stdout: 'ignore',
      stderr: 'pipe',
    },
  ],
});
