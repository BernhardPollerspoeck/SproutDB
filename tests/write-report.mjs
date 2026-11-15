// Writes tests/results/report.md: per suite the passed, failed and skipped tests, then the failures.
// Usage: node write-report.mjs <resultsDir> <unitExitCode> <clientExitCode> <e2eExitCode> <packageExitCode>
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const [, , resultsDir, unitExit, clientExit, e2eExit, packageExit] = process.argv;
const suites = [];
const stripAnsi = (s) => s.replace(/\u001b\[[0-9;]*m/g, '');
const cell = (s) => s.replaceAll('|', '\\|');

// 1. xUnit: the TRX files of dotnet test
{
  const dir = path.join(resultsDir, 'unit');
  const counts = { passed: 0, failed: 0, skipped: 0 };
  const failures = [];
  const files = existsSync(dir) ? readdirSync(dir).filter((f) => f.endsWith('.trx')) : [];
  for (const file of files) {
    const trx = readFileSync(path.join(dir, file), 'utf8');
    for (const m of trx.matchAll(/<UnitTestResult\b[^>]*>/g)) {
      const outcome = /outcome="([^"]*)"/.exec(m[0])?.[1] ?? '';
      const name = /testName="([^"]*)"/.exec(m[0])?.[1] ?? '';
      if (outcome === 'Passed') counts.passed++;
      else if (outcome === 'NotExecuted') counts.skipped++;
      else {
        counts.failed++;
        failures.push(name);
      }
    }
  }
  suites.push({ name: 'Unit and integration tests (xUnit, dotnet test)', exit: unitExit, ...counts, failures });
}

// 2. End-to-end test client: PASS / FAIL lines of its output
{
  const file = path.join(resultsDir, 'client', 'output.txt');
  const lines = existsSync(file) ? readFileSync(file, 'utf8').split('\n') : [];
  const failures = lines.filter((l) => l.startsWith('FAIL')).map((l) => l.slice(4).trim());
  const passed = lines.filter((l) => l.startsWith('PASS')).length;
  suites.push({ name: 'End-to-end test client (sandbox/SproutDB.TestClient)', exit: clientExit, passed, failed: failures.length, skipped: 0, failures });
}

// 3. and 4. Playwright: their JSON reports
const playwrightSuite = (name, exit, reportFile) => {
  const file = path.join(resultsDir, reportFile);
  const counts = { passed: 0, failed: 0, skipped: 0 };
  const failures = [];
  const walk = (suite, titles) => {
    const names = suite.title ? [...titles, suite.title] : titles;
    for (const spec of suite.specs ?? []) {
      for (const t of spec.tests ?? []) {
        const result = t.results?.[t.results.length - 1];
        const status = result?.status ?? 'skipped';
        const title = [...names.slice(1), spec.title].join(' › ');
        if (status === 'passed') counts.passed++;
        else if (status === 'skipped') counts.skipped++;
        else {
          counts.failed++;
          failures.push(`${title}: ${stripAnsi(result?.error?.message ?? status).split('\n')[0]}`);
        }
      }
    }
    for (const child of suite.suites ?? []) walk(child, names);
  };
  if (existsSync(file)) {
    for (const suite of JSON.parse(readFileSync(file, 'utf8')).suites ?? []) walk(suite, []);
  }
  return { name, exit, ...counts, failures };
};
suites.push(playwrightSuite('HTTP smoke and Admin UI tests (Playwright)', e2eExit, 'results.json'));

// 4. The app on the SproutDB package: without a Playwright report, packing or building failed.
{
  const suite = playwrightSuite('Admin UI in an app on the SproutDB package (Playwright)', packageExit, 'package-results.json');
  if (packageExit !== '0' && !existsSync(path.join(resultsDir, 'package-results.json'))) {
    suite.failures.push('packing SproutDB or building the app failed, see `package/build.log`');
  }
  suites.push(suite);
}

const result = (s) => (s.exit === '0' ? 'passed' : s.failed > 0 ? 'failed' : `failed (exit code ${s.exit})`);
const lines = [
  '# SproutDB automated tests',
  '',
  `Overall: ${suites.every((s) => s.exit === '0') ? 'passed' : 'failed'}`,
  '',
  '| Suite | Passed | Failed | Skipped | Result |',
  '| --- | ---: | ---: | ---: | --- |',
  ...suites.map((s) => `| ${s.name} | ${s.passed} | ${s.failed} | ${s.skipped} | ${result(s)} |`),
  '',
];
for (const s of suites.filter((x) => x.failures.length > 0)) {
  lines.push(`## Failed: ${s.name}`, '', ...s.failures.map((f) => `- ${cell(f)}`), '');
}
lines.push(
  'Screenshots are grouped by feature in the subfolders of this folder. Videos and traces of failed',
  'Playwright tests are under `test-output`, the TRX files under `unit`, the client log under `client`,',
  'the build log of the app on the SproutDB package under `package`.',
  '',
);
writeFileSync(path.join(resultsDir, 'report.md'), lines.join('\n'));
