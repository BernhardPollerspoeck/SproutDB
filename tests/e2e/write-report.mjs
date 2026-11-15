// Turns Playwright's JSON report into a short Markdown summary (results/report.md).
import { readFileSync, writeFileSync } from 'node:fs';

const [, , jsonPath, mdPath] = process.argv;
const report = JSON.parse(readFileSync(jsonPath, 'utf8'));
const rows = [];

function walk(suite, titles) {
  const path = suite.title ? [...titles, suite.title] : titles;
  for (const spec of suite.specs ?? []) {
    for (const t of spec.tests ?? []) {
      const result = t.results?.[t.results.length - 1];
      const status = result?.status ?? 'skipped';
      // Strip the terminal colour codes Playwright puts into error messages.
      const error = (result?.error?.message ?? '').replace(/\u001b\[[0-9;]*m/g, '').split('\n')[0];
      rows.push({ title: [...path.slice(1), spec.title].join(' › '), status, ms: result?.duration ?? 0, error });
    }
  }
  for (const child of suite.suites ?? []) walk(child, path);
}
for (const suite of report.suites ?? []) walk(suite, []);

const passed = rows.filter((r) => r.status === 'passed').length;
const failed = rows.filter((r) => r.status === 'failed' || r.status === 'timedOut').length;
const lines = [
  '# SproutDB automated tests',
  '',
  `${passed} passed, ${failed} failed, ${rows.length - passed - failed} other, of ${rows.length} tests.`,
  '',
  '| Test | Result | Time |',
  '| --- | --- | --- |',
  ...rows.map((r) => `| ${r.title.replaceAll('|', '\\|')} | ${r.status}${r.error ? `: ${r.error.replaceAll('|', '\\|')}` : ''} | ${(r.ms / 1000).toFixed(1)} s |`),
  '',
  'Screenshots are grouped by feature in the subfolders of this folder; videos and traces of failed tests are under `test-output`.',
  '',
];
writeFileSync(mdPath, lines.join('\n'));
