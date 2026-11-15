import { expect, test } from '@playwright/test';
import { openAdmin, screenshot } from './helpers';

test.describe('Admin UI: Query', () => {
  // The query tabs live on the server and are shared by all browser sessions,
  // so earlier tests may have left queries behind: a new tab starts empty.
  test('shows an empty query tab', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin');
    await expect(page.locator('.topbar-nav a.active')).toHaveText('Query');
    await page.getByTitle('New tab', { exact: true }).click();
    await expect(page.locator('textarea.query-editor')).toHaveValue('');
    await expect(page.getByText('Run a query to see results')).toBeVisible();
    await screenshot(page, 'query', '1-empty-tab');
  });

  test('runs a query and shows the results table', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin');
    await page.locator('.query-db-select').selectOption('garden');
    await page.locator('textarea.query-editor').fill('get plants select name, species, category, price order by name limit 10');
    await page.getByRole('button', { name: 'Run' }).first().click();
    await expect(page.locator('.results-table tbody tr')).toHaveCount(10);
    await expect(page.locator('.query-stats .stat-ok')).toBeVisible();
    await screenshot(page, 'query', '2-results');
  });

  test('shows the error panel for an invalid query', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin');
    await page.locator('.query-db-select').selectOption('garden');
    await page.locator('textarea.query-editor').fill('get plants select nam');
    await page.getByRole('button', { name: 'Run' }).first().click();
    await expect(page.locator('.error-panel')).toBeVisible();
    await expect(page.locator('.error-panel .error-code').first()).toHaveText('UNKNOWN_COLUMN');
    await screenshot(page, 'query', '3-error');
  });
});
