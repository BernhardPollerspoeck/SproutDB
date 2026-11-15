import { expect, test } from '@playwright/test';
import { openAdmin, screenshot } from './helpers';

test.describe('Admin UI: Schema', () => {
  test('shows the tables of the garden database', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin/schema');
    await expect(page.getByText('Select a database')).toBeVisible();
    await page.locator('.schema-toolbar .query-db-select').selectOption('garden');
    await expect(page.locator('.schema-table-name', { hasText: /^plants$/ })).toBeVisible();
  });

  test('expands a table and lists its columns', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin/schema');
    await page.locator('.schema-toolbar .query-db-select').selectOption('garden');
    await page.locator('.schema-table-header').filter({ has: page.locator('.schema-table-name', { hasText: /^plants$/ }) }).click();
    await expect(page.locator('.schema-columns').first()).toContainText('species');
    await screenshot(page, 'schema', '1-table-columns');
  });
});

test.describe('Admin UI: Monitoring', () => {
  test('shows the overview cards', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin/monitoring');
    await expect(page.locator('.topbar-nav a.active')).toHaveText('Monitoring');
    await expect(page.locator('.monitoring-cards .metric-label', { hasText: 'Total Reads' })).toBeVisible();
    await screenshot(page, 'monitoring', '1-overview');
  });
});

test.describe('Admin UI: Administration', () => {
  test('shows the action cards', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin/settings');
    await expect(page.locator('.admin-header')).toHaveText('Administration');
    await expect(page.locator('.admin-card')).toHaveCount(6);
    await screenshot(page, 'admin', '1-cards');
  });

  test('opens the export panel', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin/settings');
    await page.locator('.admin-card', { hasText: 'Export Database' }).click();
    await expect(page.locator('.admin-panel-header')).toContainText('Export Database');
  });
});
