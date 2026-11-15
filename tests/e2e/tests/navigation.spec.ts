import { expect, test } from '@playwright/test';
import { openAdmin, screenshot } from './helpers';

// The sidebar is part of every Admin UI screenshot; only the filtered state gets its own.

test.describe('Admin UI: Sidebar', () => {
  test('lists the tables of the garden database', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin');
    const garden = page.locator('.sidebar-tree > .tree-item', { hasText: 'garden' });
    await expect(garden).toHaveClass(/expanded/);
    await expect(page.locator('.tree-children .tree-item', { hasText: /^\W*plants$/ })).toBeVisible();
  });

  test('opens a table in the query editor', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin');
    await page.locator('.tree-children .tree-item', { hasText: /^\W*suppliers$/ }).click();
    await expect(page.locator('textarea.query-editor')).toHaveValue('get suppliers');
    await expect(page.locator('.query-db-select')).toHaveValue('garden');
  });

  test('filters the tables', async ({ page }) => {
    await openAdmin(page, '/sproutdb/admin');
    await page.getByPlaceholder('Filter tables...').fill('order');
    await expect(page.locator('.tree-children .tree-item')).toHaveCount(2);
    await expect(page.locator('.tree-children')).toContainText('orders');
    await expect(page.locator('.tree-children')).toContainText('order_items');
    await screenshot(page, 'sidebar', '1-filtered');
  });
});
