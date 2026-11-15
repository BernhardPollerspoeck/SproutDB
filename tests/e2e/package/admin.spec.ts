import { expect, test } from '@playwright/test';
import { openAdmin, screenshot } from '../tests/helpers';

// The app of tests/package-app hosts SproutDB from the NuGet package and has no .razor files
// of its own: the package has to bring the Blazor script along. Runs once per project:
// "development" (build output) and "production" (published app).
test.describe('Package app: Admin UI', () => {
  test('serves the Blazor script and the admin CSS and JS', async ({ request }) => {
    const assets = [
      ['/_framework/blazor.web.js', 'javascript'],
      ['/_content/SproutDB/css/sproutdb-admin.css', 'text/css'],
      ['/_content/SproutDB/js/sproutdb-autocomplete.js', 'javascript'],
    ];
    for (const [url, contentType] of assets) {
      const response = await request.get(url);
      expect(response.status(), url).toBe(200);
      expect(response.headers()['content-type'], url).toContain(contentType);
    }
  });

  test('the Admin UI reacts to clicks', async ({ page, request }, testInfo) => {
    // The app starts empty (auth off): a database with one table over the HTTP API.
    for (const query of [
      'create database',
      'create table products (name string 100, price double)',
      "upsert products {name: 'Rake', price: 12.5}",
    ]) {
      const response = await request.post('/sproutdb/query', {
        headers: { 'X-SproutDB-Database': 'shop', 'Content-Type': 'text/plain' },
        data: query,
      });
      expect(response.status(), query).toBe(200);
      expect((await response.json())[0].errors ?? null, query).toBeNull();
    }

    await openAdmin(page, '/sproutdb/admin');
    await page.locator('.tree-children .tree-item', { hasText: /^\W*products$/ }).click();
    await expect(page.locator('textarea.query-editor')).toHaveValue('get products');
    await expect(page.locator('.query-db-select')).toHaveValue('shop');
    await page.getByRole('button', { name: 'Run' }).first().click();
    await expect(page.locator('.results-table tbody tr')).toHaveCount(1);
    await expect(page.locator('.results-table')).toContainText('Rake');
    await screenshot(page, 'package', `1-admin-${testInfo.project.name}`);
  });
});
