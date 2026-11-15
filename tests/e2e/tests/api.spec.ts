import { expect, test } from '@playwright/test';
import { masterKey } from './helpers';

// Smoke tests of the HTTP API (POST /sproutdb/query) against the started sandbox.
test.describe('HTTP API', () => {
  test('answers a get query on the seeded garden database', async ({ request }) => {
    const response = await request.post('/sproutdb/query', {
      headers: { 'X-SproutDB-ApiKey': masterKey, 'X-SproutDB-Database': 'garden', 'Content-Type': 'text/plain' },
      data: 'get plants select name, category order by name limit 3',
    });
    expect(response.status()).toBe(200);
    const body = await response.json();
    expect(body).toHaveLength(1);
    expect(body[0].errors ?? null).toBeNull();
    expect(body[0].data).toHaveLength(3);
    expect(body[0].data[0]).toHaveProperty('name');
  });

  test('binds JSON parameters', async ({ request }) => {
    const response = await request.post('/sproutdb/query', {
      headers: { 'X-SproutDB-ApiKey': masterKey, 'X-SproutDB-Database': 'garden' },
      data: { query: 'get plants select name where category = @category', parameters: { category: 'herb' } },
    });
    expect(response.status()).toBe(200);
    const body = await response.json();
    expect(body[0].errors ?? null).toBeNull();
    expect(body[0].data.length).toBeGreaterThan(0);
  });

  test('rejects a query without API key', async ({ request }) => {
    const response = await request.post('/sproutdb/query', {
      headers: { 'X-SproutDB-Database': 'garden', 'Content-Type': 'text/plain' },
      data: 'get plants',
    });
    expect(response.status()).toBe(401);
    const body = await response.json();
    expect(body[0].errors[0].code).toBe('AUTH_REQUIRED');
  });

  test('reports a parse error', async ({ request }) => {
    const response = await request.post('/sproutdb/query', {
      headers: { 'X-SproutDB-ApiKey': masterKey, 'X-SproutDB-Database': 'garden', 'Content-Type': 'text/plain' },
      data: 'get plants wher name = 1',
    });
    expect(response.status()).toBe(200);
    const body = await response.json();
    expect(body[0].errors.length).toBeGreaterThan(0);
  });
});
