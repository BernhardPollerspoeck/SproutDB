import { expect, type Page } from '@playwright/test';
import path from 'node:path';

const resultsDir = process.env.SPROUTDB_E2E_RESULTS ?? path.join(__dirname, '../../results');

/** The sandbox's master key (sandbox/SproutDB.Sandbox/Program.cs). */
export const masterKey = 'sdb_ak_1234';

/**
 * Opens an Admin UI page and waits until the Blazor circuit is interactive.
 * The layout asks for the theme over JS interop on its first interactive render,
 * so that call marks the moment clicks and inputs reach the server.
 */
export async function openAdmin(page: Page, url: string): Promise<void> {
  await page.addInitScript(() => {
    const w = window as unknown as Record<string, unknown>;
    let admin: Record<string, unknown> | undefined;
    Object.defineProperty(w, 'sproutAdmin', {
      configurable: true,
      get: () => admin,
      set: (value: Record<string, unknown>) => {
        const getTheme = value.getTheme as () => string;
        value.getTheme = () => {
          w.__sproutInteractive = true;
          return getTheme();
        };
        admin = value;
      },
    });
  });
  await page.goto(url);
  await page.waitForFunction(() => (window as unknown as Record<string, unknown>).__sproutInteractive === true);
  await expect(page.locator('.topbar')).toBeVisible();
}

/** Saves a full-page screenshot under results/<feature>/<name>.png. */
export async function screenshot(page: Page, feature: string, name: string): Promise<void> {
  await page.screenshot({ path: path.join(resultsDir, feature, `${name}.png`), fullPage: true });
}
