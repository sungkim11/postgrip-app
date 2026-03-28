import { test, expect } from '@playwright/test';
import { launchApp, closeApp } from './helpers';
import type { ElectronApplication, Page } from '@playwright/test';

let app: ElectronApplication | undefined;
let page: Page;

test.beforeAll(async () => {
  ({ app, page } = await launchApp());
});

test.afterAll(async () => {
  await closeApp(app);
});

test.describe('Toolbar and Menus', () => {
  test('menu buttons exist', async () => {
    await expect(page.getByRole('button', { name: 'File', exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: 'SQL Editor', exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Monitoring', exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Backup & Restore', exact: true })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Help', exact: true })).toBeVisible();
  });

  test('SQL Editor button opens editor', async () => {
    // The "SQL Editor" text is a menu button in the header
    await page.locator('button', { hasText: 'SQL Editor' }).click();
    await expect(page.locator('.cm-editor')).toBeVisible({ timeout: 5000 });
  });

  test('editor has line numbers and gutter', async () => {
    await expect(page.locator('.cm-gutters')).toBeVisible();
  });

  test('Close button hides editor', async () => {
    await page.locator('button', { hasText: 'Close' }).click();
    await expect(page.locator('.cm-editor')).not.toBeVisible();
  });

  test('Refresh button exists', async () => {
    await expect(page.locator('button[title="Refresh"]').first()).toBeVisible();
  });
});
