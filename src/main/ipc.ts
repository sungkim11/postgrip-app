import { ipcMain, dialog, BrowserWindow } from 'electron';
import { appState } from './state';
import { loadConnections, saveConnections } from './storage';
import * as postgres from './postgres';
import {
  toSavedConnection,
  toSafe,
  toSummary,
  type AppSnapshot,
  type ConnectionInput,
} from './types';
import fs from 'node:fs';

function snapshot(): AppSnapshot {
  const savedConnections = loadConnections().map(toSafe);
  const activeConnection = appState.activeConnection
    ? toSummary(appState.activeConnection)
    : null;

  return {
    savedConnections,
    activeConnection,
    databaseTree: [],
  };
}

async function snapshotWithTree(): Promise<AppSnapshot> {
  const snap = snapshot();
  if (appState.activeConnection) {
    snap.databaseTree = await postgres.fetchTree(appState.activeConnection);
  }
  return snap;
}

export function registerIpcHandlers(): void {
  ipcMain.handle('bootstrap', async () => {
    return snapshotWithTree();
  });

  ipcMain.handle('connect', async (_event, connection: ConnectionInput, save: boolean) => {
    const saved = toSavedConnection(connection);
    await postgres.testConnection(saved);

    if (save) {
      const existing = loadConnections();
      const idx = existing.findIndex((c) => c.id === saved.id);
      if (idx >= 0) {
        existing[idx] = saved;
      } else {
        existing.push(saved);
      }
      saveConnections(existing);
    }

    appState.activeConnection = saved;
    return snapshotWithTree();
  });

  ipcMain.handle('activate-saved-connection', async (_event, id: string) => {
    const saved = loadConnections();
    const conn = saved.find((c) => c.id === id);
    if (!conn) throw new Error('Saved connection not found');

    await postgres.testConnection(conn);
    appState.activeConnection = conn;
    return snapshotWithTree();
  });

  ipcMain.handle('delete-saved-connection', async (_event, id: string) => {
    const saved = loadConnections().filter((c) => c.id !== id);
    saveConnections(saved);

    if (appState.activeConnection?.id === id) {
      appState.activeConnection = null;
    }

    return snapshotWithTree();
  });

  ipcMain.handle('disconnect', async () => {
    appState.activeConnection = null;
    return snapshotWithTree();
  });

  ipcMain.handle('run-query', async (_event, sql: string, limit?: number) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    return postgres.runQuery(appState.activeConnection, sql, limit ?? 500);
  });

  ipcMain.handle('preview-table', async (_event, schema: string, table: string, limit?: number, offset?: number) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    return postgres.previewTable(appState.activeConnection, schema, table, limit ?? 200, offset ?? 0);
  });

  ipcMain.handle('get-table-ddl', async (_event, schema: string, table: string) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    const ddl = await postgres.getTableDdl(appState.activeConnection, schema, table);
    return { ddl };
  });

  ipcMain.handle('export-parquet', async (_event, schema: string, table: string, path: string) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    return postgres.exportParquet(appState.activeConnection, schema, table, path);
  });

  ipcMain.handle('show-save-dialog', async (_event, options: Electron.SaveDialogOptions) => {
    const win = BrowserWindow.getFocusedWindow();
    if (!win) return null;
    const result = await dialog.showSaveDialog(win, options);
    return result.canceled ? null : result.filePath;
  });

  ipcMain.handle('write-file', async (_event, filePath: string, content: string) => {
    fs.writeFileSync(filePath, content, 'utf-8');
  });

  ipcMain.handle('close-window', () => {
    const win = BrowserWindow.getFocusedWindow();
    win?.close();
  });
}
