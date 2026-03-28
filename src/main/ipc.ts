import { app, ipcMain, dialog, BrowserWindow } from 'electron';
import { appState } from './state';
import { loadConnections, saveConnections } from './storage';
import * as postgres from './postgres';
import { closeAllPools } from './postgres';
import { closeAllTunnels } from './ssh-tunnel';
import {
  buildConnectionString,
  toSavedConnection,
  toSafe,
  toSummary,
  type AppSnapshot,
  type ConnectionInput,
} from './types';
import { parsePgpassEntries } from './pgpass';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';

const execFileAsync = promisify(execFile);

const pgToolCache = new Map<string, string>();
function findPgTool(name: string): string {
  const cached = pgToolCache.get(name);
  if (cached) return cached;
  const candidates = [
    name,
    `/opt/homebrew/bin/${name}`,
    `/opt/homebrew/opt/libpq/bin/${name}`,
    `/usr/local/bin/${name}`,
    `/usr/local/opt/libpq/bin/${name}`,
    `/usr/lib/postgresql/16/bin/${name}`,
    `/usr/lib/postgresql/15/bin/${name}`,
    `/usr/lib/postgresql/14/bin/${name}`,
  ];
  for (const c of candidates) {
    try {
      fs.accessSync(c, fs.constants.X_OK);
      pgToolCache.set(name, c);
      return c;
    } catch { /* try next */ }
  }
  return name;
}

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

  ipcMain.handle('get-pgpass-entries', () => {
    return parsePgpassEntries();
  });

  ipcMain.handle('host-stats', async () => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    return postgres.getHostStats(appState.activeConnection);
  });

  ipcMain.handle('monitoring-data', async () => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    return postgres.getMonitoringData(appState.activeConnection);
  });

  ipcMain.handle('active-queries', async () => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    return postgres.getActiveQueries(appState.activeConnection);
  });

  ipcMain.handle('test-connection', async (_event, connection: ConnectionInput) => {
    console.log('[test-connection] received:', JSON.stringify(connection));
    const saved = toSavedConnection(connection);
    console.log('[test-connection] connectionString:', buildConnectionString(saved));
    await postgres.testConnection(saved);
    return { success: true };
  });

  ipcMain.handle('connect', async (_event, connection: ConnectionInput, save: boolean) => {
    const saved = toSavedConnection(connection);
    await postgres.testConnection(saved);

    if (save) {
      const existing = loadConnections();
      const idx = existing.findIndex((c) =>
        c.id === saved.id ||
        (c.host === saved.host && c.port === saved.port && c.database === saved.database && c.user === saved.user)
      );
      if (idx >= 0) {
        saved.id = existing[idx].id;
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
    closeAllPools();
    closeAllTunnels();
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

  ipcMain.handle('drop-table', async (_event, schema: string, table: string, cascade: boolean) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    await postgres.dropTable(appState.activeConnection, schema, table, cascade);
    return snapshotWithTree();
  });

  ipcMain.handle('truncate-table', async (_event, schema: string, table: string, cascade: boolean) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    await postgres.truncateTable(appState.activeConnection, schema, table, cascade);
    return snapshotWithTree();
  });

  ipcMain.handle('get-editable-table-data', async (_event, schema: string, table: string, limit: number, offset: number) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    return postgres.getEditableTableData(appState.activeConnection, schema, table, limit, offset);
  });

  ipcMain.handle('execute-dml', async (_event, schema: string, table: string, operations: postgres.DmlOperation[]) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    await postgres.executeDml(appState.activeConnection, schema, table, operations);
  });

  ipcMain.handle('get-modify-table-info', async (_event, schema: string, table: string) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    return postgres.getModifyTableInfo(appState.activeConnection, schema, table);
  });

  ipcMain.handle('alter-table', async (_event, schema: string, table: string, actions: postgres.AlterTableAction[]) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    await postgres.alterTable(appState.activeConnection, schema, table, actions);
    return snapshotWithTree();
  });

  ipcMain.handle('export-table-csv', async (_event, schema: string, table: string, path: string) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    return postgres.exportTableCsv(appState.activeConnection, schema, table, path);
  });

  ipcMain.handle('create-schema', async (_event, schemaName: string) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    await postgres.createSchema(appState.activeConnection, schemaName);
    return snapshotWithTree();
  });

  ipcMain.handle('create-table', async (_event, schema: string, tableName: string, columns: Array<{ name: string; type: string; nullable: boolean; defaultValue?: string }>) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    await postgres.createTable(appState.activeConnection, schema, tableName, columns);
    return snapshotWithTree();
  });

  ipcMain.handle('export-pg-dump', async (_event, schema: string, table: string, filePath: string, format: string) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    const conn = appState.activeConnection;
    const args = [
      '-h', conn.host,
      '-p', String(conn.port),
      '-U', conn.user,
      '-d', conn.database,
      '-t', `"${schema.replace(/"/g, '""')}"."${table.replace(/"/g, '""')}"`,
      '-f', filePath,
    ];
    if (format === 'sql') {
      args.push('--no-owner', '--no-acl');
    } else if (format === 'custom') {
      args.push('-Fc');
    } else if (format === 'tar') {
      args.push('-Ft');
    }

    const env = { ...process.env, PGPASSWORD: conn.password };
    await execFileAsync(findPgTool('pg_dump'), args, { env, timeout: 120000 });
  });

  ipcMain.handle('list-backups', async (_event, dirPath: string) => {
    try {
      const entries = await fs.promises.readdir(dirPath, { withFileTypes: true });
      const backupFiles = entries.filter((e) => e.isFile() && /\.(sql|dump|tar)$/i.test(e.name));
      const backups = (await Promise.all(backupFiles.map(async (e) => {
        try {
          const fullPath = path.join(dirPath, e.name);
          const [stat, meta] = await Promise.all([
            fs.promises.stat(fullPath),
            fs.promises.readFile(fullPath + '.meta.json', 'utf-8').then((c) => JSON.parse(c)).catch(() => undefined),
          ]);
          return { name: e.name, path: fullPath, size: stat.size, modified: stat.mtime.toISOString(), meta };
        } catch { return null; }
      }))).filter((b): b is NonNullable<typeof b> => b !== null);
      backups.sort((a, b) => b.modified.localeCompare(a.modified));
      return backups;
    } catch {
      return [];
    }
  });

  ipcMain.handle('get-backup-dir', () => {
    const prefPath = path.join(app.getPath('userData'), 'backup_dir.txt');
    let backupDir: string;
    try {
      backupDir = fs.readFileSync(prefPath, 'utf-8').trim();
    } catch {
      backupDir = path.join(os.homedir(), 'PostGrip_Backups');
    }
    try { fs.mkdirSync(backupDir, { recursive: true }); } catch { /* ignore */ }
    return backupDir;
  });

  // --- Backup Schedules ---
  const schedulesPath = path.join(app.getPath('userData'), 'backup_schedules.json');

  function loadSchedules(): Array<Record<string, unknown>> {
    try { return JSON.parse(fs.readFileSync(schedulesPath, 'utf-8')); } catch { return []; }
  }
  function saveSchedules(schedules: Array<Record<string, unknown>>) {
    fs.writeFileSync(schedulesPath, JSON.stringify(schedules, null, 2));
  }

  ipcMain.handle('list-backup-schedules', () => loadSchedules());

  ipcMain.handle('add-backup-schedule', (_event, schedule: Record<string, unknown>) => {
    const schedules = loadSchedules();
    schedule.id = randomUUID();
    schedule.createdAt = new Date().toISOString();
    schedules.push(schedule);
    saveSchedules(schedules);
    return schedule;
  });

  ipcMain.handle('update-backup-schedule', (_event, id: string, updates: Record<string, unknown>) => {
    const schedules = loadSchedules();
    const idx = schedules.findIndex((s) => s.id === id);
    if (idx >= 0) { Object.assign(schedules[idx], updates); saveSchedules(schedules); }
    return schedules;
  });

  ipcMain.handle('delete-backup-schedule', (_event, id: string) => {
    const schedules = loadSchedules().filter((s) => s.id !== id);
    saveSchedules(schedules);
    return schedules;
  });

  // Check schedules every minute and run due backups
  setInterval(async () => {
    if (!appState.activeConnection) return;
    const schedules = loadSchedules();
    if (schedules.length === 0) return;
    const now = new Date();
    const dayNames = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
    const currentDay = dayNames[now.getDay()];
    const currentTime = `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;

    for (const schedule of schedules) {
      if (!(schedule.enabled !== false)) continue;
      const days = schedule.days as string[] ?? [];
      if (!days.includes(currentDay)) continue;
      if (schedule.time !== currentTime) continue;
      // Prevent running same schedule twice in the same minute
      const lastRun = schedule.lastRun as string | undefined;
      if (lastRun && new Date(lastRun).getTime() > now.getTime() - 120000) continue;

      // Run the backup
      try {
        const conn = appState.activeConnection;
        const timestamp = now.toISOString().replace(/[:.]/g, '-').slice(0, 19);
        const fmt = (schedule.format as string) || 'tar';
        const ext = fmt === 'custom' ? '.dump' : fmt === 'tar' ? '.tar' : '.sql';
        const backupDirPref = path.join(app.getPath('userData'), 'backup_dir.txt');
        let bDir: string;
        try { bDir = fs.readFileSync(backupDirPref, 'utf-8').trim(); } catch { bDir = path.join(os.homedir(), 'PostGrip_Backups'); }
        try { fs.mkdirSync(bDir, { recursive: true }); } catch { /* ignore */ }

        const filePath = (schedule.outputDir as string || bDir) + `/${conn.database}_scheduled_${timestamp}${ext}`;
        const args = ['-h', conn.host, '-p', String(conn.port), '-U', conn.user, '-d', conn.database, '-f', filePath];
        if (fmt === 'custom') args.push('-Fc');
        else if (fmt === 'tar') args.push('-Ft');

        const tables = schedule.tables as string[] ?? [];
        const schemas = schedule.schemas as string[] ?? [];
        if (schemas.length) for (const s of schemas) args.push('-n', s);
        if (tables.length) for (const t of tables) args.push('-t', t);
        if (schedule.dataOnly) args.push('--data-only');
        if (schedule.schemaOnly) args.push('--schema-only');
        if (schedule.noOwner) args.push('--no-owner');
        if (schedule.noPrivileges) args.push('--no-acl');

        const env = { ...process.env, PGPASSWORD: conn.password };
        const startTime = Date.now();
        await execFileAsync(findPgTool('pg_dump'), args, { env, timeout: 600000 });
        const durationMs = Date.now() - startTime;

        // Save metadata
        const meta = { database: conn.database, host: conn.host, port: conn.port, user: conn.user, format: fmt, schemas, tables, scope: (schemas.length || tables.length) ? 'selected' : 'full', dataOnly: !!schedule.dataOnly, schemaOnly: !!schedule.schemaOnly, noOwner: !!schedule.noOwner, noPrivileges: !!schedule.noPrivileges, clean: false, createDb: false, ifExists: false, compress: 0, createdAt: now.toISOString(), durationMs, scheduled: true, scheduleId: schedule.id };
        try { fs.writeFileSync(filePath + '.meta.json', JSON.stringify(meta, null, 2)); } catch { /* ignore */ }

        // Update lastRun
        schedule.lastRun = now.toISOString();
        saveSchedules(schedules);
      } catch { /* log error but continue */ }
    }
  }, 60000);

  ipcMain.handle('set-backup-dir', (_event, dirPath: string) => {
    const prefPath = path.join(app.getPath('userData'), 'backup_dir.txt');
    try { fs.mkdirSync(dirPath, { recursive: true }); } catch { /* ignore */ }
    fs.writeFileSync(prefPath, dirPath);
  });

  ipcMain.handle('delete-backup', async (_event, filePath: string) => {
    await fs.promises.unlink(filePath);
  });

  ipcMain.handle('backup-database', async (_event, options: {
    filePath: string;
    format: string;
    schemas?: string[];
    tables?: string[];
    dataOnly?: boolean;
    schemaOnly?: boolean;
    noOwner?: boolean;
    noPrivileges?: boolean;
    clean?: boolean;
    createDb?: boolean;
    ifExists?: boolean;
    compress?: number;
    verbose?: boolean;
    blobs?: boolean;
    noBlobs?: boolean;
  }) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    const conn = appState.activeConnection;
    const args = [
      '-h', conn.host,
      '-p', String(conn.port),
      '-U', conn.user,
      '-d', conn.database,
      '-f', options.filePath,
    ];

    // Format
    if (options.format === 'custom') args.push('-Fc');
    else if (options.format === 'tar') args.push('-Ft');
    else if (options.format === 'directory') args.push('-Fd');
    // else plain SQL (default)

    // Scope
    if (options.schemas?.length) {
      for (const s of options.schemas) { args.push('-n', s); }
    }
    if (options.tables?.length) {
      for (const t of options.tables) { args.push('-t', t); }
    }

    // Options
    if (options.dataOnly) args.push('--data-only');
    if (options.schemaOnly) args.push('--schema-only');
    if (options.noOwner) args.push('--no-owner');
    if (options.noPrivileges) args.push('--no-acl');
    if (options.clean) args.push('--clean');
    if (options.createDb) args.push('--create');
    if (options.ifExists) args.push('--if-exists');
    if (options.compress != null && options.compress > 0) args.push(`-Z${options.compress}`);
    if (options.verbose) args.push('--verbose');
    if (options.blobs) args.push('--blobs');
    if (options.noBlobs) args.push('--no-blobs');

    const env = { ...process.env, PGPASSWORD: conn.password };
    const startTime = Date.now();
    await execFileAsync(findPgTool('pg_dump'), args, { env, timeout: 600000 });
    const durationMs = Date.now() - startTime;

    // Save metadata alongside the backup
    const meta = {
      database: conn.database,
      host: conn.host,
      port: conn.port,
      user: conn.user,
      format: options.format,
      schemas: options.schemas ?? [],
      tables: options.tables ?? [],
      scope: (options.schemas?.length || options.tables?.length) ? 'selected' : 'full',
      dataOnly: !!options.dataOnly,
      schemaOnly: !!options.schemaOnly,
      noOwner: !!options.noOwner,
      noPrivileges: !!options.noPrivileges,
      clean: !!options.clean,
      createDb: !!options.createDb,
      ifExists: !!options.ifExists,
      compress: options.compress ?? 0,
      createdAt: new Date().toISOString(),
      durationMs,
    };
    try { fs.writeFileSync(options.filePath + '.meta.json', JSON.stringify(meta, null, 2)); } catch { /* ignore */ }

    return { durationMs };
  });

  ipcMain.handle('restore-database', async (_event, filePath: string) => {
    if (!appState.activeConnection) throw new Error('No active database connection');
    const conn = appState.activeConnection;
    const ext = filePath.toLowerCase();

    if (ext.endsWith('.sql')) {
      const sql = await fs.promises.readFile(filePath, 'utf-8');
      await postgres.executeSql(conn, sql);
    } else {
      const args = [
        '-h', conn.host,
        '-p', String(conn.port),
        '-U', conn.user,
        '-d', conn.database,
        filePath,
      ];
      const env = { ...process.env, PGPASSWORD: conn.password };
      await execFileAsync(findPgTool('pg_restore'), args, { env, timeout: 600000 });
    }
  });

  ipcMain.handle('show-open-dialog', async (_event, options: Electron.OpenDialogOptions) => {
    const win = BrowserWindow.getFocusedWindow();
    if (!win) return null;
    const result = await dialog.showOpenDialog(win, options);
    return result.canceled ? null : result.filePaths[0] ?? null;
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

  ipcMain.handle('list-directory', async (_event, dirPath: string) => {
    const entries = await fs.promises.readdir(dirPath, { withFileTypes: true });
    return entries
      .filter((e) => !e.name.startsWith('.'))
      .sort((a, b) => {
        if (a.isDirectory() && !b.isDirectory()) return -1;
        if (!a.isDirectory() && b.isDirectory()) return 1;
        return a.name.localeCompare(b.name);
      })
      .map((e) => ({
        name: e.name,
        path: path.join(dirPath, e.name),
        isDirectory: e.isDirectory(),
      }));
  });

  ipcMain.handle('read-text-file', async (_event, filePath: string) => {
    const content = await fs.promises.readFile(filePath, 'utf-8');
    return content;
  });

  ipcMain.handle('get-home-dir', () => {
    return os.homedir();
  });

  ipcMain.handle('get-app-info', () => {
    const pkg = require('../../package.json');
    return {
      name: pkg.productName || pkg.name,
      version: pkg.version,
      electronVersion: process.versions.electron,
      nodeVersion: process.versions.node,
      chromiumVersion: process.versions.chrome,
      platform: `${process.platform} ${process.arch}`,
    };
  });

  ipcMain.handle('read-help', async () => {
    // Try bundled docs/help.md first, then fall back to development path
    const candidates = [
      path.join(__dirname, '../../docs/help.md'),
      path.join(__dirname, '../../../docs/help.md'),
    ];
    for (const p of candidates) {
      try {
        return await fs.promises.readFile(p, 'utf-8');
      } catch { /* try next */ }
    }
    return '# Help\n\nHelp file not found.';
  });

  ipcMain.handle('find-git-repos', async (_event, dirPath: string) => {
    const home = os.homedir();
    // Only scan directories that don't trigger macOS TCC permission prompts
    const SCAN_DIRS = ['Developer', 'Projects', 'repos', 'src', 'code', 'workspace', 'git', 'work'];
    const repos: Array<{ name: string; path: string }> = [];

    async function scanDir(dir: string) {
      try {
        const entries = await fs.promises.readdir(dir, { withFileTypes: true });
        if (entries.some((e) => e.name === '.git' && e.isDirectory())) {
          repos.push({ name: path.basename(dir), path: dir });
          return;
        }
        const subDirs = entries.filter((e) => e.isDirectory() && !e.name.startsWith('.') && e.name !== 'node_modules');
        await Promise.all(subDirs.map(async (sub) => {
          const subPath = path.join(dir, sub.name);
          try {
            const subEntries = await fs.promises.readdir(subPath, { withFileTypes: true });
            if (subEntries.some((e) => e.name === '.git' && e.isDirectory())) {
              repos.push({ name: sub.name, path: subPath });
            }
          } catch { /* skip */ }
        }));
      } catch { /* skip */ }
    }

    // Scan known developer directories under home
    await Promise.all(SCAN_DIRS.map((name) => scanDir(path.join(home, name))));

    // If a custom dirPath was provided and differs from home, scan it too
    if (dirPath !== home) {
      await scanDir(dirPath);
    }

    repos.sort((a, b) => a.name.localeCompare(b.name));
    return repos;
  });

  ipcMain.handle('git-repo-root', async (_event, dirPath: string) => {
    try {
      const { stdout } = await execFileAsync('git', ['rev-parse', '--show-toplevel'], { cwd: dirPath });
      return stdout.trim();
    } catch {
      return null;
    }
  });

  ipcMain.handle('git-status', async (_event, repoPath: string) => {
    try {
      const [branchResult, statusResult, logResult] = await Promise.all([
        execFileAsync('git', ['rev-parse', '--abbrev-ref', 'HEAD'], { cwd: repoPath }),
        execFileAsync('git', ['status', '--porcelain', '-u'], { cwd: repoPath }),
        execFileAsync('git', ['log', '--oneline', '-20'], { cwd: repoPath }),
      ]);
      const files = branchResult.stdout && statusResult.stdout.trim()
        ? statusResult.stdout.trim().split('\n').filter(Boolean).map((line: string) => ({
            status: line.substring(0, 2).trim(),
            path: line.substring(3),
          }))
        : [];
      const commits = logResult.stdout.trim().split('\n').filter(Boolean).map((line: string) => {
        const spaceIdx = line.indexOf(' ');
        return { hash: line.substring(0, spaceIdx), message: line.substring(spaceIdx + 1) };
      });
      return { branch: branchResult.stdout.trim(), files, commits };
    } catch {
      return null;
    }
  });

  ipcMain.handle('git-diff', async (_event, repoPath: string, filePath: string) => {
    try {
      const { stdout } = await execFileAsync('git', ['diff', 'HEAD', '--', filePath], { cwd: repoPath });
      return stdout || '(no changes)';
    } catch {
      return null;
    }
  });

  ipcMain.handle('close-window', () => {
    const win = BrowserWindow.getFocusedWindow();
    win?.close();
  });
}
