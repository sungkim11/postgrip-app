import type { PropsWithChildren, ReactNode } from 'react';
import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../lib/api';
import { SqlEditor } from './SqlEditor';
import type {
  ActiveConnectionSummary,
  AlterTableAction,
  AppInfo,
  AppSnapshot,
  BackupEntry,
  BackupOptions,
  BackupSchedule,
  MonitoringData,
  ConnectionInput,
  DdlResult,
  DmlOperation,
  EditableTableData,
  HostStats,
  IndexNode,
  KeyNode,
  ModifyTableColumn,
  ModifyTableInfo,
  FileEntry,
  GitRepo,
  GitStatus,
  PgpassEntry,
  QueryResult,
  SafeSavedConnection,
  SchemaNode,
  SshConfig,
  TableNode,
} from '../lib/types';

const EMPTY_SSH: SshConfig = {
  enabled: false,
  host: '',
  port: 22,
  user: '',
  authMethod: 'password',
  password: '',
  privateKey: '',
  passphrase: '',
};

const EMPTY_CONNECTION: ConnectionInput = {
  name: '',
  host: '127.0.0.1',
  port: 5432,
  user: 'postgres',
  password: '',
  database: 'postgres',
  authMethod: 'password',
  ssh: { ...EMPTY_SSH },
};

const QUERY_PRESETS = [
  {
    label: 'Session',
    sql: 'select current_database(), current_user, now();',
  },
  {
    label: 'Tables',
    sql: "select table_schema, table_name\nfrom information_schema.tables\nwhere table_schema not in ('pg_catalog', 'information_schema')\norder by 1, 2\nlimit 200;",
  },
  {
    label: 'Activity',
    sql: 'select pid, usename, state, query\nfrom pg_stat_activity\norder by backend_start desc\nlimit 50;',
  },
];

interface QueryHistoryEntry {
  id: string;
  title: string;
  sql: string;
  resultMeta: string;
}

type TopMenu = 'file' | 'help' | null;

type EditDataState = {
  tableData: EditableTableData;
  editedCells: Map<string, string | null>; // "rowIdx:colIdx" -> new value
  deletedRows: Set<number>; // original row indexes
  newRows: Array<(string | null)[]>; // added rows
  editingCell: { row: number; col: number; isNew: boolean } | null;
  page: number;
  pageSize: number;
};

type EditorTab = {
  id: string;
  kind: 'query' | 'table' | 'ddl' | 'editdata';
  title: string;
  sql: string;
  source?: { schema: string; table: string };
  sortState: SortState;
  currentPage: number;
  result: QueryResult | null;
  ddlText?: string;
  editData?: EditDataState;
};

type ContextMenuState = {
  x: number;
  y: number;
  schema: string;
  table: string;
} | null;

type DatabaseContextMenuState = {
  x: number;
  y: number;
} | null;

type SchemaContextMenuState = {
  x: number;
  y: number;
  schema: string;
} | null;

type ConnectionContextMenuState = {
  x: number;
  y: number;
  connection: SafeSavedConnection;
} | null;

type ConfirmDialogState = {
  message: string;
  onConfirm: () => void;
} | null;

type DestructiveTableDialogState = {
  action: 'drop' | 'truncate';
  schema: string;
  table: string;
} | null;

type SqlTab = {
  id: string;
  title: string;
  sql: string;
};

type SortState = {
  columnIndex: number;
  direction: 'asc' | 'desc';
} | null;

type DragState = 'sidebar' | 'connections' | null;

function classNames(...values: Array<string | false | null | undefined>) {
  return values.filter(Boolean).join(' ');
}

function makeTabId(prefix: string) {
  return `${prefix}-${crypto.randomUUID()}`;
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms} ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)} s`;
  const m = Math.floor(s / 60);
  return `${m}m ${Math.floor(s % 60)}s`;
}

function errorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === 'string') return err;
  return 'An unknown error occurred.';
}

export function AppShell() {
  const [snapshot, setSnapshot] = useState<AppSnapshot | null>(null);
  const [pgpassEntries, setPgpassEntries] = useState<PgpassEntry[]>([]);
  const [queryHistory, setQueryHistory] = useState<QueryHistoryEntry[]>([]);
  const [loading, setLoading] = useState('Booting desktop shell...');
  const [error, setError] = useState<string | null>(null);
  const [showConnectionModal, setShowConnectionModal] = useState(false);
  const [showSqlEditor, setShowSqlEditor] = useState(false);
  const [sqlTabs, setSqlTabs] = useState<SqlTab[]>([]);
  const [activeSqlTabId, setActiveSqlTabId] = useState<string | null>(null);
  const [openMenu, setOpenMenu] = useState<TopMenu>(null);
  const [draft, setDraft] = useState<ConnectionInput>(EMPTY_CONNECTION);
  const [persistConnection, setPersistConnection] = useState(true);
  const [editorTabs, setEditorTabs] = useState<EditorTab[]>([]);
  const [activeEditorTabId, setActiveEditorTabId] = useState('');
  const PAGE_SIZE = 500;
  const [sidebarWidth, setSidebarWidth] = useState(320);
  const [connectionsHeight, setConnectionsHeight] = useState(180);
  const [dragState, setDragState] = useState<DragState>(null);
  const [contextMenu, setContextMenu] = useState<ContextMenuState>(null);
  const [connectionContextMenu, setConnectionContextMenu] = useState<ConnectionContextMenuState>(null);
  const [databaseContextMenu, setDatabaseContextMenu] = useState<DatabaseContextMenuState>(null);
  const [schemaContextMenu, setSchemaContextMenu] = useState<SchemaContextMenuState>(null);
  const [showCreateSchemaModal, setShowCreateSchemaModal] = useState(false);
  const [showCreateTableModal, setShowCreateTableModal] = useState(false);
  const [createTableSchema, setCreateTableSchema] = useState('');
  const [confirmDialog, setConfirmDialog] = useState<ConfirmDialogState>(null);
  const [destructiveTableDialog, setDestructiveTableDialog] = useState<DestructiveTableDialogState>(null);
  const [destructiveCascade, setDestructiveCascade] = useState(false);
  const [modifyTableInfo, setModifyTableInfo] = useState<ModifyTableInfo | null>(null);
  const [modifyTableDraft, setModifyTableDraft] = useState<{ columns: ModifyTableColumn[]; newTableName: string; addColumns: Array<{ name: string; dataType: string; nullable: boolean; defaultValue: string }>; dropColumns: Set<string>; addForeignKeys: Array<{ column: string; refTable: string; refColumn: string }>; addIndexes: Array<{ name: string; columns: string; unique: boolean }> }>({ columns: [], newTableName: '', addColumns: [], dropColumns: new Set(), addForeignKeys: [], addIndexes: [] });
  const [modifyTableError, setModifyTableError] = useState('');
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'fail'>('idle');
  const [connectionTab, setConnectionTab] = useState<'general' | 'ssh'>('general');
  const [testError, setTestError] = useState('');
  const [activePanel, setActivePanel] = useState<'monitoring' | 'backup' | null>(null);
  const showMonitoringPanel = activePanel === 'monitoring';
  const showBackupPanel = activePanel === 'backup';
  const [monitoringData, setMonitoringData] = useState<MonitoringData | null>(null);
  const [showBackupModal, setShowBackupModal] = useState(false);
  const [runningBackup, setRunningBackup] = useState<{ name: string; startTime: number } | null>(null);
  const [showScheduleModal, setShowScheduleModal] = useState(false);
  const [editingSchedule, setEditingSchedule] = useState<BackupSchedule | null>(null);
  const [backupSchedules, setBackupSchedules] = useState<BackupSchedule[]>([]);
  const [backupEntries, setBackupEntries] = useState<BackupEntry[]>([]);
  const [backupDir, setBackupDir] = useState('');
  const [showHelpViewer, setShowHelpViewer] = useState(false);
  const [helpContent, setHelpContent] = useState('');
  const [showAboutDialog, setShowAboutDialog] = useState(false);
  const [appInfo, setAppInfo] = useState<AppInfo | null>(null);
  const [hostStats, setHostStats] = useState<HostStats | null>(null);
  const MAX_HISTORY = 30;
  const [cpuHistory, setCpuHistory] = useState<number[]>([]);
  const [ramHistory, setRamHistory] = useState<number[]>([]);
  const [tpsHistory, setTpsHistory] = useState<number[]>([]);
  const [saturationHistory, setSaturationHistory] = useState<number[]>([]);
  const [cacheHitHistory, setCacheHitHistory] = useState<number[]>([]);
  const [explorerTab, setExplorerTab] = useState<'database' | 'files' | 'git'>('database');
  const [fileEntries, setFileEntries] = useState<FileEntry[]>([]);
  const [currentDir, setCurrentDir] = useState<string>('');
  const [expandedDirs, setExpandedDirs] = useState<Record<string, FileEntry[]>>({});
  const [gitStatus, setGitStatus] = useState<GitStatus | null>(null);
  const [gitRepoPath, setGitRepoPath] = useState('');
  const [gitRepos, setGitRepos] = useState<GitRepo[]>([]);

  const activeEditorTab = useMemo(
    () => editorTabs.find((tab) => tab.id === activeEditorTabId) ?? editorTabs[0],
    [activeEditorTabId, editorTabs],
  );

  const databaseTree = snapshot?.databaseTree ?? [];

  const unsavedPgpass = useMemo(() => {
    const savedKeys = new Set((snapshot?.savedConnections ?? []).map((c) => `${c.host}:${c.port}:${c.database}:${c.user}`));
    return pgpassEntries.filter((e) => !savedKeys.has(`${e.host}:${e.port}:${e.database}:${e.user}`));
  }, [pgpassEntries, snapshot?.savedConnections]);

  const destructiveTableHasDependents = useMemo(() => {
    if (!destructiveTableDialog) return false;
    const qualifiedName = `${destructiveTableDialog.schema}.${destructiveTableDialog.table}`;
    for (const schema of databaseTree) {
      for (const table of schema.tables) {
        if (schema.name === destructiveTableDialog.schema && table.name === destructiveTableDialog.table) continue;
        for (const key of table.keys) {
          if (key.type === 'FOREIGN KEY' && key.referencedTable === qualifiedName) return true;
        }
      }
    }
    return false;
  }, [destructiveTableDialog, databaseTree]);

  const activeSqlTab = useMemo(
    () => sqlTabs.find((t) => t.id === activeSqlTabId) ?? sqlTabs[0] ?? null,
    [activeSqlTabId, sqlTabs],
  );

  const sqlEditorText = activeSqlTab?.sql ?? '';
  const setSqlEditorText = (text: string) => {
    if (!activeSqlTab) return;
    setSqlTabs((tabs) => tabs.map((t) => (t.id === activeSqlTab.id ? { ...t, sql: text } : t)));
  };

  function openSqlEditor() {
    setShowSqlEditor(true);
    setActivePanel(null);
    if (sqlTabs.length === 0) addSqlTab();
  }

  function addSqlTab() {
    const id = makeTabId('sql');
    const num = sqlTabs.length + 1;
    setSqlTabs((tabs) => [...tabs, { id, title: `Query ${num}`, sql: '' }]);
    setActiveSqlTabId(id);
  }

  function closeSqlTab(id: string) {
    setSqlTabs((tabs) => {
      const next = tabs.filter((t) => t.id !== id);
      if (next.length === 0) {
        setShowSqlEditor(false);
        setActiveSqlTabId(null);
        setEditorTabs([]);
        setActiveEditorTabId('');
      } else if (activeSqlTabId === id) {
        setActiveSqlTabId(next[next.length - 1]?.id ?? null);
      }
      return next;
    });
  }

  const processedResult = useMemo(() => {
    const result = activeEditorTab?.result;
    if (!result) {
      return null;
    }

    const tabSort = activeEditorTab.sortState;
    const tabPage = activeEditorTab.currentPage;
    let rows = result.rows;

    if (tabSort) {
      rows = [...rows].sort((left, right) => {
        const leftValue = left[tabSort.columnIndex] ?? '';
        const rightValue = right[tabSort.columnIndex] ?? '';
        const ordered = leftValue.localeCompare(rightValue, undefined, {
          numeric: true,
          sensitivity: 'base',
        });
        return tabSort.direction === 'asc' ? ordered : -ordered;
      });
    }

    const totalRows = rows.length;
    const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
    const safePage = Math.min(tabPage, totalPages - 1);
    const pagedRows = rows.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);

    return {
      ...result,
      rows: pagedRows,
      rowCount: totalRows,
      totalPages,
      currentPage: safePage,
      pageStart: safePage * PAGE_SIZE,
      notice: tabSort
        ? `View rows ${totalRows} of ${result.rows.length}`
        : result.notice,
    };
  }, [activeEditorTab]);

  useEffect(() => {
    void refresh();
    api.getPgpassEntries().then(setPgpassEntries).catch(() => {});
    api.getHomeDir().then((home) => {
      setCurrentDir(home);
      api.listDirectory(home).then(setFileEntries).catch(() => {});
      api.findGitRepos(home).then(setGitRepos).catch(() => {});
      api.gitRepoRoot(home).then((root) => {
        if (root) {
          setGitRepoPath(root);
          api.gitStatus(root).then(setGitStatus).catch(() => {});
        }
      }).catch(() => {});
    }).catch(() => {});
  }, []);

  async function browseDirectory(dirPath: string) {
    try {
      const entries = await api.listDirectory(dirPath);
      setCurrentDir(dirPath);
      setFileEntries(entries);
      setExpandedDirs({});
      api.gitRepoRoot(dirPath).then((root) => {
        if (root) {
          setGitRepoPath(root);
          api.gitStatus(root).then(setGitStatus).catch(() => {});
        } else {
          setGitRepoPath('');
          setGitStatus(null);
        }
      }).catch(() => {});
    } catch { /* ignore */ }
  }

  async function toggleSubDir(dirPath: string) {
    if (expandedDirs[dirPath]) {
      setExpandedDirs((prev) => { const next = { ...prev }; delete next[dirPath]; return next; });
    } else {
      try {
        const entries = await api.listDirectory(dirPath);
        setExpandedDirs((prev) => ({ ...prev, [dirPath]: entries }));
      } catch { /* ignore */ }
    }
  }

  function openSqlFileInEditor(content: string, title: string) {
    const id = makeTabId('sql');
    setSqlTabs((tabs) => [...tabs, { id, title, sql: content }]);
    setActiveSqlTabId(id);
    setShowSqlEditor(true);
  }

  async function openFileInTab(filePath: string, fileName: string) {
    try {
      const content = await api.readTextFile(filePath);
      if (fileName.endsWith('.sql')) {
        openSqlFileInEditor(content, fileName);
      } else {
        openNewQueryTab(content, fileName);
      }
    } catch { /* ignore */ }
  }

  async function switchGitRepo(repoPath: string) {
    setGitRepoPath(repoPath);
    try {
      const status = await api.gitStatus(repoPath);
      setGitStatus(status);
    } catch { setGitStatus(null); }
  }

  async function refreshGitStatus() {
    if (!gitRepoPath) return;
    try {
      const status = await api.gitStatus(gitRepoPath);
      setGitStatus(status);
    } catch { /* ignore */ }
  }

  async function openGitDiff(filePath: string) {
    if (!gitRepoPath) return;
    try {
      const diff = await api.gitDiff(gitRepoPath, filePath);
      if (diff) openNewQueryTab(diff, `diff: ${filePath.split('/').pop()}`);
    } catch { /* ignore */ }
  }

  useEffect(() => {
    if (!snapshot?.activeConnection) {
      setHostStats(null);
      setCpuHistory([]);
      setRamHistory([]);
      setTpsHistory([]);
      setSaturationHistory([]);
      setCacheHitHistory([]);
      return;
    }

    let cancelled = false;
    async function fetchStats() {
      try {
        const stats = await api.hostStats();
        if (cancelled) return;
        setHostStats(stats);
        if (stats.cpuUsagePercent != null) {
          setCpuHistory((h) => [...h, stats.cpuUsagePercent!].slice(-MAX_HISTORY));
        }
        if (stats.memUsagePercent != null) {
          setRamHistory((h) => [...h, stats.memUsagePercent!].slice(-MAX_HISTORY));
        }
        if (stats.tps != null) {
          setTpsHistory((h) => [...h, stats.tps!].slice(-MAX_HISTORY));
        }
        if (stats.connectionSaturationPercent != null) {
          setSaturationHistory((h) => [...h, stats.connectionSaturationPercent!].slice(-MAX_HISTORY));
        }
        if (stats.cacheHitRatio != null) {
          setCacheHitHistory((h) => [...h, stats.cacheHitRatio!].slice(-MAX_HISTORY));
        }
      } catch {}
    }

    void fetchStats();
    let interval = setInterval(() => void fetchStats(), 10000);

    function onVisibilityChange() {
      if (document.hidden) {
        clearInterval(interval);
      } else {
        void fetchStats();
        interval = setInterval(() => void fetchStats(), 10000);
      }
    }
    document.addEventListener('visibilitychange', onVisibilityChange);

    return () => { cancelled = true; clearInterval(interval); document.removeEventListener('visibilitychange', onVisibilityChange); };
  }, [snapshot?.activeConnection?.id]);

  useEffect(() => {
    function onPointerMove(event: PointerEvent) {
      if (dragState === 'sidebar') {
        setSidebarWidth(Math.min(520, Math.max(240, event.clientX)));
      } else if (dragState === 'connections') {
        const headerHeight = 36;
        const y = event.clientY - headerHeight;
        setConnectionsHeight(Math.min(400, Math.max(80, y)));
      }
    }

    function onPointerUp() {
      setDragState(null);
    }

    if (dragState) {
      window.addEventListener('pointermove', onPointerMove);
      window.addEventListener('pointerup', onPointerUp);
    }

    return () => {
      window.removeEventListener('pointermove', onPointerMove);
      window.removeEventListener('pointerup', onPointerUp);
    };
  }, [dragState]);

  async function refresh() {
    try {
      setLoading('Loading workspace...');
      setError(null);
      setSnapshot(await api.bootstrap());
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  function updateActiveTab(patch: Partial<EditorTab>) {
    if (!activeEditorTab) {
      return;
    }

    setEditorTabs((current) =>
      current.map((tab) => (tab.id === activeEditorTab.id ? { ...tab, ...patch } : tab)),
    );
  }

  async function handleExit() {
    await api.closeWindow();
  }

  async function changeBackupDir() {
    const newDir = await api.showOpenDialog({
      properties: ['openDirectory', 'createDirectory'],
    });
    if (!newDir) return;
    setBackupDir(newDir);
    await api.setBackupDir(newDir);
    try {
      const entries = await api.listBackups(newDir);
      setBackupEntries(entries);
    } catch { /* ignore */ }
  }

  function mergeBackupEntries(current: BackupEntry[], diskEntries: BackupEntry[]): BackupEntry[] {
    // Keep in-progress and failed entries, merge with disk entries
    const statusEntries = current.filter((e) => e.status === 'in_progress' || e.status === 'failed');
    const statusPaths = new Set(statusEntries.map((e) => e.path));
    const merged = [...statusEntries, ...diskEntries.filter((e) => !statusPaths.has(e.path))];
    return merged;
  }

  async function openMonitoringPanel() {
    if (showMonitoringPanel) { setActivePanel(null); return; }
    setActivePanel('monitoring');
    if (!snapshot?.activeConnection) return;
    try {
      const data = await api.monitoringData();
      setMonitoringData(data);
    } catch { /* ignore */ }
  }

  async function refreshMonitoring() {
    if (!snapshot?.activeConnection) return;
    try {
      const data = await api.monitoringData();
      setMonitoringData(data);
    } catch { /* ignore */ }
  }

  async function openBackupPanel() {
    if (showBackupPanel) { setActivePanel(null); return; }
    setActivePanel('backup');
    try {
      const dir = await api.getBackupDir();
      setBackupDir(dir);
      const [diskEntries, schedules] = await Promise.all([
        api.listBackups(dir),
        api.listBackupSchedules(),
      ]);
      setBackupEntries((prev) => mergeBackupEntries(prev, diskEntries));
      setBackupSchedules(schedules);
    } catch { /* ignore */ }
  }

  async function refreshBackups() {
    if (!backupDir) return;
    try {
      const diskEntries = await api.listBackups(backupDir);
      setBackupEntries((prev) => mergeBackupEntries(prev, diskEntries));
    } catch { /* ignore */ }
  }

  function handleBackup() {
    if (!snapshot?.activeConnection) { setError('No active database connection'); return; }
    setShowBackupModal(true);
  }

  async function executeBackup(options: BackupOptions) {
    setShowBackupModal(false);
    const fileName = options.filePath.split('/').pop() ?? 'backup';
    const inProgressEntry: BackupEntry = {
      name: fileName,
      path: options.filePath,
      size: 0,
      modified: new Date().toISOString(),
      status: 'in_progress',
      meta: {
        database: snapshot?.activeConnection?.database ?? '',
        host: snapshot?.activeConnection?.host ?? '',
        port: snapshot?.activeConnection?.port ?? 5432,
        user: snapshot?.activeConnection?.user ?? '',
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
        durationMs: 0,
      },
    };
    setRunningBackup({ name: fileName, startTime: Date.now() });
    setBackupEntries((prev) => [inProgressEntry, ...prev]);
    try {
      const result = await api.backupDatabase(options);
      setRunningBackup(null);
      setBackupEntries((prev) => prev.map((e) =>
        e.path === options.filePath && e.status === 'in_progress'
          ? { ...e, status: 'completed' as const, durationMs: result.durationMs }
          : e
      ));
      void refreshBackups();
    } catch (err) {
      setRunningBackup(null);
      setBackupEntries((prev) => prev.map((e) =>
        e.path === options.filePath && e.status === 'in_progress'
          ? { ...e, status: 'failed' as const, error: errorMessage(err) }
          : e
      ));
      setError(errorMessage(err));
    }
  }

  async function handleDeleteBackup(filePath: string) {
    try {
      await api.deleteBackup(filePath);
      void refreshBackups();
    } catch (err) {
      setError(errorMessage(err));
    }
  }

  async function handleRestore(filePath: string) {
    if (!snapshot?.activeConnection) { setError('No active database connection'); return; }
    try {
      setLoading('Restoring database...');
      await api.restoreDatabase(filePath);
      setLoading('Restore complete.');
      setTimeout(() => setLoading(''), 2000);
      setSnapshot(await api.bootstrap());
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleAddSchedule(schedule: Omit<BackupSchedule, 'id' | 'createdAt'>) {
    setShowScheduleModal(false);
    setEditingSchedule(null);
    try {
      await api.addBackupSchedule(schedule);
      setBackupSchedules(await api.listBackupSchedules());
    } catch (err) { setError(errorMessage(err)); }
  }

  async function handleUpdateSchedule(id: string, updates: Partial<BackupSchedule>) {
    setShowScheduleModal(false);
    setEditingSchedule(null);
    try {
      await api.updateBackupSchedule(id, updates);
      setBackupSchedules(await api.listBackupSchedules());
    } catch (err) { setError(errorMessage(err)); }
  }

  async function handleDeleteSchedule(id: string) {
    try {
      await api.deleteBackupSchedule(id);
      setBackupSchedules(await api.listBackupSchedules());
    } catch (err) { setError(errorMessage(err)); }
  }

  async function handleCreateSchema(schemaName: string) {
    setShowCreateSchemaModal(false);
    try {
      setLoading(`Creating schema ${schemaName}...`);
      const next = await api.createSchema(schemaName);
      setSnapshot(next);
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleCreateTable(schema: string, tableName: string, columns: Array<{ name: string; type: string; nullable: boolean; defaultValue?: string; pk?: boolean }>, foreignKeys?: Array<{ column: string; refTable: string; refColumn: string }>, indexes?: Array<{ name?: string; columns: string; unique?: boolean }>) {
    setShowCreateTableModal(false);
    try {
      setLoading(`Creating table ${schema}.${tableName}...`);
      const next = await api.createTable(schema, tableName, columns, foreignKeys, indexes);
      setSnapshot(next);
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function openHelpViewer() {
    setOpenMenu(null);
    try {
      const content = await api.readHelp();
      setHelpContent(content);
      setShowHelpViewer(true);
    } catch { /* ignore */ }
  }

  async function openAboutDialog() {
    setOpenMenu(null);
    try {
      const info = await api.getAppInfo();
      setAppInfo(info);
      setShowAboutDialog(true);
    } catch { /* ignore */ }
  }

  function openNewQueryTab(sql = QUERY_PRESETS[0].sql, title = 'sql-query.sql') {
    const id = makeTabId('query');
    setEditorTabs((current) => [
      ...current,
      {
        id,
        kind: 'query',
        title,
        sql,
        result: null,
        sortState: null,
        currentPage: 0,
      },
    ]);
    setActiveEditorTabId(id);
    openSqlEditor();
    setOpenMenu(null);
  }

  function openOrActivateTableTab(schema: string, table: string, sql: string, result: QueryResult) {
    const existing = editorTabs.find((tab) => tab.kind === 'table' && tab.source?.schema === schema && tab.source?.table === table);
    if (existing) {
      setEditorTabs((current) =>
        current.map((tab) =>
          tab.id === existing.id
            ? { ...tab, sql, result, title: `${schema}.${table}`, source: { schema, table }, sortState: null, currentPage: 0 }
            : tab,
        ),
      );
      setActiveEditorTabId(existing.id);
      return;
    }

    const id = makeTabId('table');
    setEditorTabs((current) => [
      ...current,
      {
        id,
        kind: 'table',
        title: `${schema}.${table}`,
        sql,
        source: { schema, table },
        result,
        sortState: null,
        currentPage: 0,
      },
    ]);
    setActiveEditorTabId(id);
  }

  function closeEditorTab(id: string) {
    setEditorTabs((current) => {
      const next = current.filter((tab) => tab.id !== id);
      if (activeEditorTabId === id) {
        setActiveEditorTabId(next.length > 0 ? next[next.length - 1].id : '');
      }
      return next;
    });
  }

  function clearResultView() {
    updateActiveTab({ sortState: null, currentPage: 0 });
  }

  function toggleSort(columnIndex: number) {
    const current = activeEditorTab?.sortState ?? null;
    if (!current || current.columnIndex !== columnIndex) {
      updateActiveTab({ sortState: { columnIndex, direction: 'asc' } });
    } else if (current.direction === 'asc') {
      updateActiveTab({ sortState: { columnIndex, direction: 'desc' } });
    } else {
      updateActiveTab({ sortState: null });
    }
  }

  function getResultForExport() {
    if (!activeEditorTab?.result) return null;
    return activeEditorTab.result;
  }

  function exportCurrentResult() {
    exportResultAsCsv();
  }

  async function exportResultAsCsv() {
    const result = getResultForExport();
    if (!result) return;

    const csv = [result.columns, ...result.rows]
      .map((row) => row.map((cell) => `"${String(cell).replaceAll('"', '""')}"`).join(','))
      .join('\n');

    const path = await api.showSaveDialog({
      defaultPath: `query_results.csv`,
      filters: [{ name: 'CSV Files', extensions: ['csv'] }],
    });
    if (path) {
      await api.writeFile(path, csv);
      setLoading('Exported to CSV.');
      setTimeout(() => setLoading(''), 2000);
    }
  }

  async function exportResultAsExcel() {
    const result = getResultForExport();
    if (!result) return;

    // Build a simple XML spreadsheet (Excel-compatible)
    const escXml = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    const headerRow = result.columns.map((c) => `<Cell><Data ss:Type="String">${escXml(c)}</Data></Cell>`).join('');
    const dataRows = result.rows.map((row) => {
      const cells = row.map((cell) => {
        const isNum = cell !== 'NULL' && !isNaN(Number(cell)) && cell.trim() !== '';
        const type = isNum ? 'Number' : 'String';
        return `<Cell><Data ss:Type="${type}">${escXml(cell)}</Data></Cell>`;
      }).join('');
      return `<Row>${cells}</Row>`;
    }).join('\n');

    const xml = `<?xml version="1.0"?>\n<?mso-application progid="Excel.Sheet"?>\n<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"\n xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">\n<Worksheet ss:Name="Results"><Table>\n<Row>${headerRow}</Row>\n${dataRows}\n</Table></Worksheet></Workbook>`;

    const path = await api.showSaveDialog({
      defaultPath: `query_results.xls`,
      filters: [{ name: 'Excel Files', extensions: ['xls', 'xlsx'] }],
    });
    if (path) {
      await api.writeFile(path, xml);
      setLoading('Exported to Excel.');
      setTimeout(() => setLoading(''), 2000);
    }
  }

  function openConnectionModal(draftValue: ConnectionInput) {
    setDraft(draftValue);
    setPersistConnection(true);
    setTestStatus('idle');
    setTestError('');
    setConnectionTab('general');
    setShowConnectionModal(true);
    setOpenMenu(null);
  }

  function openNewConnectionModal() {
    openConnectionModal(EMPTY_CONNECTION);
  }

  function openConnectionFromPgpass(entry: PgpassEntry) {
    openConnectionModal({
      name: `${entry.database}@${entry.host}`,
      host: entry.host,
      port: entry.port,
      user: entry.user,
      password: '',
      database: entry.database,
      authMethod: 'pgpass',
      ssh: { ...EMPTY_SSH },
    });
  }

  function openEditConnectionModal(connection: SafeSavedConnection) {
    openConnectionModal({
      id: connection.id,
      name: connection.name,
      host: connection.host,
      port: connection.port,
      user: connection.user,
      password: '',
      database: connection.database,
      authMethod: connection.authMethod ?? 'password',
      ssh: connection.ssh ? { ...EMPTY_SSH, enabled: connection.ssh.enabled, host: connection.ssh.host, port: connection.ssh.port, user: connection.ssh.user, authMethod: connection.ssh.authMethod as 'password' | 'privateKey' } : { ...EMPTY_SSH },
    });
  }

  async function handleTestConnection() {
    try {
      setTestStatus('testing');
      setTestError('');
      await api.testConnection(draft);
      setTestStatus('success');
    } catch (err) {
      setTestStatus('fail');
      setTestError(errorMessage(err));
    }
  }

  async function handleConnect() {
    try {
      setLoading(draft.id ? 'Updating saved connection...' : 'Opening database connection...');
      setError(null);
      const next = await api.connect(draft, persistConnection);
      setSnapshot(next);
      setShowConnectionModal(false);
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleActivate(connection: SafeSavedConnection) {
    try {
      setLoading(`Connecting to ${connection.host}...`);
      setError(null);
      const next = await api.activateSavedConnection(connection.id);
      setSnapshot(next);
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleDeleteConnection(connection: SafeSavedConnection) {
    try {
      setLoading(`Removing ${connection.name || connection.host}...`);
      setError(null);
      setSnapshot(await api.deleteSavedConnection(connection.id));
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleDisconnect() {
    try {
      setLoading('Closing connection...');
      setError(null);
      setSnapshot(await api.disconnect());
      setOpenMenu(null);
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleRunQuery(sqlOverride?: string) {
    const sql = (sqlOverride ?? sqlEditorText).trim();
    if (!sql) return;

    try {
      setLoading('Running SQL...');
      setError(null);
      const nextResult = await api.runQuery(sql, 500);
      const title = summarizeSql(sql);

      const existingQueryTab = editorTabs.find((tab) => tab.kind === 'query');
      if (existingQueryTab) {
        setEditorTabs((current) =>
          current.map((tab) =>
            tab.id === existingQueryTab.id
              ? { ...tab, sql, result: nextResult, title, sortState: null, currentPage: 0 }
              : tab,
          ),
        );
        setActiveEditorTabId(existingQueryTab.id);
      } else {
        const id = makeTabId('query');
        setEditorTabs((current) => [
          ...current,
          { id, kind: 'query', title, sql, result: nextResult, sortState: null, currentPage: 0 },
        ]);
        setActiveEditorTabId(id);
      }

      setOpenMenu(null);
      setQueryHistory((current) => [
        {
          id: crypto.randomUUID(),
          title,
          sql,
          resultMeta: formatDuration(nextResult.executionTimeMs),
        },
        ...current,
      ].slice(0, 12));
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleSaveAs() {
    const text = sqlEditorText.trim();
    if (!text) return;
    try {
      const path = await api.showSaveDialog({
        defaultPath: 'query.sql',
        filters: [{ name: 'SQL Files', extensions: ['sql'] }],
      });
      if (path) {
        await api.writeFile(path, text);
      }
    } catch (err) {
      setError(errorMessage(err));
    }
  }

  async function handlePreviewTable(schema: string, table: string) {
    setActivePanel(null);
    try {
      setLoading(`Loading ${schema}.${table}...`);
      setError(null);
      const nextResult = await api.previewTable(schema, table, 200, 0);
      const sql = `select *\nfrom ${schema}.${table}\nlimit 200;`;
      openOrActivateTableTab(schema, table, sql, nextResult);
      clearResultView();
      setQueryHistory((current) => [
        {
          id: crypto.randomUUID(),
          title: `${schema}.${table}`,
          sql,
          resultMeta: formatDuration(nextResult.executionTimeMs),
        },
        ...current,
      ].slice(0, 12));
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleExportTable(schema: string, table: string) {
    try {
      setLoading(`Exporting ${schema}.${table}...`);
      setError(null);
      const result = await api.previewTable(schema, table, 10000, 0);
      const header = result.columns;
      const csv = [header, ...result.rows]
        .map((row) => row.map((cell) => `"${String(cell).replaceAll('"', '""')}"`).join(','))
        .join('\n');
      const path = await api.showSaveDialog({
        defaultPath: `${schema}.${table}.csv`,
        filters: [{ name: 'CSV Files', extensions: ['csv'] }],
      });
      if (path) {
        await api.writeFile(path, csv);
      }
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleExportFullTableCsv(schema: string, table: string) {
    try {
      const path = await api.showSaveDialog({
        defaultPath: `${schema}.${table}.csv`,
        filters: [{ name: 'CSV Files', extensions: ['csv'] }],
      });
      if (!path) return;
      setLoading(`Exporting ${schema}.${table} (full table)...`);
      setError(null);
      const rowCount = await api.exportTableCsv(schema, table, path);
      setLoading(`Exported ${rowCount} rows.`);
      setTimeout(() => setLoading(''), 3000);
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleExportPgDump(schema: string, table: string) {
    try {
      const path = await api.showSaveDialog({
        defaultPath: `${schema}.${table}.sql`,
        filters: [
          { name: 'SQL Files', extensions: ['sql'] },
          { name: 'Custom Format', extensions: ['dump'] },
          { name: 'Tar Archive', extensions: ['tar'] },
        ],
      });
      if (!path) return;
      const ext = path.split('.').pop()?.toLowerCase();
      const format = ext === 'dump' ? 'custom' : ext === 'tar' ? 'tar' : 'sql';
      setLoading(`Running pg_dump for ${schema}.${table}...`);
      setError(null);
      await api.exportPgDump(schema, table, path, format);
      setLoading(`pg_dump export complete.`);
      setTimeout(() => setLoading(''), 3000);
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleExportParquet(schema: string, table: string) {
    try {
      const path = await api.showSaveDialog({
        defaultPath: `${schema}.${table}.parquet`,
        filters: [{ name: 'Parquet Files', extensions: ['parquet'] }],
      });
      if (!path) return;
      setLoading(`Exporting ${schema}.${table} to Parquet...`);
      setError(null);
      const count = await api.exportTableParquet(schema, table, path);
      setLoading(`Exported ${count} rows to Parquet.`);
      setTimeout(() => setLoading(''), 3000);
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleDropTable(schema: string, table: string, cascade: boolean) {
    try {
      setLoading(`Dropping ${schema}.${table}${cascade ? ' (CASCADE)' : ''}...`);
      setError(null);
      const next = await api.dropTable(schema, table, cascade);
      setSnapshot(next);
      // Close any editor tabs referencing this table
      setEditorTabs((tabs) => tabs.filter((t) => !(t.source?.schema === schema && t.source?.table === table)));
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleTruncateTable(schema: string, table: string, cascade: boolean) {
    try {
      setLoading(`Truncating ${schema}.${table}${cascade ? ' (CASCADE)' : ''}...`);
      setError(null);
      const next = await api.truncateTable(schema, table, cascade);
      setSnapshot(next);
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleShowDdl(schema: string, table: string) {
    try {
      setLoading(`Loading DDL for ${schema}.${table}...`);
      setError(null);
      const result = await api.getTableDdl(schema, table);

      const existing = editorTabs.find((tab) => tab.kind === 'ddl' && tab.source?.schema === schema && tab.source?.table === table);
      if (existing) {
        setEditorTabs((current) =>
          current.map((tab) =>
            tab.id === existing.id
              ? { ...tab, ddlText: result.ddl, title: `${table} DDL` }
              : tab,
          ),
        );
        setActiveEditorTabId(existing.id);
      } else {
        const id = makeTabId('ddl');
        setEditorTabs((current) => [
          ...current,
          {
            id,
            kind: 'ddl',
            title: `${table} DDL`,
            sql: '',
            source: { schema, table },
            result: null,
            sortState: null,
            currentPage: 0,
            ddlText: result.ddl,
          },
        ]);
        setActiveEditorTabId(id);
      }
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleOpenEditData(schema: string, table: string) {
    try {
      setLoading(`Loading editable data for ${schema}.${table}...`);
      setError(null);
      const pageSize = 200;
      const tableData = await api.getEditableTableData(schema, table, pageSize, 0);

      if (tableData.primaryKeyColumns.length === 0) {
        setLoading('');
        setError(`Cannot edit data: table ${schema}.${table} has no primary key.`);
        return;
      }

      const editState: EditDataState = {
        tableData,
        editedCells: new Map(),
        deletedRows: new Set(),
        newRows: [],
        editingCell: null,
        page: 0,
        pageSize,
      };

      const existing = editorTabs.find((tab) => tab.kind === 'editdata' && tab.source?.schema === schema && tab.source?.table === table);
      if (existing) {
        setEditorTabs((current) =>
          current.map((tab) =>
            tab.id === existing.id
              ? { ...tab, editData: editState, title: `Edit ${schema}.${table}` }
              : tab,
          ),
        );
        setActiveEditorTabId(existing.id);
      } else {
        const id = makeTabId('editdata');
        setEditorTabs((current) => [
          ...current,
          {
            id,
            kind: 'editdata',
            title: `Edit ${schema}.${table}`,
            sql: '',
            source: { schema, table },
            result: null,
            sortState: null,
            currentPage: 0,
            editData: editState,
          },
        ]);
        setActiveEditorTabId(id);
      }
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  function updateEditData(patch: Partial<EditDataState>) {
    if (!activeEditorTab?.editData) return;
    setEditorTabs((current) =>
      current.map((tab) =>
        tab.id === activeEditorTab.id
          ? { ...tab, editData: { ...tab.editData!, ...patch } }
          : tab,
      ),
    );
  }

  function editDataSetCell(rowIdx: number, colIdx: number, value: string | null, isNew: boolean) {
    if (!activeEditorTab?.editData) return;
    if (isNew) {
      const newRows = [...activeEditorTab.editData.newRows];
      newRows[rowIdx] = [...newRows[rowIdx]];
      newRows[rowIdx][colIdx] = value;
      updateEditData({ newRows });
    } else {
      const edited = new Map(activeEditorTab.editData.editedCells);
      const key = `${rowIdx}:${colIdx}`;
      const original = activeEditorTab.editData.tableData.rows[rowIdx]?.[colIdx] ?? null;
      if (value === original) {
        edited.delete(key);
      } else {
        edited.set(key, value);
      }
      updateEditData({ editedCells: edited });
    }
  }

  function editDataToggleDeleteRow(rowIdx: number) {
    if (!activeEditorTab?.editData) return;
    const deleted = new Set(activeEditorTab.editData.deletedRows);
    if (deleted.has(rowIdx)) {
      deleted.delete(rowIdx);
    } else {
      deleted.add(rowIdx);
    }
    updateEditData({ deletedRows: deleted });
  }

  function editDataAddRow() {
    if (!activeEditorTab?.editData) return;
    const cols = activeEditorTab.editData.tableData.columns;
    const newRow: (string | null)[] = cols.map(() => null);
    updateEditData({ newRows: [...activeEditorTab.editData.newRows, newRow] });
  }

  function editDataRemoveNewRow(idx: number) {
    if (!activeEditorTab?.editData) return;
    updateEditData({ newRows: activeEditorTab.editData.newRows.filter((_, i) => i !== idx) });
  }

  function editDataDiscard() {
    if (!activeEditorTab?.editData) return;
    updateEditData({
      editedCells: new Map(),
      deletedRows: new Set(),
      newRows: [],
      editingCell: null,
    });
  }

  async function editDataApply() {
    if (!activeEditorTab?.editData || !activeEditorTab.source) return;
    const { schema, table } = activeEditorTab.source;
    const { tableData, editedCells, deletedRows, newRows } = activeEditorTab.editData;
    const { columns, columnTypes, primaryKeyColumns } = tableData;
    const operations: DmlOperation[] = [];

    // Build column name -> pg type map for proper casting
    const colTypeMap: Record<string, string> = {};
    for (let i = 0; i < columns.length; i++) {
      colTypeMap[columns[i]] = columnTypes[i];
    }

    const pkIndexes = primaryKeyColumns.map((pk) => columns.indexOf(pk));

    // Deletes
    for (const rowIdx of deletedRows) {
      const row = tableData.rows[rowIdx];
      if (!row) continue;
      const pkValues: Record<string, string | null> = {};
      for (let pi = 0; pi < primaryKeyColumns.length; pi++) {
        pkValues[primaryKeyColumns[pi]] = row[pkIndexes[pi]];
      }
      operations.push({ type: 'delete', pkValues });
    }

    // Updates - group edits by row
    const editsByRow = new Map<number, Map<number, string | null>>();
    for (const [key, val] of editedCells) {
      const [r, c] = key.split(':').map(Number);
      if (deletedRows.has(r)) continue;
      if (!editsByRow.has(r)) editsByRow.set(r, new Map());
      editsByRow.get(r)!.set(c, val);
    }
    for (const [rowIdx, cellChanges] of editsByRow) {
      const row = tableData.rows[rowIdx];
      if (!row) continue;
      const pkValues: Record<string, string | null> = {};
      for (let pi = 0; pi < primaryKeyColumns.length; pi++) {
        pkValues[primaryKeyColumns[pi]] = row[pkIndexes[pi]];
      }
      const changes: Record<string, string | null> = {};
      for (const [colIdx, val] of cellChanges) {
        changes[columns[colIdx]] = val;
      }
      operations.push({ type: 'update', pkValues, changes, columnTypes: colTypeMap });
    }

    // Inserts
    for (const newRow of newRows) {
      const values: Record<string, string | null> = {};
      let hasAnyValue = false;
      for (let ci = 0; ci < columns.length; ci++) {
        if (newRow[ci] !== null) {
          values[columns[ci]] = newRow[ci];
          hasAnyValue = true;
        }
      }
      if (hasAnyValue) {
        operations.push({ type: 'insert', values, columnTypes: colTypeMap });
      }
    }

    if (operations.length === 0) return;

    try {
      setLoading('Applying changes...');
      setError(null);
      await api.executeDml(schema, table, operations);
      // Reload data
      const tableData = await api.getEditableTableData(schema, table, activeEditorTab.editData.pageSize, activeEditorTab.editData.page * activeEditorTab.editData.pageSize);
      updateEditData({
        tableData,
        editedCells: new Map(),
        deletedRows: new Set(),
        newRows: [],
        editingCell: null,
      });
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function editDataChangePage(newPage: number) {
    if (!activeEditorTab?.editData || !activeEditorTab.source) return;
    const { schema, table } = activeEditorTab.source;
    const { pageSize } = activeEditorTab.editData;
    try {
      setLoading('Loading page...');
      const tableData = await api.getEditableTableData(schema, table, pageSize, newPage * pageSize);
      updateEditData({
        tableData,
        editedCells: new Map(),
        deletedRows: new Set(),
        newRows: [],
        editingCell: null,
        page: newPage,
      });
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  async function handleOpenModifyTable(schema: string, table: string) {
    try {
      setLoading(`Loading table info for ${schema}.${table}...`);
      setError(null);
      const info = await api.getModifyTableInfo(schema, table);
      setModifyTableInfo(info);
      setModifyTableDraft({
        columns: info.columns.map((c) => ({ ...c })),
        newTableName: info.table,
        addColumns: [],
        dropColumns: new Set(),
        addForeignKeys: [],
        addIndexes: [],
      });
      setModifyTableError('');
      setLoading('');
    } catch (err) {
      setLoading('');
      setError(errorMessage(err));
    }
  }

  function buildModifyActions(): AlterTableAction[] {
    if (!modifyTableInfo) return [];
    const actions: AlterTableAction[] = [];
    const { table, columns: originalColumns } = modifyTableInfo;

    if (modifyTableDraft.newTableName !== table) {
      actions.push({ type: 'rename_table', newTableName: modifyTableDraft.newTableName });
    }

    for (const colName of modifyTableDraft.dropColumns) {
      actions.push({ type: 'drop_column', columnName: colName });
    }

    for (let i = 0; i < originalColumns.length; i++) {
      const orig = originalColumns[i];
      const draft = modifyTableDraft.columns[i];
      if (!draft || modifyTableDraft.dropColumns.has(orig.name)) continue;

      if (draft.name !== orig.name) {
        actions.push({ type: 'rename_column', columnName: orig.name, newColumnName: draft.name });
      }
      if (draft.dataType !== orig.dataType) {
        actions.push({ type: 'alter_type', columnName: draft.name !== orig.name ? draft.name : orig.name, dataType: draft.dataType });
      }
      if (draft.nullable !== orig.nullable) {
        actions.push({ type: draft.nullable ? 'drop_not_null' : 'set_not_null', columnName: draft.name !== orig.name ? draft.name : orig.name });
      }
      const origDefault = orig.defaultValue ?? '';
      const draftDefault = draft.defaultValue ?? '';
      if (draftDefault !== origDefault) {
        if (draftDefault) {
          actions.push({ type: 'set_default', columnName: draft.name !== orig.name ? draft.name : orig.name, defaultValue: draftDefault });
        } else {
          actions.push({ type: 'drop_default', columnName: draft.name !== orig.name ? draft.name : orig.name });
        }
      }
    }

    for (const col of modifyTableDraft.addColumns) {
      if (!col.name.trim() || !col.dataType.trim()) continue;
      actions.push({
        type: 'add_column',
        columnName: col.name,
        dataType: col.dataType,
        nullable: col.nullable,
        defaultValue: col.defaultValue || null,
      });
    }

    return actions;
  }

  const modifyPreviewDdl = useMemo(() => {
    if (!modifyTableInfo) return '';
    const rawActions = buildModifyActions();
    if (rawActions.length === 0) return '-- No changes';
    // Show rename last to match execution order
    const actions = [...rawActions.filter((a) => a.type !== 'rename_table'), ...rawActions.filter((a) => a.type === 'rename_table')];
    const qt = `${qi(modifyTableInfo.schema)}.${qi(modifyTableInfo.table)}`;
    return actions.map((a) => {
      switch (a.type) {
        case 'rename_table':
          return `ALTER TABLE ${qt} RENAME TO ${qi(a.newTableName!)};`;
        case 'drop_column':
          return `ALTER TABLE ${qt} DROP COLUMN ${qi(a.columnName!)};`;
        case 'rename_column':
          return `ALTER TABLE ${qt} RENAME COLUMN ${qi(a.columnName!)} TO ${qi(a.newColumnName!)};`;
        case 'alter_type':
          return `ALTER TABLE ${qt} ALTER COLUMN ${qi(a.columnName!)} TYPE ${a.dataType!};`;
        case 'set_not_null':
          return `ALTER TABLE ${qt} ALTER COLUMN ${qi(a.columnName!)} SET NOT NULL;`;
        case 'drop_not_null':
          return `ALTER TABLE ${qt} ALTER COLUMN ${qi(a.columnName!)} DROP NOT NULL;`;
        case 'set_default':
          return `ALTER TABLE ${qt} ALTER COLUMN ${qi(a.columnName!)} SET DEFAULT ${a.defaultValue!};`;
        case 'drop_default':
          return `ALTER TABLE ${qt} ALTER COLUMN ${qi(a.columnName!)} DROP DEFAULT;`;
        case 'add_column': {
          let sql = `ALTER TABLE ${qt} ADD COLUMN ${qi(a.columnName!)} ${a.dataType!}`;
          if (!a.nullable) sql += ' NOT NULL';
          if (a.defaultValue) sql += ` DEFAULT ${a.defaultValue}`;
          return sql + ';';
        }
        default:
          return '';
      }
    }).join('\n');
  }, [modifyTableInfo, modifyTableDraft]);

  const modifyAllCols = useMemo(() => {
    if (!modifyTableInfo) return [];
    return [
      ...modifyTableDraft.columns.filter((c) => !modifyTableDraft.dropColumns.has(modifyTableInfo.columns[modifyTableDraft.columns.indexOf(c)]?.name ?? '')).map((c) => c.name),
      ...modifyTableDraft.addColumns.filter((c) => c.name.trim()).map((c) => c.name.trim()),
    ];
  }, [modifyTableInfo, modifyTableDraft]);

  async function handleApplyModifyTable() {
    if (!modifyTableInfo) return;
    const actions = buildModifyActions();
    const fks = modifyTableDraft.addForeignKeys.filter((fk) => fk.column && fk.refTable && fk.refColumn);
    const idxs = modifyTableDraft.addIndexes.filter((idx) => idx.columns.trim());

    if (actions.length === 0 && fks.length === 0 && idxs.length === 0) {
      setModifyTableInfo(null);
      return;
    }

    try {
      setLoading('Applying table modifications...');
      setModifyTableError('');
      const qt = `${qi(modifyTableInfo.schema)}.${qi(modifyTableInfo.table)}`;

      if (actions.length > 0) {
        await api.alterTable(modifyTableInfo.schema, modifyTableInfo.table, actions);
      }
      // Add foreign keys
      for (const fk of fks) {
        const refParts = fk.refTable.includes('.') ? fk.refTable.split('.', 2) : [fk.refTable];
        const quotedRef = refParts.map((p) => qi(p.trim())).join('.');
        await api.runQuery(`ALTER TABLE ${qt} ADD FOREIGN KEY (${qi(fk.column)}) REFERENCES ${quotedRef} (${qi(fk.refColumn)})`);
      }
      // Add indexes
      for (const idx of idxs) {
        const tableName = modifyTableDraft.newTableName || modifyTableInfo.table;
        const idxName = idx.name.trim() || `idx_${tableName}_${idx.columns.trim().replace(/,\s*/g, '_')}`;
        const unique = idx.unique ? 'UNIQUE ' : '';
        const quotedCols = idx.columns.split(',').map((c) => qi(c.trim())).join(', ');
        await api.runQuery(`CREATE ${unique}INDEX ${qi(idxName)} ON ${qt} (${quotedCols})`);
      }

      const refreshed = await api.bootstrap();
      setSnapshot(refreshed);
      setModifyTableInfo(null);
      setLoading('');
    } catch (err) {
      setLoading('');
      setModifyTableError(errorMessage(err));
    }
  }

  return (
    <main className="h-screen bg-transparent text-[13px] text-black" onClick={() => { if (openMenu) setOpenMenu(null); setContextMenu(null); setConnectionContextMenu(null); setDatabaseContextMenu(null); setSchemaContextMenu(null); }}>
      <div className="flex h-screen flex-col overflow-hidden">
        <header className="glass-panel relative m-1.5 mb-0 shrink-0 overflow-visible rounded-lg">
          <div className="flex h-7 items-center justify-between px-3">
            <div className="flex min-w-0 items-center gap-4" onClick={(event) => event.stopPropagation()}>
              <div className="rounded-md bg-[var(--accent)] px-2 py-[3px] text-[10px] font-bold uppercase tracking-[0.14em] text-white">
                PostGrip
              </div>
              <div className="flex items-center gap-1 text-[12px] text-black/60">
                <DropdownMenu label="File" active={openMenu === 'file'} onToggle={() => setOpenMenu((c) => c === 'file' ? null : 'file')}>
                  <MenuItem label="Exit" onClick={() => void handleExit()} />
                </DropdownMenu>
                <MenuButton active={false} label="SQL Editor" onClick={() => { openSqlEditor(); setOpenMenu(null); }} />
                <MenuButton active={showMonitoringPanel} label="Monitoring" onClick={() => { void openMonitoringPanel(); setOpenMenu(null); }} />
                <MenuButton active={showBackupPanel} label="Backup & Restore" onClick={() => { void openBackupPanel(); setOpenMenu(null); }} />
                <DropdownMenu label="Help" active={openMenu === 'help'} onToggle={() => setOpenMenu((c) => c === 'help' ? null : 'help')}>
                  <MenuItem label="Help" onClick={() => void openHelpViewer()} />
                  <MenuItem label="About PostGrip" onClick={() => void openAboutDialog()} />
                </DropdownMenu>
              </div>
            </div>

            <div className="truncate text-[11px] uppercase tracking-[0.12em] text-black/40">
              {snapshot?.activeConnection ? snapshot.activeConnection.database : 'No active database'}
            </div>
          </div>

          <div className="flex items-center gap-1 border-t border-black/5 bg-white px-3 py-0.5 text-black/50">
            <ToolbarIconButton onClick={openNewConnectionModal} title="New connection"><PlusIcon /></ToolbarIconButton>
            <ToolbarIconButton onClick={() => openNewQueryTab()} title="New query"><QueryIcon /></ToolbarIconButton>
            <ToolbarIconButton onClick={() => openSqlEditor()} title="Open SQL editor"><PanelIcon /></ToolbarIconButton>
            <ToolbarIconButton onClick={() => void refresh()} title="Refresh"><RefreshIcon /></ToolbarIconButton>
            <ToolbarIconButton onClick={() => { exportCurrentResult(); }} title="Export CSV"><ExportIcon /></ToolbarIconButton>
          </div>
        </header>

        <div className="flex flex-1 gap-1.5 overflow-hidden px-1.5 pb-0 pt-1.5">
          <aside className="flex min-h-0 shrink-0 flex-col gap-1.5 text-[12px]" style={{ width: sidebarWidth }}>
            <div className="glass-panel flex shrink-0 flex-col overflow-hidden rounded-xl" style={{ height: connectionsHeight }}>
              <div className="flex h-7 items-center justify-between border-b border-black/5 px-3">
                <div className="text-[13px] font-medium text-black">Connections</div>
                <ToolbarIconButton onClick={openNewConnectionModal} title="New connection"><PlusIcon /></ToolbarIconButton>
              </div>
              <div className="sidebar-scroll flex-1 overflow-y-scroll rounded-b-xl bg-white px-2 py-2">
                {snapshot?.savedConnections.length ? (
                  <div>
                    {snapshot.savedConnections.map((connection) => {
                      const active = snapshot.activeConnection?.id === connection.id;
                      return (
                        <div className="group flex items-center gap-2 px-1 py-1" key={connection.id} onContextMenu={(event) => { event.preventDefault(); setConnectionContextMenu({ x: event.clientX, y: event.clientY, connection }); }}>
                          <ExplorerIcon><ConnectionIcon active={active} /></ExplorerIcon>
                          <button className="min-w-0 flex-1 truncate text-left text-black" onClick={() => void handleActivate(connection)} type="button">
                            {`${connection.database}@${connection.host}`}
                          </button>
                        </div>
                      );
                    })}
                  </div>
                ) : null}
                {unsavedPgpass.length > 0 ? (
                  <div>
                    <div className="px-1 pb-1 pt-2 text-[10px] font-medium uppercase tracking-wider text-black/40">.pgpass</div>
                    {unsavedPgpass.map((entry) => (
                      <div className="group flex items-center gap-2 px-1 py-1" key={`${entry.host}:${entry.port}:${entry.database}:${entry.user}`}>
                        <ExplorerIcon><PgpassIcon /></ExplorerIcon>
                        <button className="min-w-0 flex-1 truncate text-left text-black/50" onClick={() => openConnectionFromPgpass(entry)} type="button">
                          {`${entry.database}@${entry.host}`}
                        </button>
                      </div>
                    ))}
                  </div>
                ) : !snapshot?.savedConnections.length ? <EmptyInline message="No saved connections" /> : null}
              </div>
            </div>

            <div className="h-px shrink-0 cursor-row-resize bg-black/10 hover:bg-[var(--accent)]" onPointerDown={(event) => { event.stopPropagation(); setDragState('connections'); }} />

            <div className="glass-panel flex min-h-0 flex-1 flex-col overflow-hidden rounded-xl">
              <div className="flex h-7 items-center justify-between border-b border-black/5 px-3">
                <div className="text-[13px] font-medium text-black">Explorer</div>
                <div className="flex items-center gap-1 text-gray-500">
                  {explorerTab === 'database' ? (
                    <ToolbarIconButton onClick={() => void refresh()} title="Refresh"><RefreshIcon /></ToolbarIconButton>
                  ) : explorerTab === 'files' ? (
                    <ToolbarIconButton onClick={() => { if (currentDir) { const parent = currentDir.replace(/\/[^/]+$/, '') || '/'; void browseDirectory(parent); } }} title="Go up"><ChevronUpIcon /></ToolbarIconButton>
                  ) : (
                    <ToolbarIconButton onClick={() => void refreshGitStatus()} title="Refresh"><RefreshIcon /></ToolbarIconButton>
                  )}
                  <ToolbarIconButton onClick={() => openSqlEditor()} title="Open SQL editor"><PanelIcon /></ToolbarIconButton>
                </div>
              </div>
              <div className="flex items-center border-b border-black/5 bg-white px-1 pt-1">
                <button
                  className={classNames(
                    'flex items-center gap-1.5 border-r border-black/5 px-3 py-1.5 text-[12px] rounded-t-lg',
                    explorerTab === 'database' ? 'bg-white text-black' : 'bg-gray-50 text-gray-400',
                  )}
                  onClick={() => setExplorerTab('database')}
                  type="button"
                >
                  <span className="text-[11px] text-gray-500"><DatabaseIcon /></span>
                  Database
                </button>
                <button
                  className={classNames(
                    'flex items-center gap-1.5 border-r border-black/5 px-3 py-1.5 text-[12px] rounded-t-lg',
                    explorerTab === 'files' ? 'bg-white text-black' : 'bg-gray-50 text-gray-400',
                  )}
                  onClick={() => setExplorerTab('files')}
                  type="button"
                >
                  <span className="text-[11px] text-gray-500"><FolderIcon /></span>
                  Files
                </button>
                <button
                  className={classNames(
                    'flex items-center gap-1.5 border-r border-black/5 px-3 py-1.5 text-[12px] rounded-t-lg',
                    explorerTab === 'git' ? 'bg-white text-black' : 'bg-gray-50 text-gray-400',
                  )}
                  onClick={() => { setExplorerTab('git'); void refreshGitStatus(); }}
                  type="button"
                >
                  <span className="text-[11px] text-gray-500"><GitIcon /></span>
                  Git
                  {gitStatus && gitStatus.files.length > 0 ? (
                    <span className="rounded-full bg-amber-100 px-1.5 text-[10px] font-medium text-amber-700">{gitStatus.files.length}</span>
                  ) : null}
                </button>
              </div>
              <div className="sidebar-scroll min-h-0 flex-1 overflow-y-scroll rounded-b-xl bg-white px-2 py-2">
                {explorerTab === 'database' ? (
                  snapshot?.activeConnection ? (
                    <DatabaseTree
                      connection={snapshot.activeConnection}
                      tree={databaseTree}
                      onPreview={handlePreviewTable}
                      onTableContextMenu={(event, schema, table) => {
                        event.preventDefault();
                        setContextMenu({ x: event.clientX, y: event.clientY, schema, table });
                      }}
                      onDatabaseContextMenu={(event) => {
                        setDatabaseContextMenu({ x: event.clientX, y: event.clientY });
                      }}
                      onSchemaContextMenu={(event, schema) => {
                        setSchemaContextMenu({ x: event.clientX, y: event.clientY, schema });
                      }}
                    />
                  ) : (
                    <EmptyInline message="Connect to browse schema" />
                  )
                ) : explorerTab === 'files' ? (
                  <FileTree
                    entries={fileEntries}
                    currentDir={currentDir}
                    expandedDirs={expandedDirs}
                    onNavigate={browseDirectory}
                    onToggleDir={toggleSubDir}
                    onOpenFile={openFileInTab}
                  />
                ) : (
                  <GitPanel
                    gitStatus={gitStatus}
                    gitRepoPath={gitRepoPath}
                    gitRepos={gitRepos}
                    onSwitchRepo={switchGitRepo}
                    onOpenDiff={openGitDiff}
                    onOpenFile={openFileInTab}
                  />
                )}
              </div>
            </div>
          </aside>

          <div className="w-px shrink-0 cursor-col-resize bg-black/10 hover:bg-[var(--accent)]" onPointerDown={(event) => { event.stopPropagation(); setDragState('sidebar'); }} />

          <div className="flex min-w-0 flex-1 flex-col gap-1.5">
            <div className="glass-panel flex shrink-0 flex-col overflow-hidden rounded-xl" style={{ height: connectionsHeight }}>
              <div className="flex h-7 items-center justify-between border-b border-black/5 px-3">
                <div className="text-[13px] font-medium text-black">Dashboard</div>
              </div>
              <div className="flex flex-1 gap-3 overflow-hidden rounded-b-xl bg-white px-3 py-3">
                <div className="flex w-1/2 shrink-0 gap-2 overflow-hidden">
                  <DashboardGraphCard label={"Server\nCPU"} value={hostStats?.cpuUsagePercent != null ? `${hostStats.cpuUsagePercent}%` : '—'} data={cpuHistory} max={100} color="#3b82f6" warnColor="#ef4444" warn={hostStats?.cpuUsagePercent != null && hostStats.cpuUsagePercent > 80} />
                  <DashboardGraphCard label={"Server\nRAM"} value={hostStats?.memUsagePercent != null ? `${hostStats.memUsagePercent}%` : '—'} subtitle={hostStats?.memUsedMb != null ? `${hostStats.memUsedMb}/${hostStats.memTotalMb} MB` : undefined} data={ramHistory} max={100} color="#8b5cf6" warnColor="#ef4444" warn={hostStats?.memUsagePercent != null && hostStats.memUsagePercent > 80} />
                  <DashboardGraphCard label={"Cache\nHit"} value={hostStats?.cacheHitRatio != null ? `${hostStats.cacheHitRatio}%` : '—'} data={cacheHitHistory} max={100} color="#06b6d4" />
                  <DashboardGraphCard label="TXN Throughput" value={hostStats?.tps != null ? `${hostStats.tps} tps` : '—'} data={tpsHistory} color="#f59e0b" />
                  <DashboardGraphCard label="Conn Saturation" value={hostStats?.connectionSaturationPercent != null ? `${hostStats.connectionSaturationPercent}%` : '—'} subtitle={hostStats?.activeConnections != null ? `${hostStats.activeConnections}/${hostStats.maxConnections}` : undefined} data={saturationHistory} max={100} color="#10b981" warnColor="#ef4444" warn={hostStats?.connectionSaturationPercent != null && hostStats.connectionSaturationPercent > 80} />
                </div>
                <div className="flex w-1/2 flex-col overflow-hidden rounded-lg border border-black/5">
                  <div className="flex h-7 shrink-0 items-center border-b border-black/5 bg-gray-50 px-3">
                    <span className="text-[12px] font-medium text-gray-600">Query History</span>
                    <span className="ml-auto text-[11px] text-gray-400">{queryHistory.length} queries</span>
                  </div>
                  <div className="flex-1 overflow-y-auto">
                    {queryHistory.length === 0 ? (
                      <div className="flex h-full items-center justify-center text-[12px] text-gray-400">No queries yet</div>
                    ) : (
                      <table className="w-full text-[11px]">
                        <tbody>
                          {queryHistory.map((entry) => (
                            <tr key={entry.id} className="group relative border-b border-black/5 hover:bg-gray-50">
                              <td className="w-full px-2 py-1 font-mono text-gray-700">
                                <div className="truncate">{entry.sql}</div>
                                <div className="pointer-events-none absolute left-2 top-full z-50 hidden max-h-[200px] max-w-[500px] overflow-y-auto whitespace-pre-wrap break-all rounded-lg border border-black/10 bg-white px-3 py-2 font-mono text-[11px] text-gray-800 shadow-lg group-hover:block">{entry.sql}</div>
                              </td>
                              <td className="whitespace-nowrap px-2 py-1 text-right align-top tabular-nums text-gray-500">{entry.resultMeta}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {!showBackupPanel && !showMonitoringPanel ? <div className="h-px shrink-0 cursor-row-resize bg-black/10 hover:bg-[var(--accent)]" /> : null}

            {showSqlEditor && !showBackupPanel && !showMonitoringPanel ? (
              <div className="glass-panel flex h-[38%] shrink-0 flex-col overflow-hidden rounded-xl">
                <div className="flex h-7 items-center justify-between border-b border-black/5 px-3">
                  <div className="text-[13px] font-medium text-black">SQL Editor</div>
                  <button className="px-1 py-0.5 text-[12px] leading-tight text-gray-500 hover:text-black" onClick={() => { setShowSqlEditor(false); setSqlTabs([]); setActiveSqlTabId(null); setEditorTabs([]); setActiveEditorTabId(''); }} type="button">Close</button>
                </div>
                <div className="flex items-center border-b border-black/5 bg-white px-1 pt-1">
                  <div className="flex min-w-0 flex-1 overflow-auto">
                    {sqlTabs.map((tab) => (
                      <button
                        className={classNames(
                          'group flex min-w-[100px] max-w-[200px] items-center gap-2 border-r border-black/5 px-3 py-1.5 text-left rounded-t-lg',
                          tab.id === activeSqlTab?.id ? 'bg-white text-black' : 'bg-gray-50 text-gray-400',
                        )}
                        key={tab.id}
                        onClick={() => setActiveSqlTabId(tab.id)}
                        type="button"
                      >
                        <span className="text-[11px] text-gray-500"><QueryIcon /></span>
                        <span className="truncate text-[12px]">{tab.title}</span>
                        <span
                          role="button"
                          tabIndex={-1}
                          className="ml-auto shrink-0 cursor-pointer px-1 text-gray-500 hover:text-black"
                          onPointerDown={(event) => { event.stopPropagation(); event.preventDefault(); closeSqlTab(tab.id); }}
                        >
                          x
                        </span>
                      </button>
                    ))}
                    <button className="px-2 py-1 text-[18px] font-bold text-gray-400 hover:text-black" onClick={addSqlTab} type="button" title="New tab">+</button>
                  </div>
                </div>
                <div className="flex min-h-0 flex-1 rounded-b-xl bg-white">
                  <div className="flex min-w-0 flex-1 flex-col">
                    <div className="flex items-center gap-3 border-b border-black/5 px-2 py-1">
                      <button className="rounded-lg bg-[var(--accent)] px-3 py-0.5 text-[12px] leading-tight text-white hover:opacity-90" onClick={() => void handleRunQuery(sqlEditorText)} type="button">Run</button>
                      <button className="px-1 py-0.5 text-[12px] leading-tight text-gray-500 hover:text-black" onClick={() => void handleSaveAs()} type="button">Save As</button>
                    </div>
                    <SqlEditor
                      value={sqlEditorText}
                      onChange={setSqlEditorText}
                      onRun={(text) => void handleRunQuery(text)}
                      databaseTree={databaseTree}
                    />
                  </div>
                  <div className="flex w-[270px] shrink-0 flex-col gap-1 overflow-y-auto border-l border-black/5 px-2 py-1">
                    <span className="text-[12px] font-medium text-gray-500">History</span>
                    {queryHistory.map((entry) => (
                      <button
                        key={entry.id}
                        className="rounded-lg bg-white/40 px-2 py-1 text-left text-[12px] text-gray-600 hover:bg-white/60 hover:text-black"
                        onClick={() => setSqlEditorText(entry.sql)}
                        title={entry.sql}
                        type="button"
                      >
                        <div className="line-clamp-2 break-all">{entry.sql}</div>
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            ) : null}

            {showMonitoringPanel ? (
              <div className="glass-panel flex min-h-0 flex-1 flex-col overflow-hidden rounded-xl">
                <div className="flex h-7 items-center justify-between border-b border-black/5 px-3">
                  <div className="text-[13px] font-medium text-black">Monitoring</div>
                  <div className="flex items-center gap-2">
                    <button className="px-1 py-0.5 text-[12px] leading-tight text-gray-500 hover:text-black" onClick={() => void refreshMonitoring()} type="button">Refresh</button>
                    <button className="px-1 py-0.5 text-[12px] leading-tight text-gray-500 hover:text-black" onClick={() => setActivePanel(null)} type="button">Close</button>
                  </div>
                </div>
                <div className="min-h-0 flex-1 overflow-y-auto rounded-b-xl bg-white">
                  <MonitoringPanel hostStats={hostStats} monitoringData={monitoringData} activeQueries={null}
                    cpuHistory={cpuHistory} ramHistory={ramHistory} tpsHistory={tpsHistory}
                    saturationHistory={saturationHistory} cacheHitHistory={cacheHitHistory}
                    queryHistory={queryHistory} />
                </div>
              </div>
            ) : null}

            {showBackupPanel ? (
              <div className="glass-panel flex min-h-0 flex-1 flex-col overflow-hidden rounded-xl">
                <div className="flex h-7 items-center justify-between border-b border-black/5 px-3">
                  <div className="text-[13px] font-medium text-black">Backup & Restore</div>
                  <button className="px-1 py-0.5 text-[12px] leading-tight text-gray-500 hover:text-black" onClick={() => setActivePanel(null)} type="button">Close</button>
                </div>
                <div className="flex min-h-0 flex-1 flex-col rounded-b-xl bg-white">
                  <div className="flex items-center gap-3 border-b border-black/5 px-3 py-2">
                    <span className="text-[12px] font-medium text-black">Backup:</span>
                    <button className="rounded-lg border border-[var(--accent)] bg-[var(--accent)] px-3 py-1 text-[12px] text-white hover:opacity-90 disabled:opacity-50" disabled={!!runningBackup} onClick={() => void handleBackup()} type="button">Backup Now</button>
                    <button className="rounded-lg border border-[var(--accent)] px-3 py-1 text-[12px] text-[var(--accent)] hover:bg-[var(--accent)]/10" onClick={() => { setEditingSchedule(null); setShowScheduleModal(true); }} type="button">Schedule Backup</button>
                    {runningBackup ? (
                      <div className="ml-2 flex items-center gap-2">
                        <div className="h-1.5 w-32 overflow-hidden rounded-full bg-black/5">
                          <div className="backup-progress h-full rounded-full bg-[var(--accent)]" />
                        </div>
                        <span className="text-[11px] text-[var(--accent)]">Backing up {runningBackup.name}...</span>
                      </div>
                    ) : null}
                    <div className="ml-auto flex items-center gap-1.5">
                      <span className="text-[11px] text-gray-400 truncate max-w-[250px]" title={backupDir}>{backupDir}</span>
                      <button className="rounded border border-black/10 px-2 py-0.5 text-[11px] text-gray-500 hover:border-[var(--accent)] hover:text-[var(--accent)]" onClick={() => void changeBackupDir()} type="button">Change</button>
                    </div>
                  </div>
                  <div className="flex-1 overflow-y-auto">
                    {backupSchedules.length > 0 ? (
                      <div className="border-b border-black/5">
                        <div className="flex items-center gap-2 bg-gray-50 px-3 py-1.5">
                          <span className="text-[11px] font-medium uppercase tracking-wider text-black/40">Backup Schedules</span>
                          <span className="text-[11px] text-gray-400">{backupSchedules.length}</span>
                        </div>
                        <table className="min-w-full border-collapse text-[12px]">
                          <thead className="bg-gray-50/50 text-left">
                            <tr>
                              <th className="border-b border-black/8 px-3 py-1 font-medium text-black">Runs On</th>
                              <th className="border-b border-black/8 px-3 py-1 font-medium text-black">Start Time</th>
                              <th className="border-b border-black/8 px-3 py-1 font-medium text-black">Backup Details</th>
                              <th className="border-b border-black/8 px-3 py-1 font-medium text-black">Last Run</th>
                              <th className="border-b border-black/8 px-3 py-1 font-medium text-black">Actions</th>
                            </tr>
                          </thead>
                          <tbody>
                            {backupSchedules.map((schedule) => {
                              const dayAbbr: Record<string, string> = { sunday: 'Sun', monday: 'Mon', tuesday: 'Tue', wednesday: 'Wed', thursday: 'Thu', friday: 'Fri', saturday: 'Sat' };
                              const days = (schedule.days ?? []).map((d) => dayAbbr[d] ?? d).join(', ');
                              const scope = schedule.scope === 'full' ? 'Full DB' : `${(schedule.tables?.length ?? 0) || (schedule.schemas?.length ?? 0)} objects`;
                              const content = schedule.dataOnly ? 'Data' : schedule.schemaOnly ? 'Schema' : 'Schema+Data';
                              return (
                                <tr key={schedule.id} className="border-b border-black/5 hover:bg-gray-50">
                                  <td className="px-3 py-1.5 text-black">{days || 'None'}</td>
                                  <td className="px-3 py-1.5 text-black">{schedule.time}</td>
                                  <td className="px-3 py-1.5 text-gray-500">{scope} / {content} / {schedule.format}</td>
                                  <td className="px-3 py-1.5 text-gray-400">{schedule.lastRun ? new Date(schedule.lastRun).toLocaleString() : 'Never'}</td>
                                  <td className="px-3 py-1.5">
                                    <div className="flex items-center gap-2">
                                      <button className="rounded border border-[var(--accent)] px-2 py-0.5 text-[11px] text-[var(--accent)] hover:bg-[var(--accent)]/10" onClick={() => { setEditingSchedule(schedule); setShowScheduleModal(true); }} type="button">Modify</button>
                                      <button className="rounded border border-red-400 px-2 py-0.5 text-[11px] text-red-500 hover:bg-red-50" onClick={() => void handleDeleteSchedule(schedule.id)} type="button">Delete</button>
                                    </div>
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    ) : null}
                    <div className="flex items-center gap-2 bg-gray-50 px-3 py-1.5">
                      <span className="text-[11px] font-medium uppercase tracking-wider text-black/40">Backup History</span>
                      <span className="text-[11px] text-gray-400">{backupEntries.length}</span>
                    </div>
                    {backupEntries.length > 0 ? (
                      <BackupTable entries={backupEntries} onDelete={handleDeleteBackup} onRestore={handleRestore} />
                    ) : (
                      <div className="flex items-center justify-center py-6 text-[12px] text-gray-400">No backups yet. Click "Backup Now" to create one.</div>
                    )}
                  </div>
                </div>
              </div>
            ) : null}

            {!showBackupPanel && !showMonitoringPanel ? (
            <div className="glass-panel flex min-h-0 flex-1 flex-col overflow-hidden rounded-xl">
              <div className="flex h-7 items-center justify-between border-b border-black/5 px-3">
                <div className="text-[13px] font-medium text-black">
                  {activeEditorTab?.kind === 'table' && activeEditorTab.source ? `Data - ${activeEditorTab.source.schema}.${activeEditorTab.source.table}` : 'Data'}
                </div>
              </div>
              <div className="flex items-center border-b border-black/5 bg-white px-1 pt-1">
                <div className="flex min-w-0 flex-1 overflow-auto">
                  {editorTabs.map((tab) => (
                    <button
                      className={classNames(
                        'group flex min-w-[160px] max-w-[360px] items-center gap-2 border-r border-black/5 px-3 py-1.5 text-left rounded-t-lg',
                        tab.id === activeEditorTab?.id ? 'bg-white text-black' : 'bg-gray-50 text-gray-400',
                      )}
                      key={tab.id}
                      onClick={() => setActiveEditorTabId(tab.id)}
                      type="button"
                    >
                      <span className="text-[11px] text-gray-500">
                        {tab.kind === 'table' ? <TableIcon /> : tab.kind === 'ddl' ? <DdlIcon /> : tab.kind === 'editdata' ? <EditDataIcon /> : <QueryIcon />}
                      </span>
                      <span className="truncate font-sans text-[12px]">
                        {tab.title}
                      </span>
                      <span
                        className="ml-auto shrink-0 text-gray-500 hover:text-black"
                        onClick={(event) => {
                          event.stopPropagation();
                          closeEditorTab(tab.id);
                        }}
                      >
                        x
                      </span>
                    </button>
                  ))}
                </div>
                {activeEditorTab?.result ? (
                  <div className="flex shrink-0 items-center gap-1 px-2">
                    <button className="rounded border border-black/10 px-2 py-0.5 text-[10px] text-gray-500 hover:bg-gray-50 hover:text-black" onClick={() => void exportResultAsCsv()} type="button">CSV</button>
                    <button className="rounded border border-black/10 px-2 py-0.5 text-[10px] text-gray-500 hover:bg-gray-50 hover:text-black" onClick={() => void exportResultAsExcel()} type="button">Excel</button>
                  </div>
                ) : null}
              </div>

              <div className="flex min-h-0 flex-1 flex-col rounded-b-xl bg-white">
                {activeEditorTab?.kind === 'ddl' && activeEditorTab.ddlText ? (
                  <div className="min-h-0 flex-1 overflow-auto p-4">
                    <pre className="font-mono text-[13px] leading-relaxed text-black whitespace-pre">{activeEditorTab.ddlText}</pre>
                  </div>
                ) : activeEditorTab?.kind === 'editdata' && activeEditorTab.editData ? (
                  <EditDataView
                    editData={activeEditorTab.editData}
                    onCellChange={editDataSetCell}
                    onToggleDeleteRow={editDataToggleDeleteRow}
                    onAddRow={editDataAddRow}
                    onRemoveNewRow={editDataRemoveNewRow}
                    onApply={() => void editDataApply()}
                    onDiscard={editDataDiscard}
                    onSetEditingCell={(cell) => updateEditData({ editingCell: cell })}
                    onPageChange={(p) => void editDataChangePage(p)}
                  />
                ) : processedResult ? (
                  <>
                    <div className="min-h-0 flex-1 overflow-scroll">
                      <ResultsTable
                        result={processedResult}
                        sortState={activeEditorTab?.sortState ?? null}
                        onSort={toggleSort}
                        rowOffset={processedResult.pageStart}
                      />
                    </div>
                    <div className="flex shrink-0 items-center justify-center gap-2 border-t border-black/5 bg-white/30 px-3 py-1.5 text-[14px] text-gray-600">
                      <button className="px-1 py-0.5 text-[16px] font-extrabold hover:text-black disabled:opacity-30" disabled={processedResult.currentPage === 0} onClick={() => updateActiveTab({ currentPage: 0 })} type="button">{'<<'}</button>
                      <button className="px-1 py-0.5 text-[16px] font-extrabold hover:text-black disabled:opacity-30" disabled={processedResult.currentPage === 0} onClick={() => updateActiveTab({ currentPage: processedResult.currentPage - 1 })} type="button">{'<'}</button>
                      <span>{processedResult.rowCount > 0 ? `${(processedResult.pageStart + 1).toLocaleString()}-${Math.min(processedResult.pageStart + PAGE_SIZE, processedResult.rowCount).toLocaleString()}` : '0'} of {processedResult.rowCount.toLocaleString()}</span>
                      <button className="px-1 py-0.5 text-[16px] font-extrabold hover:text-black disabled:opacity-30" disabled={processedResult.currentPage >= processedResult.totalPages - 1} onClick={() => updateActiveTab({ currentPage: processedResult.currentPage + 1 })} type="button">{'>'}</button>
                      <button className="px-1 py-0.5 text-[16px] font-extrabold hover:text-black disabled:opacity-30" disabled={processedResult.currentPage >= processedResult.totalPages - 1} onClick={() => updateActiveTab({ currentPage: processedResult.totalPages - 1 })} type="button">{'>>'}</button>
                    </div>
                  </>
                ) : (
                  <div className="min-h-0 flex-1 overflow-auto p-2">
                    <WorkspaceEmpty title="No results" body="Run a query or click a table from the explorer." />
                  </div>
                )}
              </div>
            </div>
            ) : null}
          </div>
        </div>

        <footer className="flex h-6 items-center justify-between gap-3 border-t border-white/20 bg-white/40 px-3 text-[11px] text-gray-500 backdrop-blur-md">
          <div className="truncate">{loading || error || (snapshot?.activeConnection ? `Connected as ${snapshot.activeConnection.user}` : 'Waiting for a database connection.')}</div>
          <div>{error ? 'error' : snapshot?.activeConnection ? 'online' : 'offline'}</div>
        </footer>
      </div>

      {contextMenu ? (
        <div
          className="fixed inset-0 z-30"
          onClick={() => setContextMenu(null)}
          onContextMenu={(event) => { event.preventDefault(); setContextMenu(null); }}
        >
          <div
            className="context-menu absolute min-w-[180px] rounded-xl py-1"
            style={{ left: contextMenu.x, top: contextMenu.y }}
            onClick={(event) => event.stopPropagation()}
          >
            <button
              className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40"
              onClick={() => {
                setContextMenu(null);
                void refresh();
              }}
              type="button"
            >
              <span className="text-gray-500"><RefreshIcon /></span>
              Refresh
            </button>
            <button
              className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40"
              onClick={() => {
                const { schema, table } = contextMenu;
                setContextMenu(null);
                void handleOpenModifyTable(schema, table);
              }}
              type="button"
            >
              <span className="text-gray-500"><ModifyIcon /></span>
              Modify Table
            </button>
            <button
              className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40"
              onClick={() => {
                const { schema, table } = contextMenu;
                setContextMenu(null);
                void handleOpenEditData(schema, table);
              }}
              type="button"
            >
              <span className="text-gray-500"><EditDataIcon /></span>
              Edit Data
            </button>
            <div className="mx-2 my-1 border-t border-black/5" />
            <button
              className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40"
              onClick={() => {
                const { schema, table } = contextMenu;
                setContextMenu(null);
                void handleExportFullTableCsv(schema, table);
              }}
              type="button"
            >
              <span className="text-gray-500"><ExportIcon /></span>
              Export CSV
            </button>
            <button
              className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40"
              onClick={() => {
                const { schema, table } = contextMenu;
                setContextMenu(null);
                void handleExportParquet(schema, table);
              }}
              type="button"
            >
              <span className="text-gray-500"><ExportIcon /></span>
              Export Parquet
            </button>
            <button
              className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40"
              onClick={() => {
                const { schema, table } = contextMenu;
                setContextMenu(null);
                void handleExportPgDump(schema, table);
              }}
              type="button"
            >
              <span className="text-gray-500"><ExportIcon /></span>
              Export with pg_dump
            </button>
            <div className="mx-2 my-1 border-t border-black/5" />
            <button
              className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-red-500 hover:bg-red-50"
              onClick={() => {
                const { schema, table } = contextMenu;
                setContextMenu(null);
                setDestructiveCascade(false);
                setDestructiveTableDialog({ action: 'truncate', schema, table });
              }}
              type="button"
            >
              <span className="text-red-400"><TruncateIcon /></span>
              Truncate Table
            </button>
            <button
              className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-red-600 hover:bg-red-50"
              onClick={() => {
                const { schema, table } = contextMenu;
                setContextMenu(null);
                setDestructiveCascade(false);
                setDestructiveTableDialog({ action: 'drop', schema, table });
              }}
              type="button"
            >
              <span className="text-red-500"><DropIcon /></span>
              Drop Table
            </button>
          </div>
        </div>
      ) : null}

      {connectionContextMenu ? (
        <div
          className="fixed inset-0 z-30"
          onClick={() => setConnectionContextMenu(null)}
          onContextMenu={(event) => { event.preventDefault(); setConnectionContextMenu(null); }}
        >
          <div
            className="context-menu absolute min-w-[160px] rounded-xl py-1"
            style={{ left: connectionContextMenu.x, top: connectionContextMenu.y }}
            onClick={(event) => event.stopPropagation()}
          >
            <button
              className="flex w-full items-center px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40"
              onClick={() => {
                const conn = connectionContextMenu.connection;
                setConnectionContextMenu(null);
                openEditConnectionModal(conn);
              }}
              type="button"
            >
              Edit
            </button>
            <button
              className="flex w-full items-center px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40"
              onClick={() => {
                const conn = connectionContextMenu.connection;
                setConnectionContextMenu(null);
                setConfirmDialog({
                  message: `Disconnect from ${conn.database}@${conn.host}?`,
                  onConfirm: () => { setConfirmDialog(null); void handleDisconnect(); },
                });
              }}
              type="button"
            >
              Disconnect
            </button>
          </div>
        </div>
      ) : null}

      {databaseContextMenu ? (
        <div className="fixed inset-0 z-30" onClick={() => setDatabaseContextMenu(null)} onContextMenu={(e) => { e.preventDefault(); setDatabaseContextMenu(null); }}>
          <div className="context-menu absolute min-w-[160px] rounded-xl py-1" style={{ left: databaseContextMenu.x, top: databaseContextMenu.y }} onClick={(e) => e.stopPropagation()}>
            <button className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40" onClick={() => { setDatabaseContextMenu(null); setShowCreateSchemaModal(true); }} type="button">
              <span className="text-gray-500"><PlusIcon /></span>
              Create Schema
            </button>
            <button className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40" onClick={() => { setDatabaseContextMenu(null); void refresh(); }} type="button">
              <span className="text-gray-500"><RefreshIcon /></span>
              Refresh
            </button>
          </div>
        </div>
      ) : null}

      {schemaContextMenu ? (
        <div className="fixed inset-0 z-30" onClick={() => setSchemaContextMenu(null)} onContextMenu={(e) => { e.preventDefault(); setSchemaContextMenu(null); }}>
          <div className="context-menu absolute min-w-[160px] rounded-xl py-1" style={{ left: schemaContextMenu.x, top: schemaContextMenu.y }} onClick={(e) => e.stopPropagation()}>
            <button className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40" onClick={() => { setCreateTableSchema(schemaContextMenu.schema); setSchemaContextMenu(null); setShowCreateTableModal(true); }} type="button">
              <span className="text-gray-500"><PlusIcon /></span>
              Create Table
            </button>
            <button className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-[12px] text-black hover:bg-white/40" onClick={() => { setSchemaContextMenu(null); void refresh(); }} type="button">
              <span className="text-gray-500"><RefreshIcon /></span>
              Refresh
            </button>
          </div>
        </div>
      ) : null}

      {showCreateSchemaModal ? (
        <CreateSchemaModal onClose={() => setShowCreateSchemaModal(false)} onCreate={handleCreateSchema} />
      ) : null}

      {showCreateTableModal ? (
        <CreateTableModal schema={createTableSchema} tree={databaseTree} onClose={() => setShowCreateTableModal(false)} onCreate={handleCreateTable} />
      ) : null}

      {destructiveTableDialog ? (() => {
        const { action, schema, table } = destructiveTableDialog;
        const isDrop = action === 'drop';
        const title = isDrop ? 'Drop Table' : 'Truncate Table';
        const sqlPreview = isDrop
          ? `DROP TABLE ${schema}.${table}${destructiveCascade ? ' CASCADE' : ''};`
          : `TRUNCATE TABLE ${schema}.${table}${destructiveCascade ? ' CASCADE' : ''};`;
        return (
          <div className="fixed inset-0 z-40 grid place-items-center bg-black/20 backdrop-blur-sm p-4">
            <div className="glass-panel-strong w-full max-w-sm rounded-2xl shadow-xl">
              <div className="flex items-center gap-3 border-b border-black/5 px-5 py-3">
                <span className="grid h-8 w-8 shrink-0 place-items-center rounded-full bg-red-100 text-red-600">
                  <WarningIcon />
                </span>
                <div className="text-[13px] font-medium text-black">{title}</div>
              </div>
              <div className="px-5 py-4">
                <div className="text-[13px] text-black">
                  {isDrop
                    ? <>This will permanently delete <span className="font-semibold">{schema}.{table}</span> and all its data. This action cannot be undone.</>
                    : <>This will permanently delete all data in <span className="font-semibold">{schema}.{table}</span>. The table structure will be preserved. This action cannot be undone.</>
                  }
                </div>
                <label className={classNames(
                  'mt-4 flex items-center gap-2 rounded-lg border px-3 py-2 text-[12px]',
                  destructiveTableHasDependents
                    ? 'border-black/10 cursor-pointer hover:bg-gray-50'
                    : 'border-black/5 opacity-40 cursor-not-allowed',
                )}>
                  <input
                    type="checkbox"
                    checked={destructiveCascade}
                    disabled={!destructiveTableHasDependents}
                    onChange={(e) => setDestructiveCascade(e.target.checked)}
                  />
                  <div>
                    <div className="font-medium text-black">CASCADE</div>
                    <div className="text-[11px] text-gray-500">
                      {isDrop
                        ? 'Also drop all dependent objects (views, foreign keys, etc.)'
                        : 'Also truncate all tables that reference this table via foreign keys'}
                    </div>
                  </div>
                </label>
                {!destructiveTableHasDependents && (
                  <div className="mt-1.5 text-[11px] text-gray-400">No other tables reference this table.</div>
                )}
                <pre className="mt-3 rounded-lg border border-black/10 bg-gray-50/80 px-3 py-2 font-mono text-[11px] text-gray-700">{sqlPreview}</pre>
              </div>
              <div className="flex justify-end gap-2 border-t border-black/5 px-5 py-3">
                <button
                  className="rounded-lg border border-black/10 px-3 py-1.5 text-[12px] text-gray-500 hover:text-black"
                  onClick={() => setDestructiveTableDialog(null)}
                  type="button"
                >
                  Cancel
                </button>
                <button
                  className="rounded-lg bg-red-600 px-3 py-1.5 text-[12px] text-white hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-red-500/30"
                  onClick={() => {
                    setDestructiveTableDialog(null);
                    if (isDrop) {
                      void handleDropTable(schema, table, destructiveCascade);
                    } else {
                      void handleTruncateTable(schema, table, destructiveCascade);
                    }
                  }}
                  type="button"
                >
                  {isDrop ? 'Drop Table' : 'Truncate Table'}
                </button>
              </div>
            </div>
          </div>
        );
      })() : null}

      {confirmDialog ? (
        <div className="fixed inset-0 z-40 grid place-items-center bg-black/20 backdrop-blur-sm p-4">
          <div className="glass-panel-strong w-full max-w-xs rounded-2xl shadow-xl">
            <div className="px-5 py-4 text-[13px] text-black whitespace-pre-line">{confirmDialog.message}</div>
            <div className="flex justify-end gap-2 border-t border-black/5 px-5 py-3">
              <button className="rounded-lg px-3 py-1 text-[12px] text-gray-500 hover:text-black" onClick={confirmDialog.onConfirm} type="button">Yes</button>
              <button autoFocus className="rounded-lg bg-[var(--accent)] px-3 py-1 text-[12px] text-white hover:opacity-90 focus:outline-none focus:ring-2 focus:ring-[var(--accent)]/30" onClick={() => setConfirmDialog(null)} type="button">No</button>
            </div>
          </div>
        </div>
      ) : null}

      {modifyTableInfo ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 backdrop-blur-sm" onClick={() => setModifyTableInfo(null)}>
          <div className="glass-panel-strong flex h-[85vh] w-[700px] max-w-[90vw] flex-col overflow-hidden rounded-2xl shadow-2xl" onClick={(e) => e.stopPropagation()}>
            <div className="flex h-10 shrink-0 items-center justify-between border-b border-black/5 px-4">
              <span className="text-[14px] font-medium text-black">Modify Table — {modifyTableInfo.schema}.{modifyTableInfo.table}</span>
              <button className="text-[18px] text-gray-400 hover:text-black" onClick={() => setModifyTableInfo(null)} type="button">&times;</button>
            </div>
            <div className="flex-1 overflow-y-auto px-5 py-4">
              <div className="mb-4">
                <label className="mb-1 block text-[12px] font-medium text-black">Table Name</label>
                <input className="input text-[12px]" value={modifyTableDraft.newTableName} onChange={(e) => setModifyTableDraft((d) => ({ ...d, newTableName: e.target.value }))} />
              </div>

              <div className="mb-4 overflow-hidden rounded-lg border border-black/10">
                <table className="min-w-full border-collapse text-[12px]" style={{ tableLayout: 'fixed' }}>
                  <colgroup><col /><col /><col style={{ width: 55 }} /><col style={{ width: 55 }} /><col /><col style={{ width: 24 }} /></colgroup>
                  <thead>
                    <tr><td colSpan={4} className="px-2 py-1.5 text-[12px] font-semibold text-black">Columns</td><td colSpan={2} className="px-1 py-1 text-right"><button className="w-[104px] rounded border border-[var(--accent)] py-0.5 text-center text-[11px] text-[var(--accent)] hover:bg-[var(--accent)]/10" onClick={() => setModifyTableDraft((d) => ({ ...d, addColumns: [...d.addColumns, { name: '', dataType: 'text', nullable: true, defaultValue: '' }] }))} type="button">+ Add Column</button></td></tr>
                    <tr>
                      <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Name</th>
                      <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Type</th>
                      <th className="border-b border-black/8 px-2 py-1 text-center text-[11px] font-medium text-gray-500">Primary</th>
                      <th className="border-b border-black/8 px-2 py-1 text-center text-[11px] font-medium text-gray-500">Null</th>
                      <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Default</th>
                      <th className="border-b border-black/8" />
                    </tr>
                  </thead>
                  <tbody>
                    {modifyTableDraft.columns.map((col, i) => {
                      const isDropped = modifyTableDraft.dropColumns.has(modifyTableInfo.columns[i]?.name);
                      const schemaNode = databaseTree.find((s) => s.name === modifyTableInfo.schema);
                      const tableNode = schemaNode?.tables.find((t) => t.name === modifyTableInfo.table);
                      const pkKey = (tableNode?.keys ?? []).find((k) => k.type === 'PRIMARY KEY');
                      const pkCols = Array.isArray(pkKey?.columns) ? pkKey.columns : [];
                      const isPk = pkCols.includes(modifyTableInfo.columns[i]?.name);
                      return (
                        <tr key={modifyTableInfo.columns[i]?.name ?? i} className={isDropped ? 'opacity-30 line-through' : 'border-b border-black/5'}>
                          <td className="px-1 py-1"><input className="input py-1 text-[12px]" value={col.name} disabled={isDropped} onChange={(e) => { const val = e.target.value; setModifyTableDraft((d) => { const cols = [...d.columns]; cols[i] = { ...cols[i], name: val }; return { ...d, columns: cols }; }); }} /></td>
                          <td className="px-1 py-1"><select className="input py-1 text-[12px]" value={col.dataType} disabled={isDropped} onChange={(e) => { const val = e.target.value; setModifyTableDraft((d) => { const cols = [...d.columns]; cols[i] = { ...cols[i], dataType: val }; return { ...d, columns: cols }; }); }}>{!COMMON_TYPES.includes(col.dataType) ? <option value={col.dataType}>{col.dataType}</option> : null}{COMMON_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}</select></td>
                          <td className="px-1 py-1 text-center"><input type="checkbox" checked={isPk} disabled title="Primary keys cannot be changed via Modify Table" /></td>
                          <td className="px-1 py-1 text-center"><input type="checkbox" checked={col.nullable} disabled={isDropped} onChange={(e) => { const val = e.target.checked; setModifyTableDraft((d) => { const cols = [...d.columns]; cols[i] = { ...cols[i], nullable: val }; return { ...d, columns: cols }; }); }} /></td>
                          <td className="px-1 py-1"><input className="input py-1 text-[12px]" value={col.defaultValue ?? ''} disabled={isDropped} placeholder="none" onChange={(e) => { const val = e.target.value; setModifyTableDraft((d) => { const cols = [...d.columns]; cols[i] = { ...cols[i], defaultValue: val || null }; return { ...d, columns: cols }; }); }} /></td>
                          <td className="px-1 py-1 text-center"><button className="text-red-400 hover:text-red-600" title={isDropped ? 'Undo' : 'Drop'} onClick={() => { const origName = modifyTableInfo.columns[i].name; setModifyTableDraft((d) => { const next = new Set(d.dropColumns); if (next.has(origName)) next.delete(origName); else next.add(origName); return { ...d, dropColumns: next }; }); }} type="button">{isDropped ? '\u21A9' : '\u00D7'}</button></td>
                        </tr>
                      );
                    })}
                    {modifyTableDraft.addColumns.map((col, i) => (
                      <tr key={`new-${i}`} className="border-b border-black/5 bg-emerald-50/30">
                        <td className="px-1 py-1"><input className="input py-1 text-[12px]" value={col.name} placeholder="column_name" onChange={(e) => { const val = e.target.value; setModifyTableDraft((d) => { const adds = [...d.addColumns]; adds[i] = { ...adds[i], name: val }; return { ...d, addColumns: adds }; }); }} /></td>
                        <td className="px-1 py-1"><select className="input py-1 text-[12px]" value={col.dataType} onChange={(e) => { const val = e.target.value; setModifyTableDraft((d) => { const adds = [...d.addColumns]; adds[i] = { ...adds[i], dataType: val }; return { ...d, addColumns: adds }; }); }}>{COMMON_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}</select></td>
                        <td className="px-1 py-1 text-center" />
                        <td className="px-1 py-1 text-center"><input type="checkbox" checked={col.nullable} onChange={(e) => { const val = e.target.checked; setModifyTableDraft((d) => { const adds = [...d.addColumns]; adds[i] = { ...adds[i], nullable: val }; return { ...d, addColumns: adds }; }); }} /></td>
                        <td className="px-1 py-1"><input className="input py-1 text-[12px]" value={col.defaultValue} placeholder="none" onChange={(e) => { const val = e.target.value; setModifyTableDraft((d) => { const adds = [...d.addColumns]; adds[i] = { ...adds[i], defaultValue: val }; return { ...d, addColumns: adds }; }); }} /></td>
                        <td className="px-1 py-1 text-center"><button className="text-red-400 hover:text-red-600" onClick={() => setModifyTableDraft((d) => ({ ...d, addColumns: d.addColumns.filter((_, j) => j !== i) }))} type="button">&times;</button></td>
                      </tr>
                    ))}
                  </tbody>
                  <tbody>
                    <tr><td colSpan={4} className="border-t-2 border-black/8 px-2 py-1.5 text-[12px] font-semibold text-black">Foreign Keys</td><td colSpan={2} className="border-t-2 border-black/8 px-1 py-1 text-right"><button className="w-[104px] rounded border border-[var(--accent)] py-0.5 text-center text-[11px] text-[var(--accent)] hover:bg-[var(--accent)]/10" onClick={() => setModifyTableDraft((d) => ({ ...d, addForeignKeys: [...d.addForeignKeys, { column: '', refTable: '', refColumn: '' }] }))} type="button">+ Add FK</button></td></tr>
                    <tr>
                      <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Source Column</th>
                      <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Reference Table</th>
                      <th colSpan={2} className="border-b border-black/8" />
                      <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Reference Column</th>
                      <th className="border-b border-black/8" />
                    </tr>
                    {modifyTableDraft.addForeignKeys.length === 0 ? (
                      <tr><td colSpan={6} className="px-2 py-1.5 text-[11px] text-gray-400">No new foreign keys</td></tr>
                    ) : modifyTableDraft.addForeignKeys.map((fk, i) => {
                      const ref = fk.refTable.trim();
                      const parts = ref.includes('.') ? ref.split('.') : [modifyTableInfo.schema, ref];
                      const refSchema = databaseTree.find((s) => s.name === parts[0]);
                      const refTable = refSchema?.tables.find((t) => t.name === parts[1]);
                      return (
                        <tr key={`mfk-${i}`} className="border-b border-black/5 bg-emerald-50/30">
                          <td className="px-1 py-1"><select className="input py-1 text-[12px]" value={fk.column} onChange={(e) => setModifyTableDraft((d) => { const fks = [...d.addForeignKeys]; fks[i] = { ...fks[i], column: e.target.value }; return { ...d, addForeignKeys: fks }; })}><option value="">Source Column</option>{modifyAllCols.map((c) => <option key={c} value={c}>{c}</option>)}</select></td>
                          <td className="px-1 py-1">
                            <select className="input py-1 text-[12px]" value={fk.refTable} onChange={(e) => setModifyTableDraft((d) => { const fks = [...d.addForeignKeys]; fks[i] = { ...fks[i], refTable: e.target.value }; return { ...d, addForeignKeys: fks }; })}>
                              <option value="">Reference Table</option>
                              {databaseTree.flatMap((s) => s.tables.map((t) => <option key={`${s.name}.${t.name}`} value={`${s.name}.${t.name}`}>{s.name}.{t.name}</option>))}
                            </select>
                          </td>
                          <td colSpan={2} />
                          <td className="px-1 py-1"><select className="input py-1 text-[12px]" value={fk.refColumn} onChange={(e) => setModifyTableDraft((d) => { const fks = [...d.addForeignKeys]; fks[i] = { ...fks[i], refColumn: e.target.value }; return { ...d, addForeignKeys: fks }; })}><option value="">Reference Column</option>{(refTable?.columns ?? []).map((c) => <option key={c.name} value={c.name}>{c.name}</option>)}</select></td>
                          <td className="px-1 py-1 text-center"><button className="text-red-400 hover:text-red-600" onClick={() => setModifyTableDraft((d) => ({ ...d, addForeignKeys: d.addForeignKeys.filter((_, j) => j !== i) }))} type="button">&times;</button></td>
                        </tr>
                      );
                    })}
                  </tbody>
                  <tbody>
                    <tr><td colSpan={4} className="border-t-2 border-black/8 px-2 py-1.5 text-[12px] font-semibold text-black">Indexes</td><td colSpan={2} className="border-t-2 border-black/8 px-1 py-1 text-right"><button className="w-[104px] rounded border border-[var(--accent)] py-0.5 text-center text-[11px] text-[var(--accent)] hover:bg-[var(--accent)]/10" onClick={() => setModifyTableDraft((d) => ({ ...d, addIndexes: [...d.addIndexes, { name: '', columns: '', unique: false }] }))} type="button">+ Add Index</button></td></tr>
                    <tr>
                      <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Index Name (optional)</th>
                      <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Table Columns</th>
                      <th className="border-b border-black/8 px-2 py-1 text-center text-[11px] font-medium text-gray-500">Unique</th>
                      <th colSpan={2} className="border-b border-black/8" />
                      <th className="border-b border-black/8" />
                    </tr>
                    {modifyTableDraft.addIndexes.length === 0 ? (
                      <tr><td colSpan={6} className="px-2 py-1.5 text-[11px] text-gray-400">No new indexes</td></tr>
                    ) : modifyTableDraft.addIndexes.map((idx, i) => {
                      return (
                        <tr key={`midx-${i}`} className="border-b border-black/5 bg-emerald-50/30">
                          <td className="px-1 py-1"><input className="input py-1 text-[12px]" value={idx.name} onChange={(e) => setModifyTableDraft((d) => { const idxs = [...d.addIndexes]; idxs[i] = { ...idxs[i], name: e.target.value }; return { ...d, addIndexes: idxs }; })} placeholder="auto-generated" /></td>
                          <td className="px-1 py-1"><IndexColumnPicker availableColumns={modifyAllCols} selected={idx.columns} onChange={(val) => setModifyTableDraft((d) => { const idxs = [...d.addIndexes]; idxs[i] = { ...idxs[i], columns: val }; return { ...d, addIndexes: idxs }; })} /></td>
                          <td className="px-1 py-1 text-center"><input type="checkbox" checked={idx.unique} onChange={(e) => setModifyTableDraft((d) => { const idxs = [...d.addIndexes]; idxs[i] = { ...idxs[i], unique: e.target.checked }; return { ...d, addIndexes: idxs }; })} /></td>
                          <td colSpan={2} />
                          <td className="px-1 py-1 text-center"><button className="text-red-400 hover:text-red-600" onClick={() => setModifyTableDraft((d) => ({ ...d, addIndexes: d.addIndexes.filter((_, j) => j !== i) }))} type="button">&times;</button></td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>


              <div className="overflow-hidden rounded-lg border border-black/10">
                <div className="px-3 py-1.5 text-[12px] font-semibold text-black">DDL Preview</div>
                <pre className="overflow-x-auto border-t border-black/10 bg-gray-50 p-3 font-mono text-[11px] leading-relaxed text-black whitespace-pre-wrap">{(() => {
                  const tableName = modifyTableDraft.newTableName || modifyTableInfo.table;
                  const qt = `${qi(modifyTableInfo.schema)}.${qi(tableName)}`;

                  const colDefs: string[] = [];
                  for (let i = 0; i < modifyTableDraft.columns.length; i++) {
                    const orig = modifyTableInfo.columns[i];
                    if (!orig || modifyTableDraft.dropColumns.has(orig.name)) continue;
                    const col = modifyTableDraft.columns[i];
                    let def = `  ${qi(col.name)} ${col.dataType}`;
                    if (!col.nullable) def += ' NOT NULL';
                    if (col.defaultValue) def += ` DEFAULT ${col.defaultValue}`;
                    colDefs.push(def);
                  }
                  for (const col of modifyTableDraft.addColumns) {
                    if (!col.name.trim()) continue;
                    let def = `  ${qi(col.name.trim())} ${col.dataType}`;
                    if (!col.nullable) def += ' NOT NULL';
                    if (col.defaultValue.trim()) def += ` DEFAULT ${col.defaultValue.trim()}`;
                    colDefs.push(def);
                  }
                  for (const fk of modifyTableDraft.addForeignKeys) {
                    if (fk.column && fk.refTable && fk.refColumn) {
                      colDefs.push(`  FOREIGN KEY (${qi(fk.column)}) REFERENCES ${fk.refTable} (${qi(fk.refColumn)})`);
                    }
                  }

                  let ddl = `CREATE TABLE ${qt} (\n${colDefs.join(',\n')}\n);`;

                  for (const idx of modifyTableDraft.addIndexes) {
                    if (idx.columns.trim()) {
                      const idxName = idx.name.trim() || `idx_${tableName}_${idx.columns.trim().replace(/,\s*/g, '_')}`;
                      const unique = idx.unique ? 'UNIQUE ' : '';
                      ddl += `\n\nCREATE ${unique}INDEX ${qi(idxName)}\n  ON ${qt} (${idx.columns.trim()});`;
                    }
                  }

                  if (modifyPreviewDdl && modifyPreviewDdl !== '-- No changes') {
                    ddl += '\n\n-- Pending ALTER statements:\n' + modifyPreviewDdl;
                  }

                  return ddl;
                })()}</pre>
              </div>

              {modifyTableError ? (
                <div className="mt-3 rounded-lg bg-red-50 px-3 py-2 text-[12px] text-red-600">{modifyTableError}</div>
              ) : null}
            </div>
            <div className="flex items-center justify-end gap-2 border-t border-black/5 px-5 py-3">
              <button className="rounded-lg border border-black/10 px-4 py-1.5 text-[12px] text-gray-500 hover:text-black" onClick={() => setModifyTableInfo(null)} type="button">Cancel</button>
              <button className="rounded-lg bg-[var(--accent)] px-4 py-1.5 text-[12px] text-white hover:opacity-90" onClick={() => void handleApplyModifyTable()} type="button">Apply Changes</button>
            </div>
          </div>
        </div>
      ) : null}

      {showConnectionModal ? (
        <div className="fixed inset-0 z-20 grid place-items-center bg-black/20 backdrop-blur-sm p-4">
          <div className="glass-panel-strong w-full max-w-sm rounded-2xl shadow-xl flex flex-col">
            <div className="border-b border-black/5 px-5 py-3">
              <div className="text-[13px] font-medium text-black">{draft.id ? 'Edit connection' : 'New connection'}</div>
            </div>
            <div className="flex border-b border-black/5 px-5">
              <button
                className={classNames('px-3 py-2 text-[12px] border-b-2 -mb-px', connectionTab === 'general' ? 'border-[var(--accent)] text-black font-medium' : 'border-transparent text-gray-400 hover:text-gray-600')}
                onClick={() => setConnectionTab('general')}
                type="button"
              >
                General
              </button>
              <button
                className={classNames('px-3 py-2 text-[12px] border-b-2 -mb-px', connectionTab === 'ssh' ? 'border-[var(--accent)] text-black font-medium' : 'border-transparent text-gray-400 hover:text-gray-600')}
                onClick={() => setConnectionTab('ssh')}
                type="button"
              >
                SSH / SSL
              </button>
            </div>

            {connectionTab === 'general' ? (
              <div className="flex flex-col gap-4 p-5">
                <div className="grid grid-cols-[1fr_120px] gap-4">
                  <Field label="Host"><input className="input" value={draft.host} onChange={(event) => setDraft((current) => ({ ...current, host: event.target.value }))} /></Field>
                  <Field label="Port"><input className="input" inputMode="numeric" value={String(draft.port)} onChange={(event) => setDraft((current) => ({ ...current, port: Number.parseInt(event.target.value || '5432', 10) }))} /></Field>
                </div>
                <Field label="Authentication">
                  <select className="input" value={draft.authMethod ?? 'password'} onChange={(e) => setDraft((c) => ({ ...c, authMethod: e.target.value as 'password' | 'pgpass', password: e.target.value === 'pgpass' ? '' : c.password }))}>
                    <option value="password">User &amp; Password</option>
                    <option value="pgpass">pgpass (~/.pgpass)</option>
                  </select>
                </Field>
                <Field label="User"><input className="input" value={draft.user} onChange={(event) => setDraft((current) => ({ ...current, user: event.target.value }))} /></Field>
                {draft.authMethod !== 'pgpass' ? (
                  <div className="grid grid-cols-[1fr_160px] gap-4">
                    <Field label="Password">
                      <input className="input" type="password" value={draft.password} onChange={(event) => setDraft((current) => ({ ...current, password: event.target.value }))} />
                    </Field>
                    <Field label="Save">
                      <select className="input text-gray-400" disabled>
                        <option>Forever</option>
                      </select>
                    </Field>
                  </div>
                ) : (
                  <div className="rounded-lg bg-blue-50 px-3 py-2 text-[11px] text-blue-700">
                    Password will be looked up from <span className="font-mono">~/.pgpass</span> matching {draft.host}:{draft.port}:{draft.database}:{draft.user}
                  </div>
                )}
                <Field label="Database"><input className="input" value={draft.database} onChange={(event) => setDraft((current) => ({ ...current, database: event.target.value }))} /></Field>
                <Field label="URL">
                  <div className="input bg-gray-50 text-gray-400">{`postgresql://${draft.user}@${draft.host}:${draft.port}/${draft.database}`}</div>
                </Field>
              </div>
            ) : (
              <div className="flex flex-col gap-4 p-5">
                <div>
                  <label className="flex items-center gap-2 text-[12px]">
                    <input
                      type="checkbox"
                      checked={draft.ssh?.enabled ?? false}
                      onChange={(e) => setDraft((c) => ({ ...c, ssh: { ...(c.ssh ?? EMPTY_SSH), enabled: e.target.checked } }))}
                    />
                    <span className="text-[11px] font-medium uppercase tracking-[0.08em] text-black">Enable SSH Tunnel</span>
                  </label>
                </div>

                {draft.ssh?.enabled ? (
                  <>
                    <div className="grid grid-cols-[1fr_100px] gap-4">
                      <Field label="SSH Host"><input className="input" value={draft.ssh.host} onChange={(e) => setDraft((c) => ({ ...c, ssh: { ...c.ssh!, host: e.target.value } }))} placeholder="bastion.example.com" /></Field>
                      <Field label="SSH Port"><input className="input" inputMode="numeric" value={String(draft.ssh.port)} onChange={(e) => setDraft((c) => ({ ...c, ssh: { ...c.ssh!, port: Number.parseInt(e.target.value || '22', 10) } }))} /></Field>
                    </div>
                    <Field label="SSH User"><input className="input" value={draft.ssh.user} onChange={(e) => setDraft((c) => ({ ...c, ssh: { ...c.ssh!, user: e.target.value } }))} /></Field>
                    <Field label="Auth Method">
                      <select className="input" value={draft.ssh.authMethod} onChange={(e) => setDraft((c) => ({ ...c, ssh: { ...c.ssh!, authMethod: e.target.value as 'password' | 'privateKey' } }))}>
                        <option value="password">Password</option>
                        <option value="privateKey">Private Key</option>
                      </select>
                    </Field>
                    {draft.ssh.authMethod === 'password' ? (
                      <Field label="SSH Password"><input className="input" type="password" value={draft.ssh.password} onChange={(e) => setDraft((c) => ({ ...c, ssh: { ...c.ssh!, password: e.target.value } }))} /></Field>
                    ) : (
                      <>
                        <Field label="Private Key Path"><input className="input" value={draft.ssh.privateKey} onChange={(e) => setDraft((c) => ({ ...c, ssh: { ...c.ssh!, privateKey: e.target.value } }))} placeholder="~/.ssh/id_rsa" /></Field>
                        <Field label="Passphrase"><input className="input" type="password" value={draft.ssh.passphrase} onChange={(e) => setDraft((c) => ({ ...c, ssh: { ...c.ssh!, passphrase: e.target.value } }))} placeholder="optional" /></Field>
                      </>
                    )}
                  </>
                ) : (
                  <div className="rounded-lg bg-gray-50 px-3 py-2 text-[11px] text-gray-500">
                    SSH tunnel is disabled. Enable it to connect through a bastion/jump host.
                  </div>
                )}
              </div>
            )}

            {testStatus === 'success' ? (
              <div className="mx-5 rounded-lg bg-emerald-50 px-3 py-2 text-[12px] text-emerald-700">Connection successful</div>
            ) : testStatus === 'fail' ? (
              <div className="mx-5 rounded-lg bg-red-50 px-3 py-2 text-[12px] text-red-600">{testError || 'Connection failed'}</div>
            ) : null}
            <div className="flex items-center justify-between border-t border-black/5 px-5 py-3">
              <button className="rounded-lg border border-black/10 px-3 py-1.5 text-[12px] text-gray-500 hover:text-black" onClick={() => void handleTestConnection()} disabled={testStatus === 'testing'} type="button">{testStatus === 'testing' ? 'Testing...' : 'Test Connection'}</button>
              <div className="flex items-center gap-2">
                <button className="rounded-lg border border-black/10 px-3 py-1.5 text-[12px] text-gray-500 hover:text-black" onClick={() => setShowConnectionModal(false)} type="button">Cancel</button>
                <button className="rounded-lg bg-[var(--accent)] px-3 py-1.5 text-[12px] text-white hover:opacity-90" onClick={() => void handleConnect()} type="button">{draft.id ? 'Save' : 'Connect'}</button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {showScheduleModal ? (
        <ScheduleBackupModal
          tree={databaseTree}
          database={snapshot?.activeConnection?.database ?? ''}
          backupDir={backupDir}
          existing={editingSchedule}
          onClose={() => { setShowScheduleModal(false); setEditingSchedule(null); }}
          onSave={(schedule) => editingSchedule ? void handleUpdateSchedule(editingSchedule.id, schedule) : void handleAddSchedule(schedule)}
        />
      ) : null}

      {showBackupModal ? (
        <BackupModal
          tree={databaseTree}
          database={snapshot?.activeConnection?.database ?? ''}
          backupDir={backupDir}
          onClose={() => setShowBackupModal(false)}
          onExecute={executeBackup}
        />
      ) : null}

      {showHelpViewer ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 backdrop-blur-sm" onClick={() => setShowHelpViewer(false)}>
          <div className="glass-panel-strong flex h-[80vh] w-[700px] max-w-[90vw] flex-col overflow-hidden rounded-2xl shadow-2xl" onClick={(e) => e.stopPropagation()}>
            <div className="flex h-10 shrink-0 items-center justify-between border-b border-black/5 px-4">
              <span className="text-[14px] font-medium text-black">Help</span>
              <button className="text-[18px] text-gray-400 hover:text-black" onClick={() => setShowHelpViewer(false)} type="button">&times;</button>
            </div>
            <div className="flex-1 overflow-y-auto px-6 py-4">
              <MarkdownViewer content={helpContent} />
            </div>
          </div>
        </div>
      ) : null}

      {showAboutDialog ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 backdrop-blur-sm" onClick={() => setShowAboutDialog(false)}>
          <div className="glass-panel-strong w-[360px] rounded-2xl p-6 shadow-2xl" onClick={(e) => e.stopPropagation()}>
            <div className="mb-4 flex items-center gap-3">
              <div className="rounded-lg bg-[var(--accent)] px-3 py-1.5 text-[14px] font-bold uppercase tracking-wider text-white">
                PostGrip
              </div>
            </div>
            {appInfo ? (
              <div className="space-y-2 text-[13px]">
                <div className="flex justify-between"><span className="text-gray-500">Version</span><span className="font-medium text-black">{appInfo.version}</span></div>
                <div className="flex justify-between"><span className="text-gray-500">Electron</span><span className="text-black">{appInfo.electronVersion}</span></div>
                <div className="flex justify-between"><span className="text-gray-500">Node.js</span><span className="text-black">{appInfo.nodeVersion}</span></div>
                <div className="flex justify-between"><span className="text-gray-500">Chromium</span><span className="text-black">{appInfo.chromiumVersion}</span></div>
                <div className="flex justify-between"><span className="text-gray-500">Platform</span><span className="text-black">{appInfo.platform}</span></div>
                <div className="border-t border-black/5 pt-2 text-[12px] text-gray-400">
                  A lightweight, fast desktop PostgreSQL client.
                </div>
              </div>
            ) : null}
            <div className="mt-4 flex justify-end">
              <button className="rounded-lg bg-[var(--accent)] px-4 py-1.5 text-[12px] text-white hover:opacity-90" onClick={() => setShowAboutDialog(false)} type="button">Close</button>
            </div>
          </div>
        </div>
      ) : null}
    </main>
  );
}

function IndexColumnPicker({ availableColumns, selected, onChange }: { availableColumns: string[]; selected: string; onChange: (val: string) => void }) {
  const [open, setOpen] = useState(false);
  const selectedCols = selected ? selected.split(',').map((s) => s.trim()).filter(Boolean) : [];

  function toggle(col: string) {
    const next = selectedCols.includes(col)
      ? selectedCols.filter((c) => c !== col)
      : [...selectedCols, col];
    onChange(next.join(', '));
  }

  return (
    <div className="relative">
      <button
        className="input flex w-full items-center justify-between py-1 text-left text-[12px]"
        onClick={() => setOpen((c) => !c)}
        type="button"
      >
        <span className={selectedCols.length > 0 ? 'truncate text-black' : 'text-gray-400'}>
          {selectedCols.length > 0 ? selectedCols.join(', ') : 'Select columns'}
        </span>
        <span className="text-[9px] text-gray-400">{open ? '\u25B4' : '\u25BE'}</span>
      </button>
      {open ? (
        <div className="absolute left-0 top-full z-50 mt-1 max-h-[150px] w-full overflow-y-auto rounded-lg border border-black/10 bg-white py-1 shadow-lg">
          {availableColumns.length === 0 ? (
            <div className="px-3 py-1.5 text-[11px] text-gray-400">Add columns first</div>
          ) : availableColumns.map((col) => (
            <label key={col} className="flex cursor-pointer items-center gap-2 px-3 py-1 text-[12px] text-black hover:bg-gray-50">
              <input type="checkbox" checked={selectedCols.includes(col)} onChange={() => toggle(col)} />
              {col}
            </label>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function CreateSchemaModal({ onClose, onCreate }: { onClose: () => void; onCreate: (name: string) => void }) {
  const [name, setName] = useState('');
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 backdrop-blur-sm" onClick={onClose}>
      <div className="glass-panel-strong w-[400px] rounded-2xl p-5 shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="mb-4 text-[14px] font-medium text-black">Create Schema</div>
        <div className="mb-4">
          <label className="mb-1 block text-[12px] font-medium text-black">Schema Name</label>
          <input className="input text-[12px]" value={name} onChange={(e) => setName(e.target.value)} placeholder="new_schema" autoFocus onKeyDown={(e) => { if (e.key === 'Enter' && name.trim()) onCreate(name.trim()); }} />
        </div>
        <div className="flex justify-end gap-2">
          <button className="rounded-lg border border-black/10 px-4 py-1.5 text-[12px] text-gray-500 hover:text-black" onClick={onClose} type="button">Cancel</button>
          <button className="rounded-lg bg-[var(--accent)] px-4 py-1.5 text-[12px] text-white hover:opacity-90 disabled:opacity-50" disabled={!name.trim()} onClick={() => onCreate(name.trim())} type="button">Create</button>
        </div>
      </div>
    </div>
  );
}

function CreateTableModal({ schema, tree, onClose, onCreate }: { schema: string; tree: SchemaNode[]; onClose: () => void; onCreate: (schema: string, tableName: string, columns: Array<{ name: string; type: string; nullable: boolean; pk?: boolean; defaultValue?: string }>, foreignKeys?: Array<{ column: string; refTable: string; refColumn: string }>, indexes?: Array<{ name?: string; columns: string; unique?: boolean }>) => void }) {
  const [tableName, setTableName] = useState('');
  const [columns, setColumns] = useState<Array<{ name: string; type: string; nullable: boolean; pk: boolean; defaultValue: string }>>([]);
  const [foreignKeys, setForeignKeys] = useState<Array<{ column: string; refTable: string; refColumn: string }>>([]);
  const [indexes, setIndexes] = useState<Array<{ name: string; columns: string; unique: boolean }>>([]);

  function addColumn() {
    setColumns((prev) => [...prev, { name: '', type: 'text', nullable: true, pk: false, defaultValue: '' }]);
  }
  function updateColumn(index: number, field: string, value: string | boolean) {
    setColumns((prev) => prev.map((c, i) => i === index ? { ...c, [field]: value } : c));
  }
  function removeColumn(index: number) {
    setColumns((prev) => prev.filter((_, i) => i !== index));
  }
  function addForeignKey() {
    setForeignKeys((prev) => [...prev, { column: '', refTable: '', refColumn: '' }]);
  }
  function updateFk(index: number, field: string, value: string) {
    setForeignKeys((prev) => prev.map((fk, i) => i === index ? { ...fk, [field]: value } : fk));
  }
  function removeFk(index: number) {
    setForeignKeys((prev) => prev.filter((_, i) => i !== index));
  }
  function addIndex() {
    setIndexes((prev) => [...prev, { name: '', columns: '', unique: false }]);
  }
  function updateIndex(index: number, field: string, value: string | boolean) {
    setIndexes((prev) => prev.map((idx, i) => i === index ? { ...idx, [field]: value } : idx));
  }
  function removeIndex(index: number) {
    setIndexes((prev) => prev.filter((_, i) => i !== index));
  }

  function handleCreate() {
    const cols = columns.filter((c) => c.name.trim()).map((c) => ({
      name: c.name.trim(),
      type: c.type,
      nullable: c.nullable,
      pk: c.pk,
      defaultValue: c.defaultValue.trim() || undefined,
    }));
    const fks = foreignKeys.filter((fk) => fk.column.trim() && fk.refTable.trim() && fk.refColumn.trim());
    const idxs = indexes.filter((idx) => idx.columns.trim()).map((idx) => ({
      name: idx.name.trim() || undefined,
      columns: idx.columns.trim(),
      unique: idx.unique,
    }));
    if (tableName.trim() && cols.length > 0) onCreate(schema, tableName.trim(), cols, fks.length > 0 ? fks : undefined, idxs.length > 0 ? idxs : undefined);
  }

  // COMMON_TYPES is defined at module level

  function buildSqlPreview(): string {
    const cols = columns.filter((c) => c.name.trim());
    if (!tableName.trim() || cols.length === 0) return '-- Enter a table name and at least one column';
    const qt = `"${schema}"."${tableName.trim()}"`;
    const lines: string[] = [];

    // Column definitions
    for (const c of cols) {
      let def = `  "${c.name.trim()}" ${c.type}`;
      if (!c.nullable) def += ' NOT NULL';
      if (c.defaultValue.trim()) def += ` DEFAULT ${c.defaultValue.trim()}`;
      lines.push(def);
    }

    // Primary key
    const pkCols = columns.filter((c) => c.name.trim() && c.pk);
    if (pkCols.length > 0) {
      lines.push(`  PRIMARY KEY (${pkCols.map((c) => `"${c.name.trim()}"`).join(', ')})`);
    }

    // Foreign keys
    for (const fk of foreignKeys) {
      if (fk.column.trim() && fk.refTable.trim() && fk.refColumn.trim()) {
        lines.push(`  FOREIGN KEY ("${fk.column.trim()}") REFERENCES ${fk.refTable.trim()} ("${fk.refColumn.trim()}")`);
      }
    }

    let sql = `CREATE TABLE ${qt} (\n${lines.join(',\n')}\n);`;

    // Indexes
    for (const idx of indexes) {
      if (idx.columns.trim()) {
        const idxName = idx.name.trim() || `idx_${tableName.trim()}_${idx.columns.trim().replace(/,\s*/g, '_')}`;
        const unique = idx.unique ? 'UNIQUE ' : '';
        sql += `\n\nCREATE ${unique}INDEX "${idxName}"\n  ON ${qt} (${idx.columns.trim()});`;
      }
    }

    return sql;
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 backdrop-blur-sm" onClick={onClose}>
      <div className="glass-panel-strong flex h-[85vh] w-[700px] max-w-[90vw] flex-col overflow-hidden rounded-2xl shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex h-10 shrink-0 items-center justify-between border-b border-black/5 px-4">
          <span className="text-[14px] font-medium text-black">Create Table in {schema}</span>
          <button className="text-[18px] text-gray-400 hover:text-black" onClick={onClose} type="button">&times;</button>
        </div>
        <div className="flex-1 overflow-y-auto px-5 py-4">
          <div className="mb-4">
            <label className="mb-1 block text-[12px] font-medium text-black">Table Name</label>
            <input className="input text-[12px]" value={tableName} onChange={(e) => setTableName(e.target.value)} placeholder="new_table" autoFocus />
          </div>

          <div className="mb-4 overflow-hidden rounded-lg border border-black/10">
          <table className="min-w-full border-collapse text-[12px]" style={{ tableLayout: 'fixed' }}>
            <colgroup><col /><col /><col style={{ width: 55 }} /><col style={{ width: 55 }} /><col /><col style={{ width: 24 }} /></colgroup>
            <thead>
              <tr><td colSpan={4} className="px-2 py-1.5 text-[12px] font-semibold text-black">Columns</td><td colSpan={2} className="px-1 py-1 text-right"><button className="w-[104px] rounded border border-[var(--accent)] py-0.5 text-center text-[11px] font-normal text-[var(--accent)] hover:bg-[var(--accent)]/10" onClick={addColumn} type="button">+ Add Column</button></td></tr>
              <tr>
                <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Name</th>
                <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Type</th>
                <th className="border-b border-black/8 px-2 py-1 text-center text-[11px] font-medium text-gray-500">Primary</th>
                <th className="border-b border-black/8 px-2 py-1 text-center text-[11px] font-medium text-gray-500">Null</th>
                <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Default</th>
                <th className="border-b border-black/8 px-2 py-1" />
              </tr>
            </thead>
            <tbody>
              {columns.map((col, i) => (
                <tr key={`col-${i}`} className="border-b border-black/5">
                  <td className="px-1 py-1"><input className="input py-1 text-[12px]" value={col.name} onChange={(e) => updateColumn(i, 'name', e.target.value)} placeholder="column_name" /></td>
                  <td className="px-1 py-1">
                    <select className="input py-1 text-[12px]" value={col.type} onChange={(e) => updateColumn(i, 'type', e.target.value)}>
                      {COMMON_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
                    </select>
                  </td>
                  <td className="px-1 py-1 text-center"><input type="checkbox" checked={col.pk} onChange={(e) => updateColumn(i, 'pk', e.target.checked)} /></td>
                  <td className="px-1 py-1 text-center"><input type="checkbox" checked={col.nullable} onChange={(e) => updateColumn(i, 'nullable', e.target.checked)} /></td>
                  <td className="px-1 py-1"><input className="input py-1 text-[12px]" value={col.defaultValue} onChange={(e) => updateColumn(i, 'defaultValue', e.target.value)} placeholder="optional" /></td>
                  <td className="px-1 py-1 text-center"><button className="text-red-400 hover:text-red-600" onClick={() => removeColumn(i)} type="button">&times;</button></td>
                </tr>
              ))}
              <tr><td colSpan={4} className="border-t-2 border-black/8 px-2 py-1.5 text-[12px] font-semibold text-black">Foreign Keys</td><td colSpan={2} className="border-t-2 border-black/8 px-1 py-1 text-right"><button className="w-[104px] rounded border border-[var(--accent)] py-0.5 text-center text-[11px] text-[var(--accent)] hover:bg-[var(--accent)]/10" onClick={addForeignKey} type="button">+ Add FK</button></td></tr>
              <tr>
                <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Source Column</th>
                <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Reference Table</th>
                <th colSpan={2} className="border-b border-black/8" />
                <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Reference Column</th>
                <th className="border-b border-black/8" />
              </tr>
              {foreignKeys.length === 0 ? (
                <tr><td colSpan={6} className="px-2 py-1.5 text-[11px] text-gray-400">No foreign keys</td></tr>
              ) : foreignKeys.map((fk, i) => (
                <tr key={`fk-${i}`} className="border-b border-black/5">
                  <td className="px-1 py-1">
                    <select className="input py-1 text-[12px]" value={fk.column} onChange={(e) => updateFk(i, 'column', e.target.value)}>
                      <option value="">Source Column</option>
                      {columns.filter((c) => c.name.trim()).map((c) => <option key={c.name} value={c.name.trim()}>{c.name.trim()}</option>)}
                    </select>
                  </td>
                  <td className="px-1 py-1">
                    <select className="input py-1 text-[12px]" value={fk.refTable} onChange={(e) => updateFk(i, 'refTable', e.target.value)}>
                      <option value="">Reference Table</option>
                      {tree.flatMap((s) => s.tables.map((t) => <option key={`${s.name}.${t.name}`} value={`${s.name}.${t.name}`}>{s.name}.{t.name}</option>))}
                    </select>
                  </td>
                  <td colSpan={2} />
                  <td className="px-1 py-1">
                    {(() => {
                      const ref = fk.refTable.trim();
                      const parts = ref.includes('.') ? ref.split('.') : [schema, ref];
                      const s = tree.find((sc) => sc.name === parts[0]);
                      const t = s?.tables.find((tb) => tb.name === parts[1]);
                      const cols = t?.columns ?? [];
                      return (
                        <select className="input py-1 text-[12px]" value={fk.refColumn} onChange={(e) => updateFk(i, 'refColumn', e.target.value)}>
                          <option value="">Reference Column</option>
                          {cols.map((c) => <option key={c.name} value={c.name}>{c.name}</option>)}
                        </select>
                      );
                    })()}
                  </td>
                  <td className="px-1 py-1 text-center"><button className="text-red-400 hover:text-red-600" onClick={() => removeFk(i)} type="button">&times;</button></td>
                </tr>
              ))}
              <tr><td colSpan={4} className="border-t-2 border-black/8 px-2 py-1.5 text-[12px] font-semibold text-black">Indexes</td><td colSpan={2} className="border-t-2 border-black/8 px-1 py-1 text-right"><button className="w-[104px] rounded border border-[var(--accent)] py-0.5 text-center text-[11px] text-[var(--accent)] hover:bg-[var(--accent)]/10" onClick={addIndex} type="button">+ Add Index</button></td></tr>
              <tr>
                <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Index Name (optional)</th>
                <th className="border-b border-black/8 px-2 py-1 text-left text-[11px] font-medium text-gray-500">Table Columns</th>
                <th className="border-b border-black/8 px-2 py-1 text-center text-[11px] font-medium text-gray-500">Unique</th>
                <th colSpan={2} className="border-b border-black/8" />
                <th className="border-b border-black/8" />
              </tr>
              {indexes.length === 0 ? (
                <tr><td colSpan={6} className="px-2 py-1.5 text-[11px] text-gray-400">No indexes</td></tr>
              ) : indexes.map((idx, i) => (
                <tr key={`idx-${i}`} className="border-b border-black/5">
                  <td className="px-1 py-1"><input className="input py-1 text-[12px]" value={idx.name} onChange={(e) => updateIndex(i, 'name', e.target.value)} placeholder="Index name (optional)" /></td>
                  <td className="px-1 py-1">
                    <IndexColumnPicker
                      availableColumns={columns.filter((c) => c.name.trim()).map((c) => c.name.trim())}
                      selected={idx.columns}
                      onChange={(val) => updateIndex(i, 'columns', val)}
                    />
                  </td>
                  <td className="px-1 py-1 text-center"><input type="checkbox" checked={idx.unique} onChange={(e) => updateIndex(i, 'unique', e.target.checked)} title="Unique" /></td>
                  <td /><td />
                  <td className="px-1 py-1 text-center"><button className="text-red-400 hover:text-red-600" onClick={() => removeIndex(i)} type="button">&times;</button></td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>

          <div className="overflow-hidden rounded-lg border border-black/10">
            <div className="px-3 py-1.5 text-[12px] font-semibold text-black">SQL Preview</div>
            <pre className="overflow-x-auto border-t border-black/10 bg-gray-50 p-3 font-mono text-[11px] leading-relaxed text-black">{buildSqlPreview()}</pre>
          </div>
        </div>
        <div className="flex items-center justify-end gap-2 border-t border-black/5 px-5 py-3">
          <button className="rounded-lg border border-black/10 px-4 py-1.5 text-[12px] text-gray-500 hover:text-black" onClick={onClose} type="button">Cancel</button>
          <button className="rounded-lg bg-[var(--accent)] px-4 py-1.5 text-[12px] text-white hover:opacity-90 disabled:opacity-50" disabled={!tableName.trim() || columns.filter((c) => c.name.trim()).length === 0} onClick={handleCreate} type="button">Create Table</button>
        </div>
      </div>
    </div>
  );
}

function BackupTable({ entries, onDelete, onRestore }: {
  entries: BackupEntry[];
  onDelete: (path: string) => void;
  onRestore: (path: string) => void;
}) {
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(new Set());

  function toggleExpand(p: string) {
    setExpandedPaths((prev) => {
      const next = new Set(prev);
      if (next.has(p)) next.delete(p); else next.add(p);
      return next;
    });
  }

  const FORMAT_LABELS: Record<string, string> = { tar: 'Tar Archive', custom: 'Custom (pg_restore)', sql: 'Plain SQL', directory: 'Directory' };

  return (
    <table className="min-w-full border-collapse text-[12px]">
      <thead className="sticky top-0 bg-gray-50 text-left">
        <tr>
          <th className="w-5 border-b border-black/8 px-1 py-1.5" />
          <th className="border-b border-black/8 px-3 py-1.5 font-medium text-black">Created Date</th>
          <th className="border-b border-black/8 px-3 py-1.5 font-medium text-black">Status</th>
          <th className="border-b border-black/8 px-3 py-1.5 font-medium text-black">Size</th>
          <th className="border-b border-black/8 px-3 py-1.5 font-medium text-black">Duration</th>
          <th className="border-b border-black/8 px-3 py-1.5 font-medium text-black">Path</th>
          <th className="border-b border-black/8 px-3 py-1.5 font-medium text-black">Actions</th>
        </tr>
      </thead>
      <tbody>
        {entries.map((entry) => {
          const expanded = expandedPaths.has(entry.path);
          const m = entry.meta;
          const dur = entry.durationMs ?? m?.durationMs;
          return (
            <Fragment key={entry.path + (entry.status ?? '')}>
              <tr className="border-b border-black/5 hover:bg-gray-50 cursor-pointer" onClick={() => toggleExpand(entry.path)}>
                <td className="px-1 py-1.5 text-center text-gray-400">{expanded ? '\u25BE' : '\u25B8'}</td>
                <td className="px-3 py-1.5 text-black">{new Date(entry.modified).toLocaleString()}</td>
                <td className="px-3 py-1.5">
                  {entry.status === 'in_progress' ? (
                    <span className="inline-flex items-center gap-1.5 rounded bg-blue-100 px-1.5 py-0.5 text-[11px] font-medium text-blue-700">
                      <span className="backup-spinner inline-block h-2.5 w-2.5 rounded-full border-2 border-blue-300 border-t-blue-700" />
                      In Progress
                    </span>
                  ) : entry.status === 'failed' ? (
                    <span className="rounded bg-red-100 px-1.5 py-0.5 text-[11px] font-medium text-red-700">Failed</span>
                  ) : (
                    <span className="rounded bg-green-100 px-1.5 py-0.5 text-[11px] font-medium text-green-700">Successful</span>
                  )}
                </td>
                <td className="px-3 py-1.5 text-gray-500">{entry.status === 'in_progress' ? '...' : entry.size < 1024 * 1024 ? `${Math.round(entry.size / 1024)} KB` : `${(entry.size / 1024 / 1024).toFixed(1)} MB`}</td>
                <td className="px-3 py-1.5 text-gray-400">{dur != null && dur > 0 ? `${(dur / 1000).toFixed(1)}s` : entry.status === 'in_progress' ? '...' : '--'}</td>
                <td className="max-w-[200px] truncate px-3 py-1.5 text-gray-400" title={entry.path}>{entry.path}</td>
                <td className="px-3 py-1.5" onClick={(e) => e.stopPropagation()}>
                  {entry.status === 'in_progress' ? (
                    <span className="text-[11px] text-gray-400">Running...</span>
                  ) : (
                    <div className="flex items-center gap-2">
                      <button className="rounded border border-red-400 px-2 py-0.5 text-[11px] text-red-500 hover:bg-red-50" onClick={() => void onDelete(entry.path)} type="button">Delete</button>
                      {entry.status !== 'failed' ? (
                        <button className="rounded border border-[var(--accent)] bg-[var(--accent)] px-2 py-0.5 text-[11px] text-white hover:opacity-90" onClick={() => void onRestore(entry.path)} type="button">Restore</button>
                      ) : null}
                    </div>
                  )}
                </td>
              </tr>
              {expanded ? (
                <tr className="border-b border-black/5 bg-gray-50/50">
                  <td colSpan={7} className="px-6 py-3">
                    {m ? (
                      <div className="grid grid-cols-2 gap-x-8 gap-y-1.5 text-[12px]">
                        <div><span className="text-gray-400">File:</span> <span className="text-black">{entry.name}</span></div>
                        <div><span className="text-gray-400">Format:</span> <span className="text-black">{FORMAT_LABELS[m.format] ?? m.format}</span></div>
                        <div><span className="text-gray-400">Database:</span> <span className="text-black">{m.database}</span></div>
                        <div><span className="text-gray-400">Host:</span> <span className="text-black">{m.host}:{m.port}</span></div>
                        <div><span className="text-gray-400">User:</span> <span className="text-black">{m.user}</span></div>
                        <div><span className="text-gray-400">Scope:</span> <span className="text-black">{m.scope === 'full' ? 'Entire Database' : 'Selected Objects'}</span></div>
                        <div><span className="text-gray-400">Content:</span> <span className="text-black">{m.dataOnly ? 'Data Only' : m.schemaOnly ? 'Schema Only' : 'Schema + Data'}</span></div>
                        {m.compress > 0 ? <div><span className="text-gray-400">Compression:</span> <span className="text-black">Level {m.compress}</span></div> : null}
                        <div className="col-span-2 mt-1">
                          <span className="text-gray-400">Options:</span>{' '}
                          <span className="text-black">
                            {[
                              m.noOwner && 'No Owner',
                              m.noPrivileges && 'No Privileges',
                              m.clean && 'Clean (DROP)',
                              m.createDb && 'CREATE DATABASE',
                              m.ifExists && 'IF EXISTS',
                            ].filter(Boolean).join(', ') || 'None'}
                          </span>
                        </div>
                        {m.schemas.length > 0 ? (
                          <div className="col-span-2"><span className="text-gray-400">Schemas:</span> <span className="text-black">{m.schemas.join(', ')}</span></div>
                        ) : null}
                        {m.tables.length > 0 ? (
                          <div className="col-span-2"><span className="text-gray-400">Tables:</span> <span className="text-black font-mono text-[11px]">{m.tables.join(', ')}</span></div>
                        ) : null}
                        {entry.error ? (
                          <div className="col-span-2 mt-1"><span className="text-red-500">Error:</span> <span className="text-red-600">{entry.error}</span></div>
                        ) : null}
                      </div>
                    ) : (
                      <div className="text-[12px] text-gray-400">No metadata available for this backup.</div>
                    )}
                  </td>
                </tr>
              ) : null}
            </Fragment>
          );
        })}
      </tbody>
    </table>
  );
}

function qi(v: string) { return `"${v.replace(/"/g, '""')}"`; }

const COMMON_TYPES = ['serial', 'bigserial', 'integer', 'bigint', 'smallint', 'text', 'varchar(255)', 'boolean', 'timestamp', 'timestamptz', 'date', 'numeric', 'real', 'double precision', 'uuid', 'jsonb', 'json', 'bytea'];
const ALL_DAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'] as const;
const DAY_LABELS: Record<string, string> = { sunday: 'Sun', monday: 'Mon', tuesday: 'Tue', wednesday: 'Wed', thursday: 'Thu', friday: 'Fri', saturday: 'Sat' };

function MonitoringSection({ title, children }: PropsWithChildren<{ title: string }>) {
  return (
    <div className="mb-4 rounded-lg border border-black/8 overflow-hidden">
      <div className="bg-gray-50 px-3 py-1.5 text-[13px] font-semibold text-black border-b border-black/5">{title}</div>
      <div className="p-3">{children}</div>
    </div>
  );
}

function MonitoringMiniTable({ headers, rows }: { headers: string[]; rows: (string | number)[][] }) {
  return (
    <table className="min-w-full border-collapse text-[12px]">
      <thead><tr>{headers.map((h) => <th key={h} className="border-b border-black/8 px-3 py-1 text-left font-medium text-black">{h}</th>)}</tr></thead>
      <tbody>
        {rows.length === 0 ? <tr><td colSpan={headers.length} className="px-3 py-2 text-gray-400">None</td></tr> : null}
        {rows.map((row, i) => (
          <tr key={i} className="border-b border-black/5 hover:bg-gray-50">
            {row.map((cell, j) => <td key={j} className="px-3 py-1 text-black">{cell}</td>)}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function MonitoringPanel({ hostStats, monitoringData, cpuHistory, ramHistory, tpsHistory, saturationHistory, cacheHitHistory, queryHistory }: {
  hostStats: HostStats | null;
  monitoringData: MonitoringData | null;
  activeQueries: null;
  cpuHistory: number[];
  ramHistory: number[];
  tpsHistory: number[];
  saturationHistory: number[];
  cacheHitHistory: number[];
  queryHistory: QueryHistoryEntry[];
}) {
  if (!hostStats && !monitoringData) return <div className="flex h-full items-center justify-center text-[12px] text-gray-400">Connect to a database to view monitoring data</div>;

  return (
    <div className="p-4">
      <MonitoringSection title="Server Overview">
        <div className="flex flex-wrap gap-2">
          <DashboardGraphCard label={"Server\nCPU"} value={hostStats?.cpuUsagePercent != null ? `${hostStats.cpuUsagePercent}%` : '--'} data={cpuHistory} max={100} color="#3b82f6" warnColor="#ef4444" warn={hostStats?.cpuUsagePercent != null && hostStats.cpuUsagePercent > 80} />
          <DashboardGraphCard label={"Server\nRAM"} value={hostStats?.memUsagePercent != null ? `${hostStats.memUsagePercent}%` : '--'} subtitle={hostStats?.memUsedMb != null ? `${hostStats.memUsedMb}/${hostStats.memTotalMb} MB` : undefined} data={ramHistory} max={100} color="#8b5cf6" warnColor="#ef4444" warn={hostStats?.memUsagePercent != null && hostStats.memUsagePercent > 80} />
          <DashboardGraphCard label={"Cache\nHit"} value={hostStats?.cacheHitRatio != null ? `${hostStats.cacheHitRatio}%` : '--'} data={cacheHitHistory} max={100} color="#06b6d4" />
          <DashboardGraphCard label="TXN Throughput" value={hostStats?.tps != null ? `${hostStats.tps} tps` : '--'} data={tpsHistory} color="#f59e0b" />
          <DashboardGraphCard label="Conn Saturation" value={hostStats?.connectionSaturationPercent != null ? `${hostStats.connectionSaturationPercent}%` : '--'} subtitle={hostStats?.activeConnections != null ? `${hostStats.activeConnections}/${hostStats.maxConnections}` : undefined} data={saturationHistory} max={100} color="#10b981" warnColor="#ef4444" warn={hostStats?.connectionSaturationPercent != null && hostStats.connectionSaturationPercent > 80} />
          <DashboardGraphCard label="Uptime" value={hostStats?.uptime != null ? String(hostStats.uptime).replace(/ /g, '\n') : '--'} data={[]} color="#6366f1" />
          <DashboardGraphCard label="DB Size" value={hostStats?.dbSizeMb != null ? (hostStats.dbSizeMb >= 1024 ? `${(hostStats.dbSizeMb / 1024).toFixed(1)} GB` : `${hostStats.dbSizeMb} MB`) : '--'} data={[]} color="#ec4899" />
          <DashboardGraphCard label="Deadlocks" value={monitoringData?.deadlocks != null ? String(monitoringData.deadlocks) : '--'} data={[]} color="#ef4444" warn={monitoringData != null && monitoringData.deadlocks > 0} warnColor="#ef4444" />
          <DashboardGraphCard label="Temp Files" value={monitoringData?.tempFiles != null ? String(monitoringData.tempFiles) : '--'} subtitle={monitoringData?.tempBytes ? `${(monitoringData.tempBytes / 1024 / 1024).toFixed(1)} MB` : undefined} data={[]} color="#f97316" />
        </div>
      </MonitoringSection>

      {monitoringData ? (
        <>
          <MonitoringSection title="Checkpoints & Buffers">
            <div className="grid grid-cols-5 gap-3 text-[12px]">
              <div className="rounded-lg border border-black/5 px-3 py-2"><span className="text-gray-400">Timed</span><div className="font-medium text-black">{monitoringData.checkpointsTimed}</div></div>
              <div className="rounded-lg border border-black/5 px-3 py-2"><span className="text-gray-400">Requested</span><div className="font-medium text-black">{monitoringData.checkpointsReq}</div></div>
              <div className="rounded-lg border border-black/5 px-3 py-2"><span className="text-gray-400">Buf Checkpoint</span><div className="font-medium text-black">{monitoringData.buffersCheckpoint}</div></div>
              <div className="rounded-lg border border-black/5 px-3 py-2"><span className="text-gray-400">Buf BGWriter</span><div className="font-medium text-black">{monitoringData.buffersBgwriter}</div></div>
              <div className="rounded-lg border border-black/5 px-3 py-2"><span className="text-gray-400">Buf Backend</span><div className="font-medium text-black">{monitoringData.buffersBackend}</div></div>
            </div>
          </MonitoringSection>

          <MonitoringSection title="Connections">
            <div className="mb-3">
              <div className="mb-1 text-[11px] font-medium text-gray-500">By State</div>
              <MonitoringMiniTable headers={['State', 'Count']} rows={monitoringData.connectionsByState.map((r) => [r.state, r.count])} />
            </div>
            <div>
              <div className="mb-1 text-[11px] font-medium text-gray-500">By User</div>
              <MonitoringMiniTable headers={['User', 'Count']} rows={monitoringData.connectionsByUser.map((r) => [r.user, r.count])} />
            </div>
          </MonitoringSection>

          <MonitoringSection title="Locks">
            <div className="mb-3">
              <div className="mb-1 text-[11px] font-medium text-gray-500">By Type</div>
              <MonitoringMiniTable headers={['Type', 'Mode', 'Count']} rows={monitoringData.locksByType.map((r) => [r.locktype, r.mode, r.count])} />
            </div>
            <div>
              <div className="mb-1 text-[11px] font-medium text-gray-500">Blocked Queries</div>
              <MonitoringMiniTable headers={['PID', 'Waiting For', 'Duration', 'Query']} rows={monitoringData.blockedQueries.map((r) => [r.pid, r.waitingFor, `${(r.durationMs / 1000).toFixed(1)}s`, r.query.substring(0, 60)])} />
            </div>
          </MonitoringSection>

          {monitoringData.longRunningTxns.length > 0 ? (
            <MonitoringSection title="Long Running Transactions (>1 min)">
              <MonitoringMiniTable headers={['PID', 'User', 'Duration', 'State', 'Query']} rows={monitoringData.longRunningTxns.map((r) => [r.pid, r.user, r.duration.split('.')[0], r.state, r.query.substring(0, 80)])} />
            </MonitoringSection>
          ) : null}

          <MonitoringSection title="Query History">
            <MonitoringMiniTable headers={['Query', 'Result']} rows={queryHistory.map((e) => [e.sql.substring(0, 100), e.resultMeta])} />
          </MonitoringSection>

          <MonitoringSection title="Table Statistics (Top 50)">
            <div className="overflow-x-auto">
              <MonitoringMiniTable headers={['Schema', 'Table', 'Seq Scan', 'Idx Scan', 'Ins', 'Upd', 'Del', 'Dead', 'Size', 'Last Vacuum']}
                rows={monitoringData.tableStats.map((r) => [r.schema, r.table, r.seqScan, r.idxScan, r.rowsInserted, r.rowsUpdated, r.rowsDeleted, r.deadTuples, r.tableSize, r.lastVacuum?.split('.')[0] ?? 'Never'])} />
            </div>
          </MonitoringSection>

          {monitoringData.unusedIndexes.length > 0 ? (
            <MonitoringSection title="Unused Indexes">
              <MonitoringMiniTable headers={['Schema', 'Table', 'Index', 'Size']} rows={monitoringData.unusedIndexes.map((r) => [r.schema, r.table, r.index, r.size])} />
            </MonitoringSection>
          ) : null}

          {monitoringData.replicationLag.length > 0 ? (
            <MonitoringSection title="Replication">
              <MonitoringMiniTable headers={['Client', 'State', 'Write Lag', 'Flush Lag', 'Replay Lag']} rows={monitoringData.replicationLag.map((r) => [r.clientAddr, r.state, r.writeLag, r.flushLag, r.replayLag])} />
            </MonitoringSection>
          ) : null}
        </>
      ) : null}
    </div>
  );
}

function ScheduleBackupModal({ tree, database, backupDir, existing, onClose, onSave }: {
  tree: SchemaNode[];
  database: string;
  backupDir: string;
  existing: BackupSchedule | null;
  onClose: () => void;
  onSave: (schedule: Omit<BackupSchedule, 'id' | 'createdAt'>) => void;
}) {
  const [days, setDays] = useState<Set<string>>(new Set(existing?.days ?? []));
  const [time, setTime] = useState(existing?.time ?? '02:00');
  const [format, setFormat] = useState(existing?.format ?? 'tar');
  const [scope, setScope] = useState<'full' | 'selected'>(existing?.scope === 'selected' ? 'selected' : 'full');
  const [selectedSchemas, setSelectedSchemas] = useState<Set<string>>(new Set(existing?.schemas ?? []));
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set(existing?.tables ?? []));
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [dataOnly, setDataOnly] = useState(existing?.dataOnly ?? false);
  const [schemaOnly, setSchemaOnly] = useState(existing?.schemaOnly ?? false);
  const [noOwner, setNoOwner] = useState(existing?.noOwner ?? true);
  const [noPrivileges, setNoPrivileges] = useState(existing?.noPrivileges ?? true);

  function toggleDay(day: string) {
    setDays((prev) => { const next = new Set(prev); if (next.has(day)) next.delete(day); else next.add(day); return next; });
  }

  function toggleSchema(schemaName: string) {
    const schema = tree.find((s) => s.name === schemaName);
    if (!schema) return;
    const allTables = schema.tables.map((t) => `${schemaName}.${t.name}`);
    setSelectedSchemas((prev) => {
      const next = new Set(prev);
      if (next.has(schemaName)) {
        next.delete(schemaName);
        setSelectedTables((st) => { const n = new Set(st); allTables.forEach((t) => n.delete(t)); return n; });
      } else {
        next.add(schemaName);
        setSelectedTables((st) => { const n = new Set(st); allTables.forEach((t) => n.add(t)); return n; });
      }
      return next;
    });
  }

  function toggleTable(schemaName: string, tableName: string) {
    const key = `${schemaName}.${tableName}`;
    setSelectedTables((prev) => { const next = new Set(prev); if (next.has(key)) next.delete(key); else next.add(key); return next; });
  }

  function handleSave() {
    onSave({
      days: [...days],
      time,
      format,
      schemas: scope === 'selected' ? [...selectedSchemas] : [],
      tables: scope === 'selected' ? [...selectedTables] : [],
      scope,
      dataOnly,
      schemaOnly,
      noOwner,
      noPrivileges,
      outputDir: backupDir,
      enabled: true,
    });
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 backdrop-blur-sm" onClick={onClose}>
      <div className="glass-panel-strong flex h-[80vh] w-[600px] max-w-[90vw] flex-col overflow-hidden rounded-2xl shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex h-10 shrink-0 items-center justify-between border-b border-black/5 px-4">
          <span className="text-[14px] font-medium text-black">{existing ? 'Modify Backup Schedule' : 'Schedule Backup'}</span>
          <button className="text-[18px] text-gray-400 hover:text-black" onClick={onClose} type="button">&times;</button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4">
          <div className="mb-4">
            <label className="mb-2 block text-[12px] font-medium text-black">Days</label>
            <div className="flex gap-1.5">
              {ALL_DAYS.map((day) => (
                <button
                  key={day}
                  className={classNames('rounded-lg border px-3 py-1.5 text-[12px]', days.has(day) ? 'border-[var(--accent)] bg-[var(--accent)]/10 text-[var(--accent)]' : 'border-black/10 text-gray-500 hover:text-black')}
                  onClick={() => toggleDay(day)}
                  type="button"
                >
                  {DAY_LABELS[day]}
                </button>
              ))}
            </div>
          </div>

          <div className="mb-4">
            <label className="mb-1 block text-[12px] font-medium text-black">Start Time</label>
            <input type="time" className="input w-32 text-[12px]" value={time} onChange={(e) => setTime(e.target.value)} />
          </div>

          <div className="mb-4">
            <label className="mb-1 block text-[12px] font-medium text-black">Format</label>
            <div className="flex gap-2">
              {(['tar', 'custom', 'sql'] as const).map((f) => (
                <button key={f} className={classNames('rounded-lg border px-3 py-1.5 text-[12px]', format === f ? 'border-[var(--accent)] bg-[var(--accent)]/10 text-[var(--accent)]' : 'border-black/10 text-gray-500 hover:text-black')} onClick={() => setFormat(f)} type="button">
                  {f === 'tar' ? 'Tar' : f === 'custom' ? 'Custom' : 'SQL'}
                </button>
              ))}
            </div>
          </div>

          <div className="mb-4">
            <label className="mb-1 block text-[12px] font-medium text-black">Scope</label>
            <div className="mb-2 flex gap-3">
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="radio" checked={scope === 'full'} onChange={() => setScope('full')} /> Entire Database</label>
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="radio" checked={scope === 'selected'} onChange={() => setScope('selected')} /> Selected Objects</label>
            </div>
            {scope === 'selected' ? (
              <div className="max-h-[150px] overflow-y-auto rounded-lg border border-black/10 bg-white p-2">
                {tree.length === 0 ? (
                  <div className="text-[12px] text-gray-400">Connect to a database to see schemas and tables</div>
                ) : tree.map((schema) => {
                  const allTables = schema.tables.map((t) => `${schema.name}.${t.name}`);
                  const allSelected = allTables.length > 0 && allTables.every((t) => selectedTables.has(t));
                  const someSelected = allTables.some((t) => selectedTables.has(t));
                  const expanded = expandedSchemas.has(schema.name);
                  return (
                    <div key={schema.name}>
                      <div className="flex items-center gap-2 py-0.5">
                        <button className="w-4 shrink-0 text-center text-gray-500" onClick={() => setExpandedSchemas((p) => { const n = new Set(p); if (n.has(schema.name)) n.delete(schema.name); else n.add(schema.name); return n; })} type="button">{expanded ? '\u25BE' : '\u25B8'}</button>
                        <input type="checkbox" checked={allSelected} ref={(el) => { if (el) el.indeterminate = someSelected && !allSelected; }} onChange={() => toggleSchema(schema.name)} />
                        <span className="text-[12px] font-medium text-black">{schema.name}</span>
                      </div>
                      {expanded ? schema.tables.map((table) => (
                        <div className="flex items-center gap-2 py-0.5 pl-10" key={table.name}>
                          <input type="checkbox" checked={selectedTables.has(`${schema.name}.${table.name}`)} onChange={() => toggleTable(schema.name, table.name)} />
                          <span className="text-[12px] text-black">{table.name}</span>
                        </div>
                      )) : null}
                    </div>
                  );
                })}
              </div>
            ) : null}
          </div>

          <div className="mb-4">
            <label className="mb-1 block text-[12px] font-medium text-black">Content</label>
            <div className="flex gap-3">
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="radio" checked={!dataOnly && !schemaOnly} onChange={() => { setDataOnly(false); setSchemaOnly(false); }} /> Schema + Data</label>
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="radio" checked={schemaOnly} onChange={() => { setSchemaOnly(true); setDataOnly(false); }} /> Schema Only</label>
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="radio" checked={dataOnly} onChange={() => { setDataOnly(true); setSchemaOnly(false); }} /> Data Only</label>
            </div>
          </div>

          <div className="mb-4">
            <label className="mb-2 block text-[12px] font-medium text-black">Options</label>
            <div className="grid grid-cols-2 gap-x-4 gap-y-1.5">
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="checkbox" checked={noOwner} onChange={(e) => setNoOwner(e.target.checked)} /> Do not output owner</label>
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="checkbox" checked={noPrivileges} onChange={(e) => setNoPrivileges(e.target.checked)} /> Do not output privileges</label>
            </div>
          </div>
        </div>

        <div className="border-t border-black/5 px-5 py-3">
          <div className="mb-2 rounded-lg bg-amber-50 px-3 py-2 text-[11px] text-amber-700">
            Scheduled backups only run while PostGrip is open with an active database connection. If the app is closed at the scheduled time, the backup will be skipped.
          </div>
          <div className="flex items-center justify-end gap-2">
            <button className="rounded-lg border border-black/10 px-4 py-1.5 text-[12px] text-gray-500 hover:text-black" onClick={onClose} type="button">Cancel</button>
            <button className="rounded-lg bg-[var(--accent)] px-4 py-1.5 text-[12px] text-white hover:opacity-90" onClick={handleSave} type="button">{existing ? 'Update Schedule' : 'Add Backup Schedule'}</button>
          </div>
        </div>
      </div>
    </div>
  );
}

function BackupModal({ tree, database, backupDir, onClose, onExecute }: {
  tree: SchemaNode[];
  database: string;
  backupDir: string;
  onClose: () => void;
  onExecute: (options: BackupOptions) => void;
}) {
  const [format, setFormat] = useState('tar');
  const [scope, setScope] = useState<'full' | 'selected'>('full');
  const [selectedSchemas, setSelectedSchemas] = useState<Set<string>>(new Set());
  const [selectedTables, setSelectedTables] = useState<Set<string>>(new Set());
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [dataOnly, setDataOnly] = useState(false);
  const [schemaOnly, setSchemaOnly] = useState(false);
  const [noOwner, setNoOwner] = useState(true);
  const [noPrivileges, setNoPrivileges] = useState(true);
  const [clean, setClean] = useState(false);
  const [createDb, setCreateDb] = useState(false);
  const [ifExists, setIfExists] = useState(false);
  const [compress, setCompress] = useState(0);

  const defaultFileName = useMemo(() => {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const ext = format === 'custom' ? '.dump' : format === 'tar' ? '.tar' : format === 'directory' ? '' : '.sql';
    return `${database}_${timestamp}${ext}`;
  }, [database, format]);

  const [outputPath, setOutputPath] = useState(() => {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    return `${backupDir}/${database}_${timestamp}.tar`;
  });

  // Update extension when format changes
  useEffect(() => {
    setOutputPath((prev) => {
      const dir = prev.substring(0, prev.lastIndexOf('/'));
      return `${dir}/${defaultFileName}`;
    });
  }, [format, defaultFileName]);

  async function browseOutputPath() {
    const ext = format === 'custom' ? 'dump' : format === 'tar' ? 'tar' : 'sql';
    const result = await api.showSaveDialog({
      defaultPath: outputPath,
      filters: [{ name: 'Backup Files', extensions: [ext] }],
    });
    if (result) setOutputPath(result);
  }

  function toggleSchema(schemaName: string) {
    const schema = tree.find((s) => s.name === schemaName);
    if (!schema) return;
    const allTables = schema.tables.map((t) => `${schemaName}.${t.name}`);
    setSelectedSchemas((prev) => {
      const next = new Set(prev);
      if (next.has(schemaName)) {
        next.delete(schemaName);
        setSelectedTables((st) => { const n = new Set(st); allTables.forEach((t) => n.delete(t)); return n; });
      } else {
        next.add(schemaName);
        setSelectedTables((st) => { const n = new Set(st); allTables.forEach((t) => n.add(t)); return n; });
      }
      return next;
    });
  }

  function toggleTable(schemaName: string, tableName: string) {
    const key = `${schemaName}.${tableName}`;
    setSelectedTables((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key); else next.add(key);
      return next;
    });
  }

  function toggleExpandSchema(schemaName: string) {
    setExpandedSchemas((prev) => {
      const next = new Set(prev);
      if (next.has(schemaName)) next.delete(schemaName); else next.add(schemaName);
      return next;
    });
  }

  function handleExecute() {
    const options: BackupOptions = { filePath: outputPath, format, noOwner, noPrivileges };
    if (scope === 'selected') {
      if (selectedSchemas.size > 0 && selectedTables.size === 0) {
        options.schemas = [...selectedSchemas];
      } else if (selectedTables.size > 0) {
        options.tables = [...selectedTables];
      }
    }
    if (dataOnly) options.dataOnly = true;
    if (schemaOnly) options.schemaOnly = true;
    if (clean) options.clean = true;
    if (createDb) options.createDb = true;
    if (ifExists) options.ifExists = true;
    if (compress > 0) options.compress = compress;
    onExecute(options);
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 backdrop-blur-sm" onClick={onClose}>
      <div className="glass-panel-strong flex h-[80vh] w-[600px] max-w-[90vw] flex-col overflow-hidden rounded-2xl shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex h-10 shrink-0 items-center justify-between border-b border-black/5 px-4">
          <span className="text-[14px] font-medium text-black">Backup Database</span>
          <button className="text-[18px] text-gray-400 hover:text-black" onClick={onClose} type="button">&times;</button>
        </div>

        <div className="flex-1 overflow-y-auto px-5 py-4">
          <div className="mb-4">
            <label className="mb-1 block text-[12px] font-medium text-black">Format</label>
            <div className="flex gap-2">
              {(['tar', 'custom', 'sql', 'directory'] as const).map((f) => (
                <button
                  key={f}
                  className={classNames('rounded-lg border px-3 py-1.5 text-[12px]', format === f ? 'border-[var(--accent)] bg-[var(--accent)]/10 text-[var(--accent)]' : 'border-black/10 text-gray-500 hover:text-black')}
                  onClick={() => setFormat(f)}
                  type="button"
                >
                  {f === 'tar' ? 'Tar Archive' : f === 'custom' ? 'Custom (pg_restore)' : f === 'sql' ? 'Plain SQL' : 'Directory'}
                </button>
              ))}
            </div>
          </div>

          <div className="mb-4">
            <label className="mb-1 block text-[12px] font-medium text-black">Scope</label>
            <div className="mb-2 flex gap-3">
              <label className="flex items-center gap-1.5 text-[12px] text-black">
                <input type="radio" checked={scope === 'full'} onChange={() => setScope('full')} /> Entire Database
              </label>
              <label className="flex items-center gap-1.5 text-[12px] text-black">
                <input type="radio" checked={scope === 'selected'} onChange={() => setScope('selected')} /> Selected Objects
              </label>
            </div>
            {scope === 'selected' ? (
              <div className="max-h-[200px] overflow-y-auto rounded-lg border border-black/10 bg-white p-2">
                {tree.length === 0 ? (
                  <div className="text-[12px] text-gray-400">Connect to a database to see schemas and tables</div>
                ) : tree.map((schema) => {
                  const allTables = schema.tables.map((t) => `${schema.name}.${t.name}`);
                  const allSelected = allTables.length > 0 && allTables.every((t) => selectedTables.has(t));
                  const someSelected = allTables.some((t) => selectedTables.has(t));
                  const expanded = expandedSchemas.has(schema.name);
                  return (
                    <div key={schema.name}>
                      <div className="flex items-center gap-2 py-0.5">
                        <button className="w-4 shrink-0 text-center text-gray-500" onClick={() => toggleExpandSchema(schema.name)} type="button">{expanded ? '\u25BE' : '\u25B8'}</button>
                        <input
                          type="checkbox"
                          checked={allSelected}
                          ref={(el) => { if (el) el.indeterminate = someSelected && !allSelected; }}
                          onChange={() => toggleSchema(schema.name)}
                        />
                        <span className="text-[12px] font-medium text-black">{schema.name}</span>
                        <span className="text-[11px] text-gray-400">{schema.tables.length} tables</span>
                      </div>
                      {expanded ? schema.tables.map((table) => (
                        <div className="flex items-center gap-2 py-0.5 pl-10" key={table.name}>
                          <input
                            type="checkbox"
                            checked={selectedTables.has(`${schema.name}.${table.name}`)}
                            onChange={() => toggleTable(schema.name, table.name)}
                          />
                          <span className="text-[12px] text-black">{table.name}</span>
                          <span className="text-[10px] text-gray-400">{table.tableType === 'VIEW' ? 'view' : ''}</span>
                        </div>
                      )) : null}
                    </div>
                  );
                })}
              </div>
            ) : null}
          </div>

          <div className="mb-4">
            <label className="mb-1 block text-[12px] font-medium text-black">Content</label>
            <div className="flex gap-3">
              <label className="flex items-center gap-1.5 text-[12px] text-black">
                <input type="radio" checked={!dataOnly && !schemaOnly} onChange={() => { setDataOnly(false); setSchemaOnly(false); }} /> Schema + Data
              </label>
              <label className="flex items-center gap-1.5 text-[12px] text-black">
                <input type="radio" checked={schemaOnly} onChange={() => { setSchemaOnly(true); setDataOnly(false); }} /> Schema Only
              </label>
              <label className="flex items-center gap-1.5 text-[12px] text-black">
                <input type="radio" checked={dataOnly} onChange={() => { setDataOnly(true); setSchemaOnly(false); }} /> Data Only
              </label>
            </div>
          </div>

          <div className="mb-4">
            <label className="mb-2 block text-[12px] font-medium text-black">Options</label>
            <div className="grid grid-cols-2 gap-x-4 gap-y-1.5">
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="checkbox" checked={noOwner} onChange={(e) => setNoOwner(e.target.checked)} /> Do not output owner</label>
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="checkbox" checked={noPrivileges} onChange={(e) => setNoPrivileges(e.target.checked)} /> Do not output privileges</label>
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="checkbox" checked={clean} onChange={(e) => setClean(e.target.checked)} /> Clean (DROP before CREATE)</label>
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="checkbox" checked={createDb} onChange={(e) => setCreateDb(e.target.checked)} /> Include CREATE DATABASE</label>
              <label className="flex items-center gap-1.5 text-[12px] text-black"><input type="checkbox" checked={ifExists} onChange={(e) => setIfExists(e.target.checked)} /> Use IF EXISTS with DROP</label>
            </div>
          </div>

          {format === 'custom' || format === 'directory' ? (
            <div className="mb-4">
              <label className="mb-1 block text-[12px] font-medium text-black">Compression Level (0=none, 9=max)</label>
              <input type="range" min={0} max={9} value={compress} onChange={(e) => setCompress(Number(e.target.value))} className="w-48" />
              <span className="ml-2 text-[12px] text-gray-500">{compress}</span>
            </div>
          ) : null}

          <div>
            <label className="mb-1 block text-[12px] font-medium text-black">Output File</label>
            <div className="flex items-center gap-2">
              <input
                className="input flex-1 text-[12px]"
                value={outputPath}
                onChange={(e) => setOutputPath(e.target.value)}
              />
              <button className="shrink-0 rounded-lg border border-black/10 px-3 py-1.5 text-[12px] text-gray-500 hover:text-black" onClick={() => void browseOutputPath()} type="button">Browse</button>
            </div>
          </div>
        </div>

        <div className="flex items-center justify-end gap-2 border-t border-black/5 px-5 py-3">
          <button className="rounded-lg border border-black/10 px-4 py-1.5 text-[12px] text-gray-500 hover:text-black" onClick={onClose} type="button">Cancel</button>
          <button className="rounded-lg bg-[var(--accent)] px-4 py-1.5 text-[12px] text-white hover:opacity-90" onClick={handleExecute} type="button">Start Backup</button>
        </div>
      </div>
    </div>
  );
}

function MarkdownViewer({ content }: { content: string }) {
  const html = useMemo(() => {
    let result = content
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');

    // Headings
    result = result.replace(/^### (.+)$/gm, '<h3 class="mt-5 mb-2 text-[15px] font-semibold text-black">$1</h3>');
    result = result.replace(/^## (.+)$/gm, '<h2 class="mt-6 mb-2 text-[17px] font-bold text-black border-b border-black/10 pb-1">$1</h2>');
    result = result.replace(/^# (.+)$/gm, '<h1 class="mt-4 mb-3 text-[22px] font-bold text-black">$1</h1>');

    // Horizontal rules
    result = result.replace(/^---$/gm, '<hr class="my-4 border-black/10" />');

    // Bold and italic
    result = result.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
    result = result.replace(/\*(.+?)\*/g, '<em>$1</em>');

    // Inline code
    result = result.replace(/`([^`]+)`/g, '<code class="rounded bg-black/5 px-1 py-0.5 text-[12px] font-mono">$1</code>');

    // Tables
    result = result.replace(/^\|(.+)\|$/gm, (match) => {
      const cells = match.split('|').filter(Boolean).map((c) => c.trim());
      if (cells.every((c) => /^[-:]+$/.test(c))) return '';
      const tag = match.includes('---') ? 'th' : 'td';
      const row = cells.map((c) => `<${tag} class="border border-black/10 px-3 py-1.5 text-left text-[12px]">${c}</${tag}>`).join('');
      return `<tr>${row}</tr>`;
    });
    result = result.replace(/(<tr>.*<\/tr>\n?)+/g, (block) =>
      `<table class="my-2 w-full border-collapse text-[12px]">${block}</table>`
    );

    // Lists
    result = result.replace(/^(\d+)\. (.+)$/gm, '<li class="ml-4 list-decimal text-[13px] text-black leading-relaxed">$2</li>');
    result = result.replace(/^- (.+)$/gm, '<li class="ml-4 list-disc text-[13px] text-black leading-relaxed">$1</li>');

    // Paragraphs
    result = result.replace(/\n\n/g, '</p><p class="mb-2 text-[13px] leading-relaxed text-black/80">');
    result = '<p class="mb-2 text-[13px] leading-relaxed text-black/80">' + result + '</p>';

    // Clean up empty paragraphs
    result = result.replace(/<p[^>]*>\s*<\/p>/g, '');

    return result;
  }, [content]);

  return <div dangerouslySetInnerHTML={{ __html: html }} />;
}

function DropdownMenu({ label, active, onToggle, children }: PropsWithChildren<{ label: string; active: boolean; onToggle: () => void }>) {
  return (
    <div className="relative" onClick={(e) => e.stopPropagation()}>
      <button
        className={classNames(
          'rounded-md px-2 py-1 text-[12px] text-black/60 hover:bg-black/5 hover:text-black',
          active && 'bg-black/5 text-black',
        )}
        onClick={onToggle}
        type="button"
      >
        {label}
      </button>
      {active ? (
        <div className="context-menu absolute left-0 top-full z-50 mt-1 min-w-[220px] rounded-lg py-1 text-[12px] text-black">
          {children}
        </div>
      ) : null}
    </div>
  );
}

function MenuButton({ active, label, onClick }: { active: boolean; label: string; onClick: () => void }) {
  return (
    <button
      className={classNames(
        'rounded-md px-2 py-1 text-[12px] text-black/60 hover:bg-black/5 hover:text-black',
        active && 'bg-black/5 text-black',
      )}
      onClick={onClick}
      type="button"
    >
      {label}
    </button>
  );
}

function MenuPanel({ children }: PropsWithChildren) {
  return <div className="py-1 text-[12px] text-black">{children}</div>;
}

function MenuItem({ label, shortcut, onClick }: { label: string; shortcut?: string; onClick: () => void }) {
  return (
    <button
      className="flex w-full items-center justify-between px-3 py-1.5 text-left text-black hover:bg-black/5"
      onClick={onClick}
      type="button"
    >
      <span>{label}</span>
      {shortcut ? <span className="ml-6 text-[11px] text-black/40">{shortcut}</span> : null}
    </button>
  );
}

function ToolbarIconButton({ children, onClick, title }: PropsWithChildren<{ onClick: () => void; title: string }>) {
  return (
    <button className="grid h-6 w-6 place-items-center rounded-md border border-transparent text-[12px] text-black/50 hover:border-black/10 hover:bg-black/5 hover:text-black/70" onClick={onClick} title={title} type="button">
      {children}
    </button>
  );
}

function ExplorerIcon({ children }: PropsWithChildren) {
  return <span className="grid w-4 shrink-0 place-items-center text-gray-500">{children}</span>;
}

function Field({ label, children }: PropsWithChildren<{ label: string }>) {
  return (
    <label className="block text-[12px] text-gray-500">
      <div className="mb-1.5 text-[11px] font-medium uppercase tracking-[0.08em] text-black">{label}</div>
      {children}
    </label>
  );
}

function Sparkline({ data, max, color, warnColor, warn, width = 120, height = 32 }: { data: number[]; max?: number; color: string; warnColor?: string; warn?: boolean; width?: number; height?: number }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [actualWidth, setActualWidth] = useState(width);

  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setActualWidth(entry.contentRect.width);
      }
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  const strokeColor = warn && warnColor ? warnColor : color;
  const fillColor = warn && warnColor ? warnColor : color;
  const gradId = `sg-${color.replace('#', '')}-${warn ? 'w' : 'n'}`;

  return (
    <div ref={containerRef} style={{ height }} className="w-full shrink-0">
      <svg width={actualWidth} height={height}>
        <defs>
          <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={fillColor} stopOpacity="0.25" />
            <stop offset="100%" stopColor={fillColor} stopOpacity="0.02" />
          </linearGradient>
        </defs>
        {data.length >= 2 ? (() => {
          const ceiling = max ?? Math.max(...data, 1);
          const step = actualWidth / (data.length - 1);
          const points = data.map((v, i) => `${i * step},${height - (v / ceiling) * height}`).join(' ');
          const fillPoints = `0,${height} ${points} ${(data.length - 1) * step},${height}`;
          return (
            <>
              <polygon points={fillPoints} fill={`url(#${gradId})`} />
              <polyline points={points} fill="none" stroke={strokeColor} strokeWidth="1.5" strokeLinejoin="round" strokeLinecap="round" />
            </>
          );
        })() : (
          <line x1="0" y1={height - 1} x2={actualWidth} y2={height - 1} stroke={color} strokeWidth="1" strokeOpacity="0.2" />
        )}
      </svg>
    </div>
  );
}

function DashboardGraphCard({ label, value, subtitle, data, max, color, warnColor, warn }: { label: string; value: string; subtitle?: string; data: number[]; max?: number; color: string; warnColor?: string; warn?: boolean }) {
  return (
    <div className="flex min-w-[140px] flex-1 flex-col gap-1 rounded-lg border border-gray-200 bg-white/40 px-3 py-2">
      <div className="flex items-baseline justify-between">
        <div className="whitespace-pre-line text-[10px] font-medium uppercase leading-tight tracking-[0.08em] text-gray-500">{label}</div>
        <div className={classNames('whitespace-pre-line text-right text-[16px] font-bold tabular-nums leading-tight', warn ? 'text-red-500' : 'text-black')}>{value}</div>
      </div>
      <Sparkline data={data} max={max} color={color} warnColor={warnColor} warn={warn} height={36} />
      {subtitle ? <div className="text-[9px] text-gray-400">{subtitle}</div> : null}
    </div>
  );
}

function DashboardCard({ label, value, accent, warn }: { label: string; value: string; accent?: boolean; warn?: boolean }) {
  return (
    <div className="flex min-w-[90px] flex-1 flex-col justify-center rounded-lg border border-gray-200 bg-white/40 px-3 py-2">
      <div className="text-[10px] font-medium uppercase tracking-[0.08em] text-gray-500">{label}</div>
      <div className={classNames('mt-0.5 truncate text-[14px] font-semibold', warn ? 'text-red-500' : accent ? 'text-emerald-600' : 'text-black')}>{value}</div>
    </div>
  );
}

function EmptyInline({ message }: { message: string }) {
  return <div className="px-1 py-1 text-gray-500">{message}</div>;
}

function WorkspaceEmpty({ title, body }: { title: string; body: string }) {
  return (
    <div className="grid min-h-[240px] place-items-center rounded-xl border border-dashed border-black/10 bg-white/20 p-5 text-center">
      <div>
        <div className="text-sm text-black">{title}</div>
        <div className="mt-1 text-sm text-gray-500">{body}</div>
      </div>
    </div>
  );
}

function TableTreeNode({ schema, table, onPreview, onContextMenu }: { schema: string; table: TableNode; onPreview: (schema: string, table: string) => Promise<void>; onContextMenu: (event: React.MouseEvent, schema: string, table: string) => void }) {
  const [open, setOpen] = useState(false);
  const [openSections, setOpenSections] = useState<Record<string, boolean>>({});

  const toggleSection = (section: string) => {
    setOpenSections((c) => ({ ...c, [section]: !c[section] }));
  };

  return (
    <div>
      <div
        className="flex cursor-grab items-center gap-1 pl-14 pr-2"
        draggable
        onDragStart={(event) => { event.dataTransfer.setData('text/plain', `${schema}.${table.name}`); event.dataTransfer.effectAllowed = 'copy'; }}
        onContextMenu={(event) => onContextMenu(event, schema, table.name)}
      >
        <button className="w-4 shrink-0 text-center text-[10px] text-gray-400" onClick={() => setOpen((c) => !c)} type="button">
          {open ? '\u25BE' : '\u25B8'}
        </button>
        <ExplorerIcon><TableIcon /></ExplorerIcon>
        <button className="flex-1 truncate py-0.5 text-left text-black hover:bg-white/40" onClick={() => void onPreview(schema, table.name)} type="button">
          {table.name}
        </button>
      </div>
      {open ? (
        <div className="pl-[72px]">
          {/* Columns */}
          <button className="flex w-full items-center gap-1 py-0.5 text-left text-[11px] text-gray-500 hover:text-black" onClick={() => toggleSection('columns')} type="button">
            <span className="w-3 text-center text-[9px] text-gray-400">{openSections.columns ? '\u25BE' : '\u25B8'}</span>
            <span>Columns</span>
            <span className="text-gray-400">({(table.columns ?? []).length})</span>
          </button>
          {openSections.columns ? (
            <div className="pl-4">
              {table.columns.map((col) => (
                <div key={col.name} className="flex items-center gap-2 py-[1px] text-[11px]">
                  <span className="text-gray-400">&#9702;</span>
                  <span className="text-black">{col.name}</span>
                  <span className="text-gray-400">{col.dataType}</span>
                  {!col.nullable && <span className="text-[9px] text-red-400">NN</span>}
                </div>
              ))}
            </div>
          ) : null}

          {/* Keys */}
          <button className="flex w-full items-center gap-1 py-0.5 text-left text-[11px] text-gray-500 hover:text-black" onClick={() => toggleSection('keys')} type="button">
            <span className="w-3 text-center text-[9px] text-gray-400">{openSections.keys ? '\u25BE' : '\u25B8'}</span>
            <span>Keys</span>
            <span className="text-gray-400">({(table.keys ?? []).length})</span>
          </button>
          {openSections.keys ? (
            <div className="pl-4">
              {(table.keys ?? []).length > 0 ? (table.keys ?? []).map((k) => (
                <div key={k.name} className="flex items-center gap-2 py-[1px] text-[11px]">
                  <span className={k.type === 'PRIMARY KEY' ? 'text-amber-500' : k.type === 'FOREIGN KEY' ? 'text-blue-500' : 'text-gray-400'}>&#9670;</span>
                  <span className="text-black">{k.name}</span>
                  <span className="text-gray-400">{Array.isArray(k.columns) ? k.columns.join(', ') : String(k.columns ?? '')}</span>
                  {k.referencedTable ? <span className="text-[9px] text-blue-400">&rarr; {k.referencedTable}</span> : null}
                </div>
              )) : (
                <div className="py-[1px] text-[11px] text-gray-400">None</div>
              )}
            </div>
          ) : null}

          {/* Indexes */}
          <button className="flex w-full items-center gap-1 py-0.5 text-left text-[11px] text-gray-500 hover:text-black" onClick={() => toggleSection('indexes')} type="button">
            <span className="w-3 text-center text-[9px] text-gray-400">{openSections.indexes ? '\u25BE' : '\u25B8'}</span>
            <span>Indexes</span>
            <span className="text-gray-400">({(table.indexes ?? []).length})</span>
          </button>
          {openSections.indexes ? (
            <div className="pl-4">
              {table.indexes.length > 0 ? table.indexes.map((idx) => (
                <div key={idx.name} className="flex items-center gap-2 py-[1px] text-[11px]">
                  <span className="text-gray-400">&#9656;</span>
                  <span className="text-black">{idx.name}</span>
                  <span className="text-gray-400">{Array.isArray(idx.columns) ? idx.columns.join(', ') : String(idx.columns ?? '')}</span>
                  {idx.isUnique && <span className="text-[9px] text-amber-500">UQ</span>}
                </div>
              )) : (
                <div className="py-[1px] text-[11px] text-gray-400">None</div>
              )}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function DatabaseTree({ connection, tree, onPreview, onTableContextMenu, onDatabaseContextMenu, onSchemaContextMenu }: { connection: ActiveConnectionSummary; tree: SchemaNode[]; onPreview: (schema: string, table: string) => Promise<void>; onTableContextMenu: (event: React.MouseEvent, schema: string, table: string) => void; onDatabaseContextMenu: (event: React.MouseEvent) => void; onSchemaContextMenu: (event: React.MouseEvent, schema: string) => void; }) {
  const [dbOpen, setDbOpen] = useState(true);
  const [openSchemas, setOpenSchemas] = useState<Record<string, boolean>>({});

  useEffect(() => {
    setOpenSchemas((current) => {
      const next = { ...current };
      for (const schema of tree) {
        if (!(schema.name in next)) {
          next[schema.name] = true;
        }
      }
      return next;
    });
  }, [tree]);

  return (
    <div className="overflow-auto">
      <button className="flex w-full items-center gap-2 px-2 py-1 text-left hover:bg-white/40" onClick={() => setDbOpen((c) => !c)} onContextMenu={(e) => { e.preventDefault(); onDatabaseContextMenu(e); }} type="button">
        <span className="w-4 shrink-0 text-center text-gray-500">{dbOpen ? '\u25BE' : '\u25B8'}</span>
        <ExplorerIcon><DatabaseIcon /></ExplorerIcon>
        <span className="flex-1 truncate text-black">{connection.database}@{connection.host}</span>
      </button>
      {dbOpen ? (
        <div>
          {tree.map((schema) => {
            const schemaOpen = openSchemas[schema.name] ?? true;
            return (
              <div key={schema.name}>
                <button className="flex w-full items-center gap-2 py-1 pl-8 pr-2 text-left hover:bg-white/40" onClick={() => setOpenSchemas((current) => ({ ...current, [schema.name]: !schemaOpen }))} onContextMenu={(e) => { e.preventDefault(); onSchemaContextMenu(e, schema.name); }} type="button">
                  <span className="w-4 shrink-0 text-center text-gray-500">{schemaOpen ? '\u25BE' : '\u25B8'}</span>
                  <ExplorerIcon><FolderIcon /></ExplorerIcon>
                  <span className="flex-1 truncate text-black">{schema.name}</span>
                  <span className="shrink-0 text-gray-500">{schema.tables.length}</span>
                </button>
                {schemaOpen ? (
                  <div>
                    {schema.tables.map((table) => (
                      <TableTreeNode
                        key={`${schema.name}.${table.name}`}
                        schema={schema.name}
                        table={table}
                        onPreview={onPreview}
                        onContextMenu={onTableContextMenu}
                      />
                    ))}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

function FileTree({ entries, currentDir, expandedDirs, onNavigate, onToggleDir, onOpenFile }: {
  entries: FileEntry[];
  currentDir: string;
  expandedDirs: Record<string, FileEntry[]>;
  onNavigate: (path: string) => void;
  onToggleDir: (path: string) => void;
  onOpenFile: (path: string, name: string) => void;
}) {
  function renderEntries(items: FileEntry[], depth: number) {
    return items.map((entry) => {
      const expanded = !!expandedDirs[entry.path];
      return (
        <div key={entry.path}>
          <button
            className="flex w-full items-center gap-2 py-1 text-left hover:bg-white/40"
            style={{ paddingLeft: `${depth * 16 + 8}px` }}
            onClick={() => entry.isDirectory ? onToggleDir(entry.path) : onOpenFile(entry.path, entry.name)}
            onDoubleClick={() => entry.isDirectory ? onNavigate(entry.path) : undefined}
            type="button"
          >
            {entry.isDirectory ? (
              <span className="w-4 shrink-0 text-center text-gray-500">{expanded ? '\u25BE' : '\u25B8'}</span>
            ) : (
              <span className="w-4 shrink-0" />
            )}
            <ExplorerIcon>{entry.isDirectory ? <FolderIcon /> : <FileIcon />}</ExplorerIcon>
            <span className="flex-1 truncate text-black">{entry.name}</span>
          </button>
          {expanded && expandedDirs[entry.path] ? renderEntries(expandedDirs[entry.path], depth + 1) : null}
        </div>
      );
    });
  }

  return (
    <div className="overflow-auto">
      <div className="mb-1 truncate px-2 text-[10px] text-black/40">{currentDir}</div>
      {entries.length === 0 ? (
        <EmptyInline message="Empty directory" />
      ) : (
        renderEntries(entries, 0)
      )}
    </div>
  );
}

const GIT_STATUS_LABEL: Record<string, string> = {
  M: 'Modified', A: 'Added', D: 'Deleted', R: 'Renamed', '??': 'Untracked', U: 'Unmerged',
  MM: 'Modified', AM: 'Added', AD: 'Added',
};
const GIT_STATUS_COLOR: Record<string, string> = {
  M: 'text-amber-600', A: 'text-green-600', D: 'text-red-500', '??': 'text-gray-400', U: 'text-purple-500',
  MM: 'text-amber-600', AM: 'text-green-600', AD: 'text-green-600',
};

function GitPanel({ gitStatus, gitRepoPath, gitRepos, onSwitchRepo, onOpenDiff, onOpenFile }: {
  gitStatus: GitStatus | null;
  gitRepoPath: string;
  gitRepos: GitRepo[];
  onSwitchRepo: (repoPath: string) => void;
  onOpenDiff: (filePath: string) => void;
  onOpenFile: (filePath: string, fileName: string) => void;
}) {
  const [repoFiles, setRepoFiles] = useState<Record<string, FileEntry[]>>({});
  const [expandedRepoDirs, setExpandedRepoDirs] = useState<Record<string, FileEntry[]>>({});
  const [openRepos, setOpenRepos] = useState<Set<string>>(new Set());

  async function toggleRepo(repoPath: string) {
    onSwitchRepo(repoPath);
    if (openRepos.has(repoPath)) {
      setOpenRepos((prev) => { const next = new Set(prev); next.delete(repoPath); return next; });
    } else {
      setOpenRepos((prev) => new Set(prev).add(repoPath));
      if (!repoFiles[repoPath]) {
        try {
          const entries = await api.listDirectory(repoPath);
          setRepoFiles((prev) => ({ ...prev, [repoPath]: entries }));
        } catch { /* ignore */ }
      }
    }
  }

  async function toggleRepoSubDir(dirPath: string) {
    if (expandedRepoDirs[dirPath]) {
      setExpandedRepoDirs((prev) => { const next = { ...prev }; delete next[dirPath]; return next; });
    } else {
      try {
        const entries = await api.listDirectory(dirPath);
        setExpandedRepoDirs((prev) => ({ ...prev, [dirPath]: entries }));
      } catch { /* ignore */ }
    }
  }

  function renderRepoEntries(items: FileEntry[], depth: number) {
    return items.map((entry) => {
      const expanded = !!expandedRepoDirs[entry.path];
      return (
        <div key={entry.path}>
          <button
            className="flex w-full items-center gap-2 py-0.5 text-left hover:bg-white/40"
            style={{ paddingLeft: `${depth * 16 + 24}px` }}
            onClick={() => entry.isDirectory ? toggleRepoSubDir(entry.path) : onOpenFile(entry.path, entry.name)}
            type="button"
          >
            {entry.isDirectory ? (
              <span className="w-4 shrink-0 text-center text-gray-500">{expanded ? '\u25BE' : '\u25B8'}</span>
            ) : (
              <span className="w-4 shrink-0" />
            )}
            <ExplorerIcon>{entry.isDirectory ? <FolderIcon /> : <FileIcon />}</ExplorerIcon>
            <span className="flex-1 truncate text-black">{entry.name}</span>
          </button>
          {expanded && expandedRepoDirs[entry.path] ? renderRepoEntries(expandedRepoDirs[entry.path], depth + 1) : null}
        </div>
      );
    });
  }

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="shrink-0 overflow-y-auto border-b border-black/5 pb-1" style={{ maxHeight: '33%' }}>
        {gitStatus ? (
          <>
            <div className="mb-1 flex items-center gap-2 px-1 pt-1">
              <span className="text-[11px] text-gray-500"><GitIcon /></span>
              <span className="text-[12px] font-medium text-black">{gitStatus.branch}</span>
              <span className="text-[10px] text-black/40">{gitRepoPath.split('/').pop()}</span>
            </div>
            {gitStatus.files.length > 0 ? (
              <div>
                <div className="px-1 pb-0.5 text-[10px] font-medium uppercase tracking-wider text-black/40">Changes ({gitStatus.files.length})</div>
                {gitStatus.files.map((file) => (
                  <div className="group flex items-center gap-2 px-1 py-0.5" key={file.path}>
                    <span className={`shrink-0 text-[10px] font-mono font-bold ${GIT_STATUS_COLOR[file.status] ?? 'text-gray-500'}`}>{file.status}</span>
                    <button className="min-w-0 flex-1 truncate text-left text-[12px] text-black" onClick={() => file.status === '??' ? onOpenFile(gitRepoPath + '/' + file.path, file.path.split('/').pop() ?? file.path) : onOpenDiff(file.path)} type="button">
                      {file.path}
                    </button>
                  </div>
                ))}
              </div>
            ) : (
              <div className="px-1 text-[12px] text-gray-400">Working tree clean</div>
            )}
            {gitStatus.commits.length > 0 ? (
              <div className="mt-1">
                <div className="px-1 pb-0.5 text-[10px] font-medium uppercase tracking-wider text-black/40">Recent Commits</div>
                {gitStatus.commits.slice(0, 5).map((commit) => (
                  <div className="flex items-center gap-2 px-1 py-0.5" key={commit.hash}>
                    <span className="shrink-0 font-mono text-[10px] text-[var(--accent)]">{commit.hash}</span>
                    <span className="min-w-0 flex-1 truncate text-[12px] text-black">{commit.message}</span>
                  </div>
                ))}
              </div>
            ) : null}
          </>
        ) : (
          <div className="px-1 pt-1 text-[12px] text-gray-400">{gitRepoPath ? 'Loading...' : 'Select a repository below'}</div>
        )}
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto pt-1">
        <div className="px-1 pb-1 text-[10px] font-medium uppercase tracking-wider text-black/40">Repositories</div>
        {gitRepos.map((repo) => {
          const isOpen = openRepos.has(repo.path);
          const isActive = repo.path === gitRepoPath;
          return (
            <div key={repo.path}>
              <button
                className="flex w-full items-center gap-2 px-1 py-0.5 text-left hover:bg-white/40"
                onClick={() => toggleRepo(repo.path)}
                type="button"
              >
                <span className="w-4 shrink-0 text-center text-gray-500">{isOpen ? '\u25BE' : '\u25B8'}</span>
                <ExplorerIcon><GitIcon /></ExplorerIcon>
                <span className={`flex-1 truncate text-[12px] ${isActive ? 'font-medium text-black' : 'text-black/60'}`}>{repo.name}</span>
              </button>
              {isOpen && repoFiles[repo.path] ? renderRepoEntries(repoFiles[repo.path], 1) : null}
            </div>
          );
        })}
        {gitRepos.length === 0 ? <EmptyInline message="No git repositories found" /> : null}
      </div>
    </div>
  );
}

function ResultsTable({
  result,
  sortState,
  onSort,
  rowOffset = 0,
}: {
  result: QueryResult;
  sortState: SortState;
  onSort: (columnIndex: number) => void;
  rowOffset?: number;
}) {
  const [colWidths, setColWidths] = useState<Record<number, number>>({});

  function startResize(e: React.PointerEvent, colIndex: number) {
    e.preventDefault();
    e.stopPropagation();
    const th = (e.target as HTMLElement).closest('th');
    const startX = e.clientX;
    const startWidth = colWidths[colIndex] ?? th?.offsetWidth ?? 150;
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    function onMove(ev: PointerEvent) {
      const newWidth = Math.max(60, startWidth + ev.clientX - startX);
      setColWidths((prev) => ({ ...prev, [colIndex]: newWidth }));
    }
    function onUp() {
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', onUp);
    }
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp);
  }

  const DEFAULT_COL_WIDTH = 150;
  const ROW_NUM_WIDTH = 48;
  const totalWidth = useMemo(
    () => ROW_NUM_WIDTH + result.columns.reduce((sum, _, i) => sum + (colWidths[i] ?? DEFAULT_COL_WIDTH), 0),
    [colWidths, result.columns],
  );

  return (
    <div className="bg-transparent">
      {result.notice ? <div className="border-b border-black/5 bg-blue-50/60 px-4 py-2 text-sm text-blue-700">{result.notice}</div> : null}
      <div>
        <table className="border-collapse font-sans text-[12px]" style={{ tableLayout: 'fixed', width: totalWidth }}>
          <colgroup>
            <col style={{ width: ROW_NUM_WIDTH }} />
            {result.columns.map((_, i) => (
              <col key={i} style={{ width: colWidths[i] ?? DEFAULT_COL_WIDTH }} />
            ))}
          </colgroup>
          <thead className="sticky top-0 z-[1] bg-white/60 backdrop-blur-sm text-left text-black">
            <tr>
              <th className="border-b border-r border-black/8 px-2 py-1 font-medium text-right">#</th>
              {result.columns.map((column, index) => (
                <th className="relative border-b border-r border-black/8 px-2 py-1 font-medium" key={column}>
                  <div className="flex items-center gap-1">
                    <button className="flex-1 truncate text-left" onClick={() => onSort(index)} type="button">
                      {column}
                    </button>
                    <span className="shrink-0 scale-75 text-gray-400"><FilterIcon /></span>
                    <button className="inline-flex shrink-0 flex-col items-center gap-0 leading-none text-[3.5px]" onClick={() => onSort(index)} type="button">
                      <span className={classNames('-mb-[3px]', sortState?.columnIndex === index && sortState.direction === 'asc' ? 'text-black' : 'text-white')} style={{ WebkitTextStroke: '0.3px #888' }}>&#9650;</span>
                      <span className={classNames('-mt-[3px]', sortState?.columnIndex === index && sortState.direction === 'desc' ? 'text-black' : 'text-white')} style={{ WebkitTextStroke: '0.3px #888' }}>&#9660;</span>
                    </button>
                  </div>
                  <div
                    className="absolute -right-2 top-0 z-[2] h-full w-4 cursor-col-resize"
                    style={{ borderRight: '2px solid transparent' }}
                    onPointerDown={(e) => startResize(e, index)}
                    onMouseEnter={(e) => { (e.currentTarget as HTMLElement).style.borderRightColor = 'var(--accent)'; }}
                    onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.borderRightColor = 'transparent'; }}
                  />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, rowIndex) => (
              <tr key={`${rowIndex}-${row.join('|')}`}>
                <td className="border-b border-r border-black/8 px-2 py-0.5 text-right text-gray-400">{rowOffset + rowIndex + 1}</td>
                {row.map((cell, cellIndex) => (
                  <td className="border-b border-r border-black/8 px-2 py-0.5 text-black" key={`${rowIndex}-${cellIndex}`}>
                    <div className="overflow-hidden text-ellipsis whitespace-nowrap">{cell}</div>
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function IconBase({ children }: { children: ReactNode }) { return <svg aria-hidden="true" className="h-3.5 w-3.5" fill="none" viewBox="0 0 16 16">{children}</svg>; }
function PlusIcon() { return <IconBase><path d="M8 3v10M3 8h10" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" /></IconBase>; }
function RefreshIcon() { return <IconBase><path d="M13 5V2.5H10.5" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" /><path d="M13 2.5A5.5 5.5 0 1 0 14 8" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" /></IconBase>; }
function PanelIcon() { return <IconBase><path d="M2.5 3.5h11v9h-11z" stroke="currentColor" strokeWidth="1.2" /><path d="M6 3.5v9" stroke="currentColor" strokeWidth="1.2" /></IconBase>; }
function ConnectionIcon({ active }: { active: boolean }) { return <IconBase><circle cx="8" cy="8" r="4.6" stroke="currentColor" strokeWidth="1.2" /><circle cx="8" cy="8" r="2" fill={active ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth="1.1" /></IconBase>; }
function PgpassIcon() { return <IconBase><rect x="3" y="5" width="10" height="7" rx="1" stroke="currentColor" strokeWidth="1.2" /><path d="M6 5V3.5a2 2 0 0 1 4 0V5" stroke="currentColor" strokeWidth="1.2" /><circle cx="8" cy="9" r="1" fill="currentColor" /></IconBase>; }
function DatabaseIcon() { return <IconBase><ellipse cx="8" cy="4" rx="4.5" ry="2" stroke="currentColor" strokeWidth="1.2" /><path d="M3.5 4v6c0 1.1 2 2 4.5 2s4.5-.9 4.5-2V4" stroke="currentColor" strokeWidth="1.2" /></IconBase>; }
function FolderIcon() { return <IconBase><path d="M2.5 5h4l1.2-1.5h5.8v8H2.5z" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round" /></IconBase>; }
function FileIcon() { return <IconBase><path d="M4 2.5h5l3 3v8H4z" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round" /><path d="M9 2.5v3h3" stroke="currentColor" strokeWidth="1.2" /></IconBase>; }
function ChevronUpIcon() { return <IconBase><path d="M4 10l4-4 4 4" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" /></IconBase>; }
function GitIcon() { return <IconBase><circle cx="5" cy="4.5" r="1.5" stroke="currentColor" strokeWidth="1.2" /><circle cx="11" cy="4.5" r="1.5" stroke="currentColor" strokeWidth="1.2" /><circle cx="5" cy="11.5" r="1.5" stroke="currentColor" strokeWidth="1.2" /><path d="M5 6v4M6.5 4.5h3" stroke="currentColor" strokeWidth="1.2" /></IconBase>; }
function TableIcon() { return <IconBase><rect x="2.5" y="3" width="11" height="10" stroke="currentColor" strokeWidth="1.2" /><path d="M2.5 6.5h11M2.5 10h11M6 3v10M9.8 3v10" stroke="currentColor" strokeWidth="1" /></IconBase>; }
function QueryIcon() { return <IconBase><path d="M4 4.5h8M4 8h8M4 11.5h5" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" /></IconBase>; }
function ExportIcon() { return <IconBase><path d="M8 2.5v7M5.5 7l2.5 2.5L10.5 7" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" /><path d="M3 12.5h10" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" /></IconBase>; }
function FilterIcon() { return <IconBase><path d="M2.5 3.5h11L9 8.5v4l-2 1.5v-5.5z" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round" /></IconBase>; }
function DdlIcon() { return <IconBase><path d="M4 3h5.5L12 5.5V13H4z" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round" /><path d="M9.5 3v2.5H12" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round" /><path d="M6 8h4M6 10h3" stroke="currentColor" strokeWidth="1.1" strokeLinecap="round" /></IconBase>; }
function ModifyIcon() { return <IconBase><path d="M11.5 2.5l2 2-7 7H4.5v-2z" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round" /><path d="M3 13.5h10" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" /></IconBase>; }
function EditDataIcon() { return <IconBase><rect x="2.5" y="3" width="11" height="10" stroke="currentColor" strokeWidth="1.2" /><path d="M2.5 6.5h11M6 3v10" stroke="currentColor" strokeWidth="1" /><path d="M9.5 8.5l1.5 1.5-1.5 1.5" stroke="currentColor" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round" /></IconBase>; }
function TruncateIcon() { return <IconBase><path d="M3.5 5h9M5 5V4h6v1M5.5 5v7.5h5V5" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" /><path d="M7 7.5v3M9 7.5v3" stroke="currentColor" strokeWidth="1" strokeLinecap="round" /></IconBase>; }
function DropIcon() { return <IconBase><path d="M3.5 5h9M5 5V4h6v1M5.5 5v7.5h5V5" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" strokeLinejoin="round" /><path d="M6 8l4 4M10 8l-4 4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" /></IconBase>; }
function WarningIcon() { return <IconBase><path d="M8 2L1.5 13h13L8 2z" stroke="currentColor" strokeWidth="1.3" strokeLinejoin="round" fill="none" /><path d="M8 6v3.5" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" /><circle cx="8" cy="11" r="0.7" fill="currentColor" /></IconBase>; }

function EditDataView({
  editData,
  onCellChange,
  onToggleDeleteRow,
  onAddRow,
  onRemoveNewRow,
  onApply,
  onDiscard,
  onSetEditingCell,
  onPageChange,
}: {
  editData: EditDataState;
  onCellChange: (rowIdx: number, colIdx: number, value: string | null, isNew: boolean) => void;
  onToggleDeleteRow: (rowIdx: number) => void;
  onAddRow: () => void;
  onRemoveNewRow: (idx: number) => void;
  onApply: () => void;
  onDiscard: () => void;
  onSetEditingCell: (cell: { row: number; col: number; isNew: boolean } | null) => void;
  onPageChange: (page: number) => void;
}) {
  const { tableData, editedCells, deletedRows, newRows, editingCell, page, pageSize } = editData;
  const { columns, columnTypes, rows, primaryKeyColumns, totalCount } = tableData;
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const hasChanges = editedCells.size > 0 || deletedRows.size > 0 || newRows.length > 0;
  const changeCount = editedCells.size + deletedRows.size + newRows.filter((r) => r.some((v) => v !== null)).length;

  return (
    <>
      <div className="flex shrink-0 items-center gap-2 border-b border-black/5 bg-white/30 px-3 py-1.5">
        <span className="text-[11px] font-medium uppercase tracking-[0.08em] text-gray-500">
          PK: {primaryKeyColumns.join(', ')}
        </span>
        <span className="text-[11px] text-gray-400">|</span>
        <span className="text-[11px] text-gray-500">{totalCount.toLocaleString()} rows total</span>
        <div className="flex-1" />
        {hasChanges ? (
          <span className="rounded bg-amber-100 px-2 py-0.5 text-[11px] font-medium text-amber-700">
            {changeCount} pending {changeCount === 1 ? 'change' : 'changes'}
          </span>
        ) : null}
        <button
          className="rounded-lg border border-black/10 px-2 py-1 text-[11px] text-gray-500 hover:text-black disabled:opacity-30"
          disabled={!hasChanges}
          onClick={onDiscard}
          type="button"
        >
          Discard
        </button>
        <button
          className="rounded-lg bg-[var(--accent)] px-2 py-1 text-[11px] text-white hover:opacity-90 disabled:opacity-30"
          disabled={!hasChanges}
          onClick={onApply}
          type="button"
        >
          Apply
        </button>
        <button
          className="rounded-lg border border-dashed border-black/10 px-2 py-1 text-[11px] text-gray-500 hover:border-black/20 hover:text-black"
          onClick={onAddRow}
          type="button"
        >
          + Row
        </button>
      </div>
      <div className="min-h-0 flex-1 overflow-scroll">
        <div className="bg-transparent">
          <table className="min-w-full border-collapse font-sans text-[12px]">
            <thead className="sticky top-0 z-[1] bg-white/60 backdrop-blur-sm text-left text-black">
              <tr>
                <th className="w-10 border-b border-r border-black/8 px-2 py-1 font-medium text-center text-[10px] text-gray-400">#</th>
                {columns.map((col, ci) => (
                  <th className="border-b border-r border-black/8 px-2 py-1 font-medium" key={col}>
                    <div className="flex items-center gap-1">
                      <span>{col}</span>
                      {primaryKeyColumns.includes(col) && <span className="text-[9px] text-amber-500">PK</span>}
                      <span className="ml-auto text-[10px] font-normal text-gray-400">{columnTypes[ci]}</span>
                    </div>
                  </th>
                ))}
                <th className="w-12 border-b border-black/8 px-1 py-1 font-medium text-center text-[10px] text-gray-400">
                </th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, rowIdx) => {
                const isDeleted = deletedRows.has(rowIdx);
                return (
                  <tr key={rowIdx} className={isDeleted ? 'bg-red-50/60' : ''}>
                    <td className="border-b border-r border-black/8 px-2 py-0.5 text-center text-gray-400 text-[11px]">
                      {page * pageSize + rowIdx + 1}
                    </td>
                    {row.map((cell, colIdx) => {
                      const key = `${rowIdx}:${colIdx}`;
                      const isEdited = editedCells.has(key);
                      const displayValue = isEdited ? editedCells.get(key) : cell;
                      const isEditing = editingCell && !editingCell.isNew && editingCell.row === rowIdx && editingCell.col === colIdx;
                      const isPk = primaryKeyColumns.includes(columns[colIdx]);

                      return (
                        <td
                          key={colIdx}
                          className={classNames(
                            'max-w-[300px] border-b border-r border-black/8 px-0 py-0',
                            isDeleted && 'line-through opacity-40',
                            isEdited && !isDeleted && 'bg-amber-50',
                          )}
                          onDoubleClick={() => {
                            if (!isDeleted && !isPk) onSetEditingCell({ row: rowIdx, col: colIdx, isNew: false });
                          }}
                        >
                          {isEditing ? (
                            <input
                              autoFocus
                              className="w-full border-0 bg-blue-50 px-2 py-0.5 text-[12px] text-black outline-none"
                              defaultValue={displayValue ?? ''}
                              onBlur={(e) => {
                                const val = e.target.value;
                                onCellChange(rowIdx, colIdx, val === '' && cell === null ? null : val, false);
                                onSetEditingCell(null);
                              }}
                              onKeyDown={(e) => {
                                if (e.key === 'Enter') { e.currentTarget.blur(); }
                                if (e.key === 'Escape') { onSetEditingCell(null); }
                                if (e.key === 'Tab') {
                                  e.preventDefault();
                                  e.currentTarget.blur();
                                  const nextCol = e.shiftKey ? colIdx - 1 : colIdx + 1;
                                  if (nextCol >= 0 && nextCol < columns.length && !primaryKeyColumns.includes(columns[nextCol])) {
                                    onSetEditingCell({ row: rowIdx, col: nextCol, isNew: false });
                                  }
                                }
                              }}
                            />
                          ) : (
                            <div className={classNames(
                              'overflow-hidden text-ellipsis whitespace-nowrap px-2 py-0.5',
                              displayValue === null ? 'italic text-gray-400' : 'text-black',
                              !isDeleted && !isPk && 'cursor-text',
                            )}>
                              {displayValue === null ? 'NULL' : displayValue}
                            </div>
                          )}
                        </td>
                      );
                    })}
                    <td className="border-b border-black/8 px-1 py-0.5 text-center">
                      <button
                        className={classNames(
                          'text-[10px]',
                          isDeleted ? 'text-blue-500 hover:text-blue-700' : 'text-red-400 hover:text-red-600',
                        )}
                        onClick={() => onToggleDeleteRow(rowIdx)}
                        title={isDeleted ? 'Undo delete' : 'Delete row'}
                        type="button"
                      >
                        {isDeleted ? 'undo' : 'del'}
                      </button>
                    </td>
                  </tr>
                );
              })}
              {newRows.map((newRow, nri) => (
                <tr key={`new-${nri}`} className="bg-emerald-50/40">
                  <td className="border-b border-r border-black/8 px-2 py-0.5 text-center text-[11px] text-emerald-500">+</td>
                  {newRow.map((cell, colIdx) => {
                    const isEditing = editingCell && editingCell.isNew && editingCell.row === nri && editingCell.col === colIdx;
                    return (
                      <td
                        key={colIdx}
                        className="max-w-[300px] border-b border-r border-black/8 px-0 py-0"
                        onDoubleClick={() => onSetEditingCell({ row: nri, col: colIdx, isNew: true })}
                      >
                        {isEditing ? (
                          <input
                            autoFocus
                            className="w-full border-0 bg-blue-50 px-2 py-0.5 text-[12px] text-black outline-none"
                            defaultValue={cell ?? ''}
                            onBlur={(e) => {
                              const val = e.target.value;
                              onCellChange(nri, colIdx, val === '' ? null : val, true);
                              onSetEditingCell(null);
                            }}
                            onKeyDown={(e) => {
                              if (e.key === 'Enter') { e.currentTarget.blur(); }
                              if (e.key === 'Escape') { onSetEditingCell(null); }
                              if (e.key === 'Tab') {
                                e.preventDefault();
                                e.currentTarget.blur();
                                const nextCol = e.shiftKey ? colIdx - 1 : colIdx + 1;
                                if (nextCol >= 0 && nextCol < columns.length) {
                                  onSetEditingCell({ row: nri, col: nextCol, isNew: true });
                                }
                              }
                            }}
                          />
                        ) : (
                          <div
                            className={classNames(
                              'overflow-hidden text-ellipsis whitespace-nowrap px-2 py-0.5 cursor-text',
                              cell === null ? 'italic text-gray-400' : 'text-black',
                            )}
                          >
                            {cell === null ? 'NULL' : cell}
                          </div>
                        )}
                      </td>
                    );
                  })}
                  <td className="border-b border-black/8 px-1 py-0.5 text-center">
                    <button
                      className="text-[10px] text-red-400 hover:text-red-600"
                      onClick={() => onRemoveNewRow(nri)}
                      title="Remove new row"
                      type="button"
                    >
                      del
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      <div className="flex shrink-0 items-center justify-center gap-2 border-t border-black/5 bg-white/30 px-3 py-1.5 text-[14px] text-gray-600">
        <button className="px-1 py-0.5 text-[16px] font-extrabold hover:text-black disabled:opacity-30" disabled={page === 0} onClick={() => onPageChange(0)} type="button">{'<<'}</button>
        <button className="px-1 py-0.5 text-[16px] font-extrabold hover:text-black disabled:opacity-30" disabled={page === 0} onClick={() => onPageChange(page - 1)} type="button">{'<'}</button>
        <span>{rows.length > 0 ? `${(page * pageSize + 1).toLocaleString()}-${(page * pageSize + rows.length).toLocaleString()}` : '0'} of {totalCount.toLocaleString()}</span>
        <button className="px-1 py-0.5 text-[16px] font-extrabold hover:text-black disabled:opacity-30" disabled={page >= totalPages - 1} onClick={() => onPageChange(page + 1)} type="button">{'>'}</button>
        <button className="px-1 py-0.5 text-[16px] font-extrabold hover:text-black disabled:opacity-30" disabled={page >= totalPages - 1} onClick={() => onPageChange(totalPages - 1)} type="button">{'>>'}</button>
      </div>
    </>
  );
}

function summarizeSql(sql: string) {
  const line = sql.trim().replace(/\s+/g, ' ').slice(0, 56);
  return line || 'Untitled query';
}
