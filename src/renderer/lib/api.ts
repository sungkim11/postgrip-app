import type { ActiveQuery, AlterTableAction, AppInfo, AppSnapshot, BackupEntry, BackupOptions, BackupSchedule, ConnectionInput, DdlResult, DmlOperation, EditableTableData, FileEntry, GitRepo, GitStatus, HostStats, ModifyTableInfo, MonitoringData, PgpassEntry, QueryResult } from './types';

interface ElectronAPI {
  bootstrap(): Promise<AppSnapshot>;
  hostStats(): Promise<HostStats>;
  monitoringData(): Promise<MonitoringData>;
  activeQueries(): Promise<ActiveQuery[]>;
  testConnection(connection: ConnectionInput): Promise<{ success: boolean }>;
  connect(connection: ConnectionInput, save: boolean): Promise<AppSnapshot>;
  activateSavedConnection(id: string): Promise<AppSnapshot>;
  deleteSavedConnection(id: string): Promise<AppSnapshot>;
  disconnect(): Promise<AppSnapshot>;
  runQuery(sql: string, limit?: number): Promise<QueryResult>;
  previewTable(schema: string, table: string, limit?: number, offset?: number): Promise<QueryResult>;
  getTableDdl(schema: string, table: string): Promise<DdlResult>;
  dropTable(schema: string, table: string, cascade: boolean): Promise<AppSnapshot>;
  truncateTable(schema: string, table: string, cascade: boolean): Promise<AppSnapshot>;
  getEditableTableData(schema: string, table: string, limit: number, offset: number): Promise<EditableTableData>;
  executeDml(schema: string, table: string, operations: DmlOperation[]): Promise<void>;
  getModifyTableInfo(schema: string, table: string): Promise<ModifyTableInfo>;
  alterTable(schema: string, table: string, actions: AlterTableAction[]): Promise<AppSnapshot>;
  exportTableCsv(schema: string, table: string, path: string): Promise<number>;
  createSchema(schemaName: string): Promise<AppSnapshot>;
  createTable(schema: string, tableName: string, columns: Array<{ name: string; type: string; nullable: boolean; defaultValue?: string }>): Promise<AppSnapshot>;
  exportPgDump(schema: string, table: string, filePath: string, format: string): Promise<void>;
  showSaveDialog(options: { defaultPath?: string; filters?: Array<{ name: string; extensions: string[] }> }): Promise<string | null>;
  writeFile(filePath: string, content: string): Promise<void>;
  getPgpassEntries(): Promise<PgpassEntry[]>;
  listDirectory(dirPath: string): Promise<FileEntry[]>;
  readTextFile(filePath: string): Promise<string>;
  getHomeDir(): Promise<string>;
  getAppInfo(): Promise<AppInfo>;
  readHelp(): Promise<string>;
  findGitRepos(dirPath: string): Promise<GitRepo[]>;
  gitRepoRoot(dirPath: string): Promise<string | null>;
  gitStatus(repoPath: string): Promise<GitStatus | null>;
  gitDiff(repoPath: string, filePath: string): Promise<string | null>;
  listBackups(dirPath: string): Promise<BackupEntry[]>;
  getBackupDir(): Promise<string>;
  setBackupDir(dirPath: string): Promise<void>;
  listBackupSchedules(): Promise<BackupSchedule[]>;
  addBackupSchedule(schedule: Omit<BackupSchedule, 'id' | 'createdAt'>): Promise<BackupSchedule>;
  updateBackupSchedule(id: string, updates: Partial<BackupSchedule>): Promise<BackupSchedule[]>;
  deleteBackupSchedule(id: string): Promise<BackupSchedule[]>;
  deleteBackup(filePath: string): Promise<void>;
  backupDatabase(options: BackupOptions): Promise<{ durationMs: number }>;
  restoreDatabase(filePath: string): Promise<void>;
  showOpenDialog(options: { filters?: Array<{ name: string; extensions: string[] }>; properties?: string[] }): Promise<string | null>;
  closeWindow(): Promise<void>;
}

declare global {
  interface Window {
    electronAPI: ElectronAPI;
  }
}

export const api: ElectronAPI = window.electronAPI;
