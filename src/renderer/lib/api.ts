import type { AppSnapshot, ConnectionInput, DdlResult, QueryResult } from './types';

interface ElectronAPI {
  bootstrap(): Promise<AppSnapshot>;
  connect(connection: ConnectionInput, save: boolean): Promise<AppSnapshot>;
  activateSavedConnection(id: string): Promise<AppSnapshot>;
  deleteSavedConnection(id: string): Promise<AppSnapshot>;
  disconnect(): Promise<AppSnapshot>;
  runQuery(sql: string, limit?: number): Promise<QueryResult>;
  previewTable(schema: string, table: string, limit?: number, offset?: number): Promise<QueryResult>;
  getTableDdl(schema: string, table: string): Promise<DdlResult>;
  exportParquet(schema: string, table: string, path: string): Promise<number>;
  showSaveDialog(options: { defaultPath?: string; filters?: Array<{ name: string; extensions: string[] }> }): Promise<string | null>;
  writeFile(filePath: string, content: string): Promise<void>;
  closeWindow(): Promise<void>;
}

declare global {
  interface Window {
    electronAPI: ElectronAPI;
  }
}

export const api: ElectronAPI = window.electronAPI;
