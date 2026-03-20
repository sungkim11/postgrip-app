import { contextBridge, ipcRenderer } from 'electron';

contextBridge.exposeInMainWorld('electronAPI', {
  bootstrap: () => ipcRenderer.invoke('bootstrap'),
  connect: (connection: unknown, save: boolean) =>
    ipcRenderer.invoke('connect', connection, save),
  activateSavedConnection: (id: string) =>
    ipcRenderer.invoke('activate-saved-connection', id),
  deleteSavedConnection: (id: string) =>
    ipcRenderer.invoke('delete-saved-connection', id),
  disconnect: () => ipcRenderer.invoke('disconnect'),
  runQuery: (sql: string, limit?: number) =>
    ipcRenderer.invoke('run-query', sql, limit),
  previewTable: (schema: string, table: string, limit?: number, offset?: number) =>
    ipcRenderer.invoke('preview-table', schema, table, limit, offset),
  getTableDdl: (schema: string, table: string) =>
    ipcRenderer.invoke('get-table-ddl', schema, table),
  exportParquet: (schema: string, table: string, path: string) =>
    ipcRenderer.invoke('export-parquet', schema, table, path),
  showSaveDialog: (options: unknown) =>
    ipcRenderer.invoke('show-save-dialog', options),
  writeFile: (filePath: string, content: string) =>
    ipcRenderer.invoke('write-file', filePath, content),
  closeWindow: () => ipcRenderer.invoke('close-window'),
});
