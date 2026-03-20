import { app } from 'electron';
import fs from 'node:fs';
import path from 'node:path';
import type { SavedConnection } from './types';

const CONNECTIONS_FILE = 'connections.json';

function connectionsPath(): string {
  const dir = app.getPath('userData');
  return path.join(dir, CONNECTIONS_FILE);
}

export function loadConnections(): SavedConnection[] {
  const filePath = connectionsPath();
  if (!fs.existsSync(filePath)) return [];
  const raw = fs.readFileSync(filePath, 'utf-8');
  return JSON.parse(raw) as SavedConnection[];
}

export function saveConnections(connections: SavedConnection[]): void {
  const filePath = connectionsPath();
  fs.writeFileSync(filePath, JSON.stringify(connections, null, 2), 'utf-8');
}
