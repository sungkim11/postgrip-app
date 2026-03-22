import { describe, it, expect, beforeAll } from 'vitest';
import dotenv from 'dotenv';
import pg from 'pg';
import type { SavedConnection } from '../../src/main/types';
import * as postgres from '../../src/main/postgres';

/**
 * Live integration tests that connect to a real PostgreSQL database.
 * Requires a valid .env file with DB_HOST, DB_PORT, DB_NAME, DB_USER.
 * Password is resolved from ~/.pgpass (or DB_PASSWORD env var if set).
 *
 * Run with: bun run test:live
 */

dotenv.config();

const conn: SavedConnection = {
  id: 'live-test',
  name: 'Live Test',
  host: process.env.DB_HOST ?? 'localhost',
  port: Number(process.env.DB_PORT ?? 5432),
  user: process.env.DB_USER ?? 'postgres',
  password: process.env.DB_PASSWORD ?? '',
  database: process.env.DB_NAME ?? 'postgres',
};

const hasEnv = !!(process.env.DB_HOST && process.env.DB_USER);

describe.skipIf(!hasEnv)('live database tests', () => {
  beforeAll(async () => {
    // Verify connectivity
    await postgres.testConnection(conn);
  });

  describe('testConnection', () => {
    it('connects successfully', async () => {
      await expect(postgres.testConnection(conn)).resolves.toBeUndefined();
    });

    it('rejects invalid credentials', async () => {
      const bad = { ...conn, password: 'wrong-password-xyz' };
      await expect(postgres.testConnection(bad)).rejects.toThrow();
    });
  });

  describe('fetchTree', () => {
    it('returns a non-empty schema tree', async () => {
      const tree = await postgres.fetchTree(conn);
      expect(tree.length).toBeGreaterThan(0);

      const publicSchema = tree.find((s) => s.name === 'public');
      if (publicSchema) {
        expect(publicSchema.tables.length).toBeGreaterThanOrEqual(0);
        for (const table of publicSchema.tables) {
          expect(table.name).toBeTruthy();
          expect(table.columns.length).toBeGreaterThan(0);
          for (const col of table.columns) {
            expect(col.name).toBeTruthy();
            expect(col.dataType).toBeTruthy();
            expect(typeof col.nullable).toBe('boolean');
          }
        }
      }
    });

    it('includes column, key, and index metadata', async () => {
      const tree = await postgres.fetchTree(conn);
      const allTables = tree.flatMap((s) => s.tables);
      expect(allTables.length).toBeGreaterThan(0);

      // Every table should have at least the columns array
      for (const table of allTables) {
        expect(Array.isArray(table.columns)).toBe(true);
        expect(Array.isArray(table.keys)).toBe(true);
        expect(Array.isArray(table.indexes)).toBe(true);
      }
    });
  });

  describe('runQuery', () => {
    it('executes a simple SELECT', async () => {
      const result = await postgres.runQuery(conn, 'SELECT 1 AS val', 500);
      expect(result.columns).toEqual(['val']);
      expect(result.rows).toEqual([['1']]);
      expect(result.rowCount).toBe(1);
      expect(result.truncated).toBe(false);
      expect(result.executionTimeMs).toBeGreaterThanOrEqual(0);
    });

    it('handles empty result', async () => {
      const result = await postgres.runQuery(conn, 'SELECT 1 WHERE false', 500);
      expect(result.rows).toEqual([]);
      expect(result.rowCount).toBe(0);
    });

    it('respects limit and reports truncation', async () => {
      const result = await postgres.runQuery(conn, 'SELECT generate_series(1, 100) AS n', 10);
      expect(result.rows.length).toBe(10);
      expect(result.truncated).toBe(true);
      expect(result.notice).toContain('first 10 rows');
    });

    it('handles NULL values', async () => {
      const result = await postgres.runQuery(conn, 'SELECT NULL AS empty', 500);
      expect(result.rows[0][0]).toBe('NULL');
    });
  });

  describe('getHostStats', () => {
    it('returns stats with at least dbSize and connections', async () => {
      const stats = await postgres.getHostStats(conn);
      expect(stats).toBeDefined();
      expect(stats.dbSizeMb).toBeGreaterThan(0);
      expect(stats.activeConnections).toBeGreaterThan(0);
      expect(stats.maxConnections).toBeGreaterThan(0);
      expect(stats.connectionSaturationPercent).toBeGreaterThanOrEqual(0);
    });
  });

  describe('getActiveQueries', () => {
    it('returns an array of active queries', async () => {
      const queries = await postgres.getActiveQueries(conn);
      expect(Array.isArray(queries)).toBe(true);
      for (const q of queries) {
        expect(q.pid).toBeGreaterThan(0);
        expect(typeof q.query).toBe('string');
        expect(typeof q.state).toBe('string');
      }
    });
  });

  describe('table DDL and modify info', () => {
    let testTableExists = false;

    beforeAll(async () => {
      const tree = await postgres.fetchTree(conn);
      const publicSchema = tree.find((s) => s.name === 'public');
      testTableExists = publicSchema?.tables.some((t) => t.name != null) ?? false;
    });

    it('getTableDdl returns valid DDL for first table', async () => {
      if (!testTableExists) return;
      const tree = await postgres.fetchTree(conn);
      const firstTable = tree.flatMap((s) => s.tables.map((t) => ({ schema: s.name, table: t.name })))[0];
      if (!firstTable) return;

      const ddl = await postgres.getTableDdl(conn, firstTable.schema, firstTable.table);
      expect(ddl).toContain('create table');
      expect(ddl).toContain(firstTable.table);
    });

    it('getModifyTableInfo returns column info for first table', async () => {
      if (!testTableExists) return;
      const tree = await postgres.fetchTree(conn);
      const firstTable = tree.flatMap((s) => s.tables.map((t) => ({ schema: s.name, table: t.name })))[0];
      if (!firstTable) return;

      const info = await postgres.getModifyTableInfo(conn, firstTable.schema, firstTable.table);
      expect(info.schema).toBe(firstTable.schema);
      expect(info.table).toBe(firstTable.table);
      expect(info.columns.length).toBeGreaterThan(0);
      for (const col of info.columns) {
        expect(col.name).toBeTruthy();
        expect(col.dataType).toBeTruthy();
        expect(typeof col.nullable).toBe('boolean');
      }
    });
  });

  describe('CRUD lifecycle on temp table', () => {
    const tempTable = '_rdb3_test_' + Date.now();

    beforeAll(async () => {
      // Create a temporary test table
      const client = new pg.Client({
        host: conn.host,
        port: conn.port,
        user: conn.user,
        password: conn.password,
        database: conn.database,
      });
      await client.connect();
      await client.query(`CREATE TABLE public."${tempTable}" (id serial PRIMARY KEY, name text, value int)`);
      await client.end();
    });

    it('previewTable on empty table', async () => {
      const result = await postgres.previewTable(conn, 'public', tempTable, 200, 0);
      expect(result.columns).toContain('id');
      expect(result.columns).toContain('name');
      expect(result.columns).toContain('value');
      expect(result.rowCount).toBe(0);
    });

    it('getPrimaryKeyColumns returns PK', async () => {
      const pk = await postgres.getPrimaryKeyColumns(conn, 'public', tempTable);
      expect(pk).toEqual(['id']);
    });

    it('executeDml inserts rows', async () => {
      await postgres.executeDml(conn, 'public', tempTable, [
        { type: 'insert', values: { name: 'Alice', value: '10' } },
        { type: 'insert', values: { name: 'Bob', value: '20' } },
        { type: 'insert', values: { name: 'Charlie', value: '30' } },
      ]);

      const result = await postgres.previewTable(conn, 'public', tempTable, 200, 0);
      expect(result.rowCount).toBe(3);
    });

    it('getEditableTableData returns data with PK info', async () => {
      const data = await postgres.getEditableTableData(conn, 'public', tempTable, 200, 0);
      expect(data.columns).toContain('id');
      expect(data.columns).toContain('name');
      expect(data.primaryKeyColumns).toEqual(['id']);
      expect(data.totalCount).toBe(3);
      expect(data.rows.length).toBe(3);
      expect(data.columnTypes.length).toBe(data.columns.length);
    });

    it('executeDml updates a row', async () => {
      const data = await postgres.getEditableTableData(conn, 'public', tempTable, 200, 0);
      const firstId = data.rows[0][data.columns.indexOf('id')];

      await postgres.executeDml(conn, 'public', tempTable, [
        { type: 'update', pkValues: { id: firstId }, changes: { name: 'Alice Updated' } },
      ]);

      const after = await postgres.previewTable(conn, 'public', tempTable, 200, 0);
      const nameCol = after.columns.indexOf('name');
      const updatedRow = after.rows.find((r) => r[nameCol] === 'Alice Updated');
      expect(updatedRow).toBeDefined();
    });

    it('executeDml deletes a row', async () => {
      const data = await postgres.getEditableTableData(conn, 'public', tempTable, 200, 0);
      const lastId = data.rows[data.rows.length - 1][data.columns.indexOf('id')];

      await postgres.executeDml(conn, 'public', tempTable, [
        { type: 'delete', pkValues: { id: lastId } },
      ]);

      const after = await postgres.getEditableTableData(conn, 'public', tempTable, 200, 0);
      expect(after.totalCount).toBe(2);
    });

    it('alterTable adds a column', async () => {
      await postgres.alterTable(conn, 'public', tempTable, [
        { type: 'add_column', columnName: 'email', dataType: 'text', nullable: true },
      ]);

      const info = await postgres.getModifyTableInfo(conn, 'public', tempTable);
      expect(info.columns.some((c) => c.name === 'email')).toBe(true);
    });

    it('alterTable renames a column', async () => {
      await postgres.alterTable(conn, 'public', tempTable, [
        { type: 'rename_column', columnName: 'email', newColumnName: 'contact_email' },
      ]);

      const info = await postgres.getModifyTableInfo(conn, 'public', tempTable);
      expect(info.columns.some((c) => c.name === 'contact_email')).toBe(true);
      expect(info.columns.some((c) => c.name === 'email')).toBe(false);
    });

    it('truncateTable removes all data but keeps structure', async () => {
      await postgres.truncateTable(conn, 'public', tempTable, false);

      const result = await postgres.previewTable(conn, 'public', tempTable, 200, 0);
      expect(result.rowCount).toBe(0);

      const info = await postgres.getModifyTableInfo(conn, 'public', tempTable);
      expect(info.columns.length).toBeGreaterThan(0);
    });

    it('dropTable removes the table', async () => {
      await postgres.dropTable(conn, 'public', tempTable, false);

      const tree = await postgres.fetchTree(conn);
      const publicSchema = tree.find((s) => s.name === 'public');
      const exists = publicSchema?.tables.some((t) => t.name === tempTable) ?? false;
      expect(exists).toBe(false);
    });
  });
});
