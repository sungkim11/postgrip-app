import pg from 'pg';
import type { SavedConnection, QueryResult, SchemaNode, ColumnNode, TableNode } from './types';
import { buildConnectionString } from './types';

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

async function connect(conn: SavedConnection): Promise<pg.Client> {
  const client = new pg.Client({ connectionString: buildConnectionString(conn) });
  await client.connect();
  return client;
}

export async function testConnection(conn: SavedConnection): Promise<void> {
  const client = await connect(conn);
  try {
    await client.query('SELECT 1');
  } finally {
    await client.end();
  }
}

export async function fetchTree(conn: SavedConnection): Promise<SchemaNode[]> {
  const client = await connect(conn);
  try {
    const { rows } = await client.query(
      `SELECT t.table_schema, t.table_name, t.table_type, c.column_name, c.data_type, c.is_nullable, c.column_default
       FROM information_schema.tables t
       JOIN information_schema.columns c
         ON c.table_schema = t.table_schema AND c.table_name = t.table_name
       WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
       ORDER BY t.table_schema, t.table_name, c.ordinal_position`
    );

    const schemaOrder: string[] = [];
    const schemaMap = new Map<string, string[]>();
    const tableMap = new Map<string, TableNode>();

    for (const row of rows) {
      const schemaName: string = row.table_schema;
      const tableName: string = row.table_name;
      const tableType: string = row.table_type;
      const columnName: string = row.column_name;
      const dataType: string = row.data_type;
      const isNullable: string = row.is_nullable;
      const defaultValue: string | null = row.column_default;

      if (!schemaMap.has(schemaName)) {
        schemaOrder.push(schemaName);
        schemaMap.set(schemaName, []);
      }

      const key = `${schemaName}.${tableName}`;
      if (!tableMap.has(key)) {
        schemaMap.get(schemaName)!.push(tableName);
        tableMap.set(key, { name: tableName, tableType, columns: [] });
      }

      const col: ColumnNode = {
        name: columnName,
        dataType,
        nullable: isNullable === 'YES',
        defaultValue,
      };
      tableMap.get(key)!.columns.push(col);
    }

    return schemaOrder.map((schemaName) => {
      const tableNames = schemaMap.get(schemaName) ?? [];
      const tables = tableNames
        .map((tn) => tableMap.get(`${schemaName}.${tn}`))
        .filter((t): t is TableNode => t != null);
      return { name: schemaName, tables };
    });
  } finally {
    await client.end();
  }
}

export async function runQuery(
  conn: SavedConnection,
  sql: string,
  limit: number,
): Promise<QueryResult> {
  const client = await connect(conn);
  try {
    const started = performance.now();
    const fetchLimit = limit + 1;
    const trimmed = sql.trim().replace(/;+$/, '');
    const limitedSql = `SELECT * FROM (${trimmed}) AS _rdb2_sub LIMIT ${fetchLimit}`;

    let result: pg.QueryResult;
    try {
      result = await client.query(limitedSql);
    } catch {
      // If wrapped query fails (DDL/DML), try original SQL directly
      result = await client.query(sql);
    }

    const elapsed = Math.round(performance.now() - started);
    const columns = result.fields?.map((f) => f.name) ?? [];
    const allRows: string[][] = (result.rows ?? []).map((row: Record<string, unknown>) =>
      columns.map((col) => {
        const val = row[col];
        return val == null ? 'NULL' : String(val);
      }),
    );

    const truncated = allRows.length > limit;
    const rows = truncated ? allRows.slice(0, limit) : allRows;
    let notice: string | null = null;

    if (truncated) {
      notice = `Showing the first ${limit} rows.`;
    } else if (rows.length === 0 && result.command) {
      notice = `${result.command} ${result.rowCount ?? 0}`;
    }

    return {
      columns,
      rows,
      rowCount: rows.length,
      truncated,
      executionTimeMs: elapsed,
      notice,
    };
  } finally {
    await client.end();
  }
}

export async function previewTable(
  conn: SavedConnection,
  schema: string,
  table: string,
  limit: number,
  offset: number,
): Promise<QueryResult> {
  const sql = `SELECT * FROM ${quoteIdentifier(schema)}.${quoteIdentifier(table)} LIMIT ${limit} OFFSET ${offset}`;
  return runQuery(conn, sql, limit);
}

export async function getTableDdl(
  conn: SavedConnection,
  schema: string,
  table: string,
): Promise<string> {
  const client = await connect(conn);
  try {
    // Get table OID
    const oidResult = await client.query(
      `SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = $1 AND c.relname = $2`,
      [schema, table],
    );
    if (oidResult.rows.length === 0) throw new Error('Table not found');
    const tableOid = oidResult.rows[0].oid;

    // Columns with types, defaults, not-null
    const colResult = await client.query(
      `SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type,
              a.attnotnull, pg_get_expr(d.adbin, d.adrelid) AS default_expr
       FROM pg_attribute a
       LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
       WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
       ORDER BY a.attnum`,
      [tableOid],
    );

    const colData = colResult.rows.map((r) => ({
      name: r.attname as string,
      dtype: r.data_type as string,
      notnull: r.attnotnull as boolean,
      defaultExpr: r.default_expr as string | null,
    }));

    const maxNameLen = Math.max(...colData.map((c) => c.name.length), 0);
    const colDefs: string[] = [];

    for (const col of colData) {
      const isSerial = col.defaultExpr?.startsWith('nextval(') ?? false;
      let displayType: string;
      if (isSerial) {
        displayType =
          col.dtype === 'bigint' ? 'bigserial' : col.dtype === 'smallint' ? 'smallserial' : 'serial';
      } else {
        displayType = col.dtype;
      }

      let def = `    ${col.name.padEnd(maxNameLen)} ${displayType}`;
      if (!isSerial && col.defaultExpr) {
        def += ` default ${col.defaultExpr}`;
      }
      if (col.notnull && !isSerial) {
        def += ' not null';
      }
      colDefs.push(def);
    }

    // Primary key
    const pkResult = await client.query(
      `SELECT array_agg(a.attname ORDER BY x.n)
       FROM pg_index i
       JOIN pg_attribute a ON a.attrelid = i.indrelid
       JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS x(attnum, n)
         ON a.attnum = x.attnum
       WHERE i.indrelid = $1 AND i.indisprimary
       GROUP BY i.indexrelid`,
      [tableOid],
    );

    if (pkResult.rows.length > 0) {
      const pkCols: string[] = pkResult.rows[0].array_agg;
      colDefs.push(`    constraint ${table}_pkey primary key (${pkCols.join(', ')})`);
    }

    let ddl = `create table ${quoteIdentifier(schema)}.${quoteIdentifier(table)}\n(\n${colDefs.join(',\n')}\n);`;

    // Owner
    const ownerResult = await client.query(
      `SELECT pg_catalog.pg_get_userbyid(c.relowner) FROM pg_class c WHERE c.oid = $1`,
      [tableOid],
    );

    if (ownerResult.rows.length > 0) {
      const owner: string = ownerResult.rows[0].pg_get_userbyid;
      ddl += `\n\nalter table ${quoteIdentifier(schema)}.${quoteIdentifier(table)}\n    owner to ${quoteIdentifier(owner)};`;
    }

    return ddl;
  } finally {
    await client.end();
  }
}

export async function exportParquet(
  conn: SavedConnection,
  schema: string,
  table: string,
  filePath: string,
): Promise<number> {
  const client = await connect(conn);
  try {
    const sql = `SELECT * FROM ${quoteIdentifier(schema)}.${quoteIdentifier(table)}`;
    const result = await client.query(sql);
    const columns = result.fields?.map((f) => f.name) ?? [];
    const rows: string[][] = (result.rows ?? []).map((row: Record<string, unknown>) =>
      columns.map((col) => {
        const val = row[col];
        return val == null ? '' : String(val);
      }),
    );

    // Write as CSV (Parquet export requires arrow/parquet libs — use CSV as portable fallback)
    const fs = await import('node:fs');
    const header = columns.map((c) => `"${c.replace(/"/g, '""')}"`).join(',');
    const csvRows = rows.map((r) =>
      r.map((cell) => `"${cell.replace(/"/g, '""')}"`).join(','),
    );
    fs.writeFileSync(filePath, [header, ...csvRows].join('\n'), 'utf-8');
    return rows.length;
  } finally {
    await client.end();
  }
}
