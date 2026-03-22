# rdb3

A lightweight, fast desktop PostgreSQL client built with Electron, React, and TypeScript.

## Features

### Connection Management
- Save, edit, and manage multiple PostgreSQL connections
- Test connections before saving
- Quick-switch between saved connections
- Password auth or `~/.pgpass`-based auth
- Optional SSH tunneling for PostgreSQL connections
- SSH password and private-key authentication support

### Schema Explorer
- Hierarchical database tree: Schema > Tables > Columns / Keys / Indexes
- Column details with data types, nullability, and defaults
- Primary key, unique, and foreign key display with referenced tables
- Index listing with uniqueness indicators
- Drag-and-drop table names into the SQL editor
- Right-click context menu with full table operations

### SQL Editor
- CodeMirror-based editor with SQL syntax highlighting
- Schema-aware SQL autocompletion from the loaded database tree
- Multiple query tabs
- Query presets (Session, Tables, Activity)
- Save queries to `.sql` files
- Sortable, paginated result grid
- Query history with execution timing
- Result export to CSV and Excel

### Dashboard
- Real-time server monitoring with sparkline graphs
  - CPU usage, RAM usage, cache hit ratio
  - Transaction throughput (TPS), connection saturation
- Server uptime, database size, active connections
- Active query list from PostgreSQL activity views

### Table Operations (right-click context menu)
- **Refresh** -- reload the database tree
- **Show DDL** -- view the `CREATE TABLE` statement
- **Edit Data** -- inline-editable data grid with:
  - Double-click cells to edit (Tab/Shift+Tab navigation)
  - Add and delete rows
  - NULL value handling
  - Pending changes tracker with Apply/Discard
  - Requires a primary key for safe `UPDATE`/`DELETE`
- **Export CSV** -- export table preview to CSV
- **Export CSV (full table)** -- server-side export of all rows to CSV
- **Export with pg_dump** -- export via `pg_dump` (SQL, custom, or tar format)
- **Modify Table** -- GUI for altering table structure:
  - Rename table
  - Add, drop, rename columns
  - Change column types, nullability, defaults
  - Live DDL preview of pending `ALTER TABLE` statements
  - All changes applied in a single transaction
- **Truncate Table** -- delete all rows with confirmation dialog and optional CASCADE
- **Drop Table** -- permanently delete table with confirmation dialog and optional CASCADE

CASCADE options are automatically enabled only when other tables reference the target via foreign keys.

## Authentication

- Standard PostgreSQL username/password connections
- `pgpass` support via `~/.pgpass` or `PGPASSFILE`
- Optional SSH tunnel layer before connecting to PostgreSQL
- SSH auth supports password or private key + passphrase

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Runtime | Electron 41 |
| Frontend | React 19, Tailwind CSS 4.2, CodeMirror 6 |
| Backend | Node.js (pg), TypeScript 5 |
| Build | Vite 6, electron-builder |
| Package Manager | bun |

## Getting Started

### Prerequisites

- [bun](https://bun.sh/) (v1.0+)
- [Node.js](https://nodejs.org/) (v18+)
- A running PostgreSQL instance to connect to
- `pg_dump` installed if you want to use the pg_dump export option

### Install Dependencies

```bash
bun install
```

### Development

```bash
bun run dev
```

### Production (local run)

```bash
bun run start
```

## Testing

```bash
# Unit/integration tests
bun run test

# Watch mode
bun run test:watch

# Coverage
bun run test:coverage

# Live PostgreSQL tests
bun run test:live
```

`test:live` expects `DB_HOST`, `DB_PORT`, `DB_NAME`, and `DB_USER`. Password can come from `DB_PASSWORD` or from a matching entry in `~/.pgpass`.

## Building Installers

Build distributable installers for your platform:

```bash
# Current platform
bun run dist

# macOS (universal DMG -- arm64 + x64)
bun run dist:mac

# Windows (NSIS installer)
bun run dist:win

# Linux (AppImage + .deb)
bun run dist:linux
```

Output is written to the `release/` directory.

### Regenerate App Icons

```bash
node scripts/generate-icons.mjs
```

Generates `build/icon.icns` (macOS), `build/icon.png` (1024x1024), and `build/icon_256x256.png` (Windows/Linux).

## Project Structure

```
src/
  main/           Electron main process
    index.ts        Window creation, app lifecycle
    ipc.ts          IPC handlers
    postgres.ts     PostgreSQL operations
    ssh-tunnel.ts   SSH tunnel lifecycle and forwarding
    types.ts        Shared type definitions
    state.ts        App state
    storage.ts      Connection persistence
  renderer/       React frontend
    App.tsx         Root component
    components/
      AppShell.tsx    Main UI shell
      SqlEditor.tsx   CodeMirror SQL editor
    lib/
      api.ts        IPC client API
      types.ts      Frontend type definitions
  preload/        Electron preload (context bridge)
build/            App icons
scripts/          Build scripts
tests/            Unit, integration, and live tests
```

## License

MIT
