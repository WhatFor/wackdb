# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WackDB is an educational SQL database engine written in Rust. It implements SQL parsing and execution, page-based storage, B-tree indexes, WAL persistence, and LRU page caching from scratch.

## Commands

```bash
# Build
cargo build

# Run all tests
cargo test

# Run tests for a specific crate
cargo test -p lexer
cargo test -p parser
cargo test -p engine

# Run a specific test by name
cargo test test_simple_select_statement

# Run with output
cargo test -- --nocapture

# Run the CLI (REPL)
cargo run --release

# Run benchmarks
cargo bench -p engine --bench page
cargo bench -p engine --bench btree
```

The REPL accepts SQL commands. Run `.init` first to initialize the master database in `./data/`.

## Workspace Structure

Six crates with a strict layered dependency:

```
cli → parser → lexer
cli → engine → parser → lexer
cli_common ← cli, engine
macros (unused)
```

- **`crates/cli`** — REPL entry point (`main.rs`, `repl.rs`). Accepts SQL, formats and prints `ResultSet`.
- **`crates/lexer`** — Tokenizes SQL strings into `Token` values.
- **`crates/parser`** — Recursive descent parser producing an AST (`ast.rs`).
- **`crates/engine`** — Core database engine (see below).
- **`crates/cli_common`** — Shared `ParseError`/`ExecuteError` types via `thiserror`.

## Engine Architecture

The engine crate (`crates/engine/src/`) is the core of the project:

- **`engine.rs`** — `Engine` struct: entry point for `execute()`. Coordinates all components.
- **`vm.rs`** — `VirtualMachine`: executes statements.
- **`server.rs`** — `Server`: manages the master database and user databases. Knows which tables/columns exist.
- **`page.rs`** — Binary page format using `deku` for encoding/decoding. Pages are 8192 bytes; slot-based row storage.
- **`page_cache.rs`** — LRU cache for in-memory pages (backed by `lru.rs`).
- **`fm.rs`** — `FileManager`: maps file IDs to open file handles.
- **`persistence.rs`** — Low-level file I/O: creating and opening `.wak` database files.
- **`db.rs`** — `DatabaseInfo` page (page 1) encoding/decoding.
- **`btree.rs`** — B-tree index implementation.

## Data Flow

1. CLI receives SQL string
2. Lexer tokenizes → `Vec<Token>`
3. Parser builds → `Statement` (AST)
4. `Engine::execute()` dispatches to `VirtualMachine`
5. VM reads rows from pages via `PageCache` → `FileManager` → disk
6. Returns `ResultSet` (column definitions + rows) to CLI

## Storage Format

- Files stored in `./data/` (hardcoded path)
- `.wak` = primary data file, `.wal` = write-ahead log
- Each file: page 0 is `FileInfo`, page 1 is `DatabaseInfo`, subsequent pages hold table data
- See `docs/file_layout.md` for binary layout details

## Master Database

The master database tracks all user databases/tables/columns in internal system tables (`databases`, `tables`, `columns`, `indexes`). `Server` coordinates between the master DB and user DBs. See `docs/databases_and_tables.md`.

## Tests

All tests are co-located with source code using `#[cfg(test)]` modules — no separate `tests/` directory. The lexer and parser crates have the most test coverage (150+ tests combined). Engine tests focus on encoding/decoding and data structure correctness.
