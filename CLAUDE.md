# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
cargo build                          # build both crates
cargo test                           # run all tests (expr_tests + any others)
cargo test -- <test_name>            # run single test
cargo clippy -- -W clippy::pedantic  # lint (project uses pedantic)
cargo run --bin bench_queries        # run query-building benchmarks
```

Rust edition 2024. `rustfmt.toml` uses `use_small_heuristics = "Max"`. All dev/test profiles use `opt-level = 3`.

## Architecture

Two-crate workspace:

- **`sql_query`** — runtime library: type-safe SQL query builder for PostgreSQL on sqlx
- **`sql_query_derive`** — proc-macro crate providing `#[derive(SqlCols)]` and `#[derive(SqlParamEnum)]`

### Core Flow

```
Reads:  SqlQ::select::<T>()...build()? -> UnbindedQuery -> .bind_as::<T>() -> BoundQueryAs<T> -> .fetch_*(pool)
Writes: SqlQ::insert/update/delete::<T>()...build()? -> UnbindedWriteQuery -> .bind() -> InvalidatingBoundQuery -> .execute(pool, redis)
```

### Key Abstractions

- **`Table` trait** (`shared/mod.rs`) — maps a Rust struct to a Postgres table. Associates column enum (`Col`), ID type, table name, primary key.
- **`Expr<T>`** (`shared/expr.rs`, ~1400 lines) — phantom-typed expression builder. All SQL expressions go through this. Methods chain to build SQL fragments; `.build()` or query-level finalization produces `(String, Vec<SqlParam>)`.
- **`ColOps<T>` trait** (`shared/expr.rs`) — shorthand methods on column enums (`.eq()`, `.count()`, `.alias()`, etc.). Implemented automatically by `#[derive(SqlCols)]`.
- **`SqlParam`** (`shared/value.rs`) — type-erased bind parameter enum. `From` impls for all supported Rust types. Custom Postgres enums via `SqlParamEnum` derive.
- **`UnbindedQuery`** (`shared/unbinded_query.rs`) — read-path query (SELECT). Binds into `BoundQueryAs`/`BoundQueryScalar`/`CachedBound*` wrappers.
- **`UnbindedWriteQuery`** (`shared/unbinded_query.rs`) — write-path query (INSERT/UPDATE/DELETE). `.bind()` returns `InvalidatingBoundQuery` (invalidates cache by default, requires redis). `.skip_inval()` opts out.
- **Statement builders** — `SqlSelect`/`SqlSetOp` implement `SqlBase` trait. `SqlInsert`/`SqlUpdate`/`SqlDelete` have inherent `.build()` returning `UnbindedWriteQuery`.

### Redis Cache Layer

`shared/cached.rs` — optional Redis caching with table-tag invalidation:
- `CacheTag` trait — implement on an enum to group related tables. Use `.tag(&MyTag::Group)` on cached reads and invalidating writes to attach extra tables.
- `cache_key(sql, binds)` — xxh3 hash of query + params, produces `sq:e:{hash}` key
- `with_cache()` — cache-or-fetch helper, registers entries under `sq:t:{table}` sets
- `invalidate_tables()` — Lua script sweeps all entries tagged with given tables

### Derive Macros

`sql_query_derive/src/lib.rs`:
- **`SqlCols`** — generates `{Struct}Col` enum (PascalCase variants, snake_case serialization via strum). Implements `From<Col> for Expr<T>`, `EvalExpr`, `ColOps<T>`, `SqlColId` (if `id` field exists), plus direct shorthand methods on the enum.
- **`SqlParamEnum`** — generates `From<Enum> for SqlParam` for custom Postgres enum types.

### Cross-Table References

`Expr<T>` is parameterized by table type. To reference columns from another table:
- `.column_of::<OtherTable>(col)` — explicit cross-table column
- `.coerce::<OtherTable>()` — reinterprets phantom type so foreign column works in surrounding expression

### ID Types

`define_id!(FooId)` macro generates a UUID v7 newtype with all necessary trait impls (Display, FromStr, sqlx::Type, serde, ts-rs, Into<SqlParam>).
