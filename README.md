# pidgn_db

Database ORM for the pidgn web framework.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Zig](https://img.shields.io/badge/Zig-0.16.0-orange.svg)](https://ziglang.org/)

A database abstraction layer providing unified SQLite and PostgreSQL support with compile-time schema definitions, type-safe repositories, composable query builders, and connection pooling.

## Features

- **Dual Backend** -- SQLite and PostgreSQL with the same API
- **Compile-time Schema** -- zero-cost struct-to-table mapping with automatic DDL generation
- **Repository Pattern** -- type-safe CRUD with `all`, `one`, `get`, `insert`, `update`, `delete`, `count`
- **Query Builder** -- composable fluent API with where, joins, groups, aggregates, ordering, limit/offset
- **Connection Pooling** -- thread-safe pool with configurable size and timeouts
- **Transactions** -- nested transactions via savepoints, configurable isolation levels
- **Migrations** -- versioned up/down migrations with DDL helpers
- **Changesets** -- field validation and change tracking for form submissions
- **Testing Utilities** -- `TestSandbox` for auto-rollback, `Factory` for test data, `seed` for bulk inserts
- **Dialect-aware SQL** -- `?` placeholders for SQLite, `$N` for PostgreSQL

## Quick Start

### Define a Schema

```zig
const pidgn_db = @import("pidgn_db");

pub const User = struct {
    id: i64,
    name: []const u8,
    email: []const u8,
    inserted_at: i64 = 0,
    updated_at: i64 = 0,

    pub const Meta = pidgn_db.Schema.define(@This(), .{
        .table = "users",
        .primary_key = "id",
        .timestamps = true,
    });
};
```

### Connect and Query

```zig
// SQLite
var pool = try pidgn_db.SqlitePool.init(.{ .size = 5, .connection = .{} });
defer pool.deinit();

var repo = pidgn_db.SqliteRepo.init(&pool);

// Insert
const user = try repo.insert(User, .{
    .id = 0,
    .name = "Alice",
    .email = "alice@example.com",
}, allocator);

// Query
const query = pidgn_db.Query(User).init()
    .where("name", .eq, "Alice")
    .order("inserted_at", .desc)
    .limit(10);

const users = try repo.all(User, query, allocator);
```

### Migrations

```zig
const migrations = &.{
    pidgn_db.SqliteMigrationDef{
        .version = 1,
        .name = "create_users",
        .up = &struct {
            fn up(ctx: *MigrationContext) !void {
                try ctx.createTable("users", &.{
                    .{ .name = "id", .col_type = .bigint, .primary_key = true, .auto_increment = true },
                    .{ .name = "name", .col_type = .text, .not_null = true },
                    .{ .name = "email", .col_type = .text, .not_null = true, .unique = true },
                });
            }
        }.up,
        .down = &struct {
            fn down(ctx: *MigrationContext) !void {
                try ctx.dropTable("users");
            }
        }.down,
    },
};
```

### Query Builder

```zig
const q = pidgn_db.Query(User).init()
    .where("email", .like, "%@example.com")
    .orWhere("name", .eq, "admin")
    .join("posts", "users.id", "posts.user_id")
    .group(&.{"users.id"})
    .having("count(*)", .gt, "5")
    .order("name", .asc)
    .limit(20)
    .offset(40);

const results = try repo.all(User, q, allocator);
const total = try repo.count(User, q, allocator);
```

### Transactions

```zig
var tx = pidgn_db.SqliteTransaction;
try tx.begin(&conn);
// ... operations ...
try tx.commit(&conn);

// With isolation level
try tx.beginWithIsolation(&conn, .serializable);
```

### Testing

```zig
test "user creation" {
    var sandbox = try pidgn_db.TestSandbox(sqlite).begin(&conn);
    defer sandbox.rollback(); // auto-rollback after test

    const user = try repo.insert(User, .{ ... }, std.testing.allocator);
    try std.testing.expectEqualStrings("Alice", user.name);
}
```

## Building

```bash
zig build            # Build (SQLite enabled by default)
zig build test       # Run tests

# With PostgreSQL
zig build -Dpostgres=true
zig build test -Dpostgres=true
```

## Documentation

Full documentation available at [docs.pidgn.indielab.link](https://docs.pidgn.indielab.link) under the Database section.

## Ecosystem

| Package | Description |
|---------|-------------|
| [pidgn.zig](https://github.com/seemsindie/pidgn.zig) | Core web framework |
| [pidgn_db](https://github.com/seemsindie/pidgn_db) | Database ORM (SQLite + PostgreSQL) |
| [pidgn_jobs](https://github.com/seemsindie/pidgn_jobs) | Background job processing |
| [pidgn_mailer](https://github.com/seemsindie/pidgn_mailer) | Email sending |
| [pidgn_template](https://github.com/seemsindie/pidgn_template) | Template engine |
| [pidgn_cli](https://github.com/seemsindie/pidgn_cli) | CLI tooling |

## Requirements

- Zig 0.16.0-dev.2535+b5bd49460 or later
- All dependencies are vendored or fetched automatically -- no system libraries required
  - SQLite is vendored as an amalgamation build
  - libpq is fetched via `build.zig.zon` for PostgreSQL support

## License

MIT License -- Copyright (c) 2026 Ivan Stamenkovic
