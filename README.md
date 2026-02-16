# zzz_db

A database abstraction layer for the zzz web framework. Provides unified SQLite and PostgreSQL support with compile-time schema definitions, type-safe repositories, composable query builders, and connection pooling.

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
const zzz_db = @import("zzz_db");

pub const User = struct {
    id: i64,
    name: []const u8,
    email: []const u8,
    inserted_at: i64 = 0,
    updated_at: i64 = 0,

    pub const Meta = zzz_db.Schema.define(@This(), .{
        .table = "users",
        .primary_key = "id",
        .timestamps = true,
    });
};
```

### Connect and Query

```zig
// SQLite
var pool = try zzz_db.SqlitePool.init(.{ .size = 5, .connection = .{} });
defer pool.deinit();

var repo = zzz_db.SqliteRepo.init(&pool);

// Insert
const user = try repo.insert(User, .{
    .id = 0,
    .name = "Alice",
    .email = "alice@example.com",
}, allocator);

// Query
const query = zzz_db.Query(User).init()
    .where("name", .eq, "Alice")
    .order("inserted_at", .desc)
    .limit(10);

const users = try repo.all(User, query, allocator);
```

### Migrations

```zig
const migrations = &.{
    zzz_db.SqliteMigrationDef{
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
const q = zzz_db.Query(User).init()
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
var tx = zzz_db.SqliteTransaction;
try tx.begin(&conn);
// ... operations ...
try tx.commit(&conn);

// With isolation level
try tx.beginWithIsolation(&conn, .serializable);
```

### Testing

```zig
test "user creation" {
    var sandbox = try zzz_db.TestSandbox(sqlite).begin(&conn);
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

## Requirements

- Zig 0.16.0-dev.2535+b5bd49460 or later
- SQLite3 (`libsqlite3-dev` on Linux, included on macOS)
- PostgreSQL (`libpq-dev` on Linux, optional)

## License

MIT License - Copyright (c) 2026 Ivan Stamenkovic
