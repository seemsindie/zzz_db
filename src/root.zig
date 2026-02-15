//! zzz_db - Database Layer for the Zzz Web Framework
//!
//! Provides SQLite and PostgreSQL support with connection pooling, comptime schema definitions,
//! a composable query builder, and type-safe Repo operations.
//! All core types are generic over a backend type (sqlite or postgres).

const std = @import("std");

// Backend
pub const backend = @import("backend.zig");
pub const Dialect = backend.Dialect;

// Backends
pub const sqlite = @import("sqlite.zig");
pub const SqliteError = sqlite.SqliteError;

const db_options = @import("db_options");
pub const postgres_enabled = db_options.postgres_enabled;
pub const postgres = if (db_options.postgres_enabled) @import("postgres.zig") else struct {};

// Generic types
pub const Connection = @import("connection.zig").Connection;
pub const ConnectionState = @import("connection.zig").ConnectionState;
pub const Pool = @import("pool.zig").Pool;
pub const PoolConfig = @import("pool.zig").PoolConfig;
pub const PooledConnection = @import("pool.zig").PooledConnection;
pub const Repo = @import("repo.zig").Repo;
pub const Transaction = @import("transaction.zig").Transaction;

// Schema & Query (unchanged)
pub const Schema = @import("schema.zig");
pub const Query = @import("query.zig").Query;
pub const Op = @import("query.zig").Op;
pub const Order = @import("query.zig").Order;

// Helpers
pub const freeAll = @import("repo.zig").freeAll;
pub const freeOne = @import("repo.zig").freeOne;

// Convenience aliases — backward compatible
pub const SqliteConnection = Connection(sqlite);
pub const SqlitePool = Pool(sqlite);
pub const SqlitePoolConfig = PoolConfig(sqlite);
pub const SqliteRepo = Repo(sqlite);
pub const SqliteTransaction = Transaction(sqlite);

pub const PgConnection = if (db_options.postgres_enabled) Connection(postgres) else struct {};
pub const PgPool = if (db_options.postgres_enabled) Pool(postgres) else struct {};
pub const PgPoolConfig = if (db_options.postgres_enabled) PoolConfig(postgres) else struct {};
pub const PgRepo = if (db_options.postgres_enabled) Repo(postgres) else struct {};
pub const PgTransaction = if (db_options.postgres_enabled) Transaction(postgres) else struct {};

pub const version = "0.2.0";

test {
    std.testing.refAllDecls(@This());
}
