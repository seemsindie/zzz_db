//! zzz_db - Database Layer for the Zzz Web Framework
//!
//! Provides SQLite support with connection pooling, comptime schema definitions,
//! a composable query builder, and type-safe Repo operations.

const std = @import("std");

// SQLite
pub const sqlite = @import("sqlite.zig");
pub const SqliteError = sqlite.SqliteError;

// Connection
pub const Connection = @import("connection.zig").Connection;
pub const ConnectionConfig = @import("connection.zig").ConnectionConfig;

// Pool
pub const Pool = @import("pool.zig").Pool;
pub const PoolConfig = @import("pool.zig").PoolConfig;
pub const PooledConnection = @import("pool.zig").PooledConnection;

// Schema
pub const Schema = @import("schema.zig");

// Query
pub const Query = @import("query.zig").Query;
pub const Op = @import("query.zig").Op;
pub const Order = @import("query.zig").Order;

// Repo
pub const Repo = @import("repo.zig").Repo;
pub const freeAll = @import("repo.zig").freeAll;
pub const freeOne = @import("repo.zig").freeOne;

// Transactions
pub const Transaction = @import("transaction.zig");

pub const version = "0.1.0";

test {
    std.testing.refAllDecls(@This());
}
