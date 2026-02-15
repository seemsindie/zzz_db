const std = @import("std");
const backend = @import("backend.zig");
const connection_mod = @import("connection.zig");
const ConnectionState = connection_mod.ConnectionState;
const pool_mod = @import("pool.zig");
const sqlite = @import("sqlite.zig");

pub fn Transaction(comptime Backend: type) type {
    comptime backend.validate(Backend);
    const Conn = connection_mod.Connection(Backend);
    const PoolType = pool_mod.Pool(Backend);

    return struct {
        pub fn begin(conn: *Conn) !void {
            if (conn.state == .in_transaction) return error.AlreadyInTransaction;
            try conn.exec("BEGIN");
            conn.state = .in_transaction;
        }

        pub fn commit(conn: *Conn) !void {
            try conn.exec("COMMIT");
            conn.state = .connected;
        }

        pub fn rollback(conn: *Conn) !void {
            try conn.exec("ROLLBACK");
            conn.state = .connected;
        }

        pub fn run(pool: *PoolType, func: *const fn (*Conn) anyerror!void) !void {
            var pc = try pool.checkout();
            defer pc.release();

            try begin(pc.conn);
            errdefer rollback(pc.conn) catch {};

            try func(pc.conn);
            try commit(pc.conn);
        }
    };
}

pub const TransactionError = error{AlreadyInTransaction};

// ── Tests ──────────────────────────────────────────────────────────────

const SqliteConn = connection_mod.Connection(sqlite);
const SqlitePool = pool_mod.Pool(sqlite);
const Txn = Transaction(sqlite);

test "begin and commit" {
    var conn = try SqliteConn.open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");

    try Txn.begin(&conn);
    try std.testing.expectEqual(ConnectionState.in_transaction, conn.state);
    try conn.exec("INSERT INTO test (val) VALUES ('hello')");
    try Txn.commit(&conn);
    try std.testing.expectEqual(ConnectionState.connected, conn.state);

    // Verify data persisted
    var stmt = try sqlite.Statement.prepare(&conn.db, "SELECT val FROM test WHERE id = 1");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("hello", stmt.columnText(0).?);
}

test "rollback on error" {
    var conn = try SqliteConn.open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");
    try conn.exec("INSERT INTO test (val) VALUES ('original')");

    try Txn.begin(&conn);
    try conn.exec("UPDATE test SET val = 'modified'");
    try Txn.rollback(&conn);

    // Verify rollback
    var stmt = try sqlite.Statement.prepare(&conn.db, "SELECT val FROM test WHERE id = 1");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("original", stmt.columnText(0).?);
}

test "run with pool" {
    var pool = try SqlitePool.init(.{ .size = 1 });
    defer pool.deinit();

    // Create table first
    var pc = try pool.checkout();
    try pc.conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");
    pc.release();

    try Txn.run(&pool, &struct {
        fn func(conn: *SqliteConn) !void {
            try conn.exec("INSERT INTO test (val) VALUES ('from_txn')");
        }
    }.func);

    // Verify committed
    var pc2 = try pool.checkout();
    defer pc2.release();
    var stmt = try sqlite.Statement.prepare(&pc2.conn.db, "SELECT val FROM test WHERE id = 1");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("from_txn", stmt.columnText(0).?);
}

test "double begin error" {
    var conn = try SqliteConn.open(.{});
    defer conn.close();

    try Txn.begin(&conn);
    const result = Txn.begin(&conn);
    try std.testing.expectError(error.AlreadyInTransaction, result);
    try Txn.rollback(&conn);
}

test "run rollback on error" {
    var pool = try SqlitePool.init(.{ .size = 1 });
    defer pool.deinit();

    var pc = try pool.checkout();
    try pc.conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");
    try pc.conn.exec("INSERT INTO test (val) VALUES ('keep')");
    pc.release();

    const result = Txn.run(&pool, &struct {
        fn func(conn: *SqliteConn) !void {
            try conn.exec("UPDATE test SET val = 'changed'");
            return error.IntentionalError;
        }
    }.func);
    try std.testing.expectError(error.IntentionalError, result);

    // Verify rollback — original data intact
    var pc2 = try pool.checkout();
    defer pc2.release();
    var stmt = try sqlite.Statement.prepare(&pc2.conn.db, "SELECT val FROM test WHERE id = 1");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("keep", stmt.columnText(0).?);
}
