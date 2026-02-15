const std = @import("std");
const Connection = @import("connection.zig").Connection;
const ConnectionState = @import("connection.zig").ConnectionState;
const Pool = @import("pool.zig").Pool;

pub fn begin(conn: *Connection) !void {
    if (conn.state == .in_transaction) return error.AlreadyInTransaction;
    try conn.exec("BEGIN");
    conn.state = .in_transaction;
}

pub fn commit(conn: *Connection) !void {
    try conn.exec("COMMIT");
    conn.state = .connected;
}

pub fn rollback(conn: *Connection) !void {
    try conn.exec("ROLLBACK");
    conn.state = .connected;
}

/// Checkout a connection from the pool, begin a transaction, run the function,
/// commit on success or rollback on error, then check the connection back in.
pub fn run(pool: *Pool, func: *const fn (*Connection) anyerror!void) !void {
    var pc = try pool.checkout();
    defer pc.release();

    try begin(pc.conn);
    errdefer rollback(pc.conn) catch {};

    try func(pc.conn);
    try commit(pc.conn);
}

pub const TransactionError = error{AlreadyInTransaction};

// ── Tests ──────────────────────────────────────────────────────────────

test "begin and commit" {
    var conn = try Connection.open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");

    try begin(&conn);
    try std.testing.expectEqual(ConnectionState.in_transaction, conn.state);
    try conn.exec("INSERT INTO test (val) VALUES ('hello')");
    try commit(&conn);
    try std.testing.expectEqual(ConnectionState.connected, conn.state);

    // Verify data persisted
    var stmt = try conn.prepare("SELECT val FROM test WHERE id = 1");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("hello", stmt.columnText(0).?);
}

test "rollback on error" {
    var conn = try Connection.open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");
    try conn.exec("INSERT INTO test (val) VALUES ('original')");

    try begin(&conn);
    try conn.exec("UPDATE test SET val = 'modified'");
    try rollback(&conn);

    // Verify rollback
    var stmt = try conn.prepare("SELECT val FROM test WHERE id = 1");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("original", stmt.columnText(0).?);
}

test "run with pool" {
    var pool = try Pool.init(.{ .size = 1 });
    defer pool.deinit();

    // Create table first
    var pc = try pool.checkout();
    try pc.conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");
    pc.release();

    try run(&pool, &struct {
        fn func(conn: *Connection) !void {
            try conn.exec("INSERT INTO test (val) VALUES ('from_txn')");
        }
    }.func);

    // Verify committed
    var pc2 = try pool.checkout();
    defer pc2.release();
    var stmt = try pc2.conn.prepare("SELECT val FROM test WHERE id = 1");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("from_txn", stmt.columnText(0).?);
}

test "double begin error" {
    var conn = try Connection.open(.{});
    defer conn.close();

    try begin(&conn);
    const result = begin(&conn);
    try std.testing.expectError(error.AlreadyInTransaction, result);
    try rollback(&conn);
}

test "run rollback on error" {
    var pool = try Pool.init(.{ .size = 1 });
    defer pool.deinit();

    var pc = try pool.checkout();
    try pc.conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");
    try pc.conn.exec("INSERT INTO test (val) VALUES ('keep')");
    pc.release();

    const result = run(&pool, &struct {
        fn func(conn: *Connection) !void {
            try conn.exec("UPDATE test SET val = 'changed'");
            return error.IntentionalError;
        }
    }.func);
    try std.testing.expectError(error.IntentionalError, result);

    // Verify rollback — original data intact
    var pc2 = try pool.checkout();
    defer pc2.release();
    var stmt = try pc2.conn.prepare("SELECT val FROM test WHERE id = 1");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("keep", stmt.columnText(0).?);
}
