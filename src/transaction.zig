const std = @import("std");
const backend = @import("backend.zig");
const connection_mod = @import("connection.zig");
const ConnectionState = connection_mod.ConnectionState;
const pool_mod = @import("pool.zig");
const sqlite = @import("sqlite.zig");

pub const IsolationLevel = enum { read_uncommitted, read_committed, repeatable_read, serializable };

pub fn Transaction(comptime Backend: type) type {
    comptime backend.validate(Backend);
    const Conn = connection_mod.Connection(Backend);
    const PoolType = pool_mod.Pool(Backend);

    return struct {
        pub fn begin(conn: *Conn) !void {
            if (conn.state == .in_transaction) {
                // Nested transaction: use savepoint
                conn.savepoint_depth += 1;
                var buf: [80]u8 = undefined;
                const sp_sql = std.fmt.bufPrint(&buf, "SAVEPOINT sp_{d}", .{conn.savepoint_depth}) catch return error.InternalError;
                buf[sp_sql.len] = 0;
                const sp_z: [:0]const u8 = buf[0..sp_sql.len :0];
                try conn.exec(sp_z);
                return;
            }
            try conn.exec("BEGIN");
            conn.state = .in_transaction;
        }

        pub fn beginWithIsolation(conn: *Conn, level: IsolationLevel) !void {
            if (conn.state == .in_transaction) return error.AlreadyInTransaction;
            if (Backend.dialect == .sqlite) {
                // SQLite: use different BEGIN modes
                switch (level) {
                    .serializable => try conn.exec("BEGIN EXCLUSIVE"),
                    else => try conn.exec("BEGIN IMMEDIATE"),
                }
            } else {
                // PostgreSQL: BEGIN then SET TRANSACTION ISOLATION LEVEL
                try conn.exec("BEGIN");
                const set_sql: [:0]const u8 = switch (level) {
                    .read_uncommitted => "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
                    .read_committed => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
                    .repeatable_read => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                    .serializable => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                };
                try conn.exec(set_sql);
            }
            conn.state = .in_transaction;
        }

        pub fn commit(conn: *Conn) !void {
            if (conn.savepoint_depth > 0) {
                var buf: [80]u8 = undefined;
                const sp_sql = std.fmt.bufPrint(&buf, "RELEASE SAVEPOINT sp_{d}", .{conn.savepoint_depth}) catch return error.InternalError;
                buf[sp_sql.len] = 0;
                const sp_z: [:0]const u8 = buf[0..sp_sql.len :0];
                try conn.exec(sp_z);
                conn.savepoint_depth -= 1;
                return;
            }
            try conn.exec("COMMIT");
            conn.state = .connected;
        }

        pub fn rollback(conn: *Conn) !void {
            if (conn.savepoint_depth > 0) {
                var buf: [80]u8 = undefined;
                const sp_sql = std.fmt.bufPrint(&buf, "ROLLBACK TO SAVEPOINT sp_{d}", .{conn.savepoint_depth}) catch return error.InternalError;
                buf[sp_sql.len] = 0;
                const sp_z: [:0]const u8 = buf[0..sp_sql.len :0];
                try conn.exec(sp_z);
                conn.savepoint_depth -= 1;
                return;
            }
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

test "nested savepoint: begin → insert A → begin (savepoint) → insert B → rollback (savepoint) → commit → only A persists" {
    var conn = try SqliteConn.open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");

    // Outer transaction
    try Txn.begin(&conn);
    try conn.exec("INSERT INTO test (val) VALUES ('A')");

    // Nested savepoint
    try Txn.begin(&conn);
    try std.testing.expectEqual(@as(u16, 1), conn.savepoint_depth);
    try conn.exec("INSERT INTO test (val) VALUES ('B')");

    // Rollback savepoint — B should be gone
    try Txn.rollback(&conn);
    try std.testing.expectEqual(@as(u16, 0), conn.savepoint_depth);

    // Commit outer — A should persist
    try Txn.commit(&conn);
    try std.testing.expectEqual(ConnectionState.connected, conn.state);

    // Verify only A exists
    var stmt = try sqlite.Statement.prepare(&conn.db, "SELECT COUNT(*) FROM test");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));

    var stmt2 = try sqlite.Statement.prepare(&conn.db, "SELECT val FROM test");
    defer stmt2.finalize();
    _ = try stmt2.step();
    try std.testing.expectEqualStrings("A", stmt2.columnText(0).?);
}

test "double nesting: 3 levels deep" {
    var conn = try SqliteConn.open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");

    try Txn.begin(&conn); // Level 0 (real txn)
    try conn.exec("INSERT INTO test (val) VALUES ('L0')");

    try Txn.begin(&conn); // Level 1 (sp_1)
    try std.testing.expectEqual(@as(u16, 1), conn.savepoint_depth);
    try conn.exec("INSERT INTO test (val) VALUES ('L1')");

    try Txn.begin(&conn); // Level 2 (sp_2)
    try std.testing.expectEqual(@as(u16, 2), conn.savepoint_depth);
    try conn.exec("INSERT INTO test (val) VALUES ('L2')");

    try Txn.commit(&conn); // Release sp_2
    try std.testing.expectEqual(@as(u16, 1), conn.savepoint_depth);

    try Txn.rollback(&conn); // Rollback sp_1 (removes L1)
    try std.testing.expectEqual(@as(u16, 0), conn.savepoint_depth);

    try Txn.commit(&conn); // Commit outer

    // Verify: L0 persists, L1 rolled back (L2 was released into L1 which was then rolled back)
    var stmt = try sqlite.Statement.prepare(&conn.db, "SELECT val FROM test ORDER BY id");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("L0", stmt.columnText(0).?);
    // L1 and L2 should be gone since sp_1 was rolled back
    const has_more = try stmt.step();
    try std.testing.expect(!has_more);
}

test "isolation level SQLite IMMEDIATE" {
    var conn = try SqliteConn.open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");

    try Txn.beginWithIsolation(&conn, .read_committed);
    try std.testing.expectEqual(ConnectionState.in_transaction, conn.state);
    try conn.exec("INSERT INTO test (val) VALUES ('isolated')");
    try Txn.commit(&conn);

    var stmt = try sqlite.Statement.prepare(&conn.db, "SELECT val FROM test WHERE id = 1");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("isolated", stmt.columnText(0).?);
}

test "run still works with savepoints" {
    var pool = try SqlitePool.init(.{ .size = 1 });
    defer pool.deinit();

    var pc = try pool.checkout();
    try pc.conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)");
    pc.release();

    try Txn.run(&pool, &struct {
        fn func(conn: *SqliteConn) !void {
            try conn.exec("INSERT INTO test (val) VALUES ('outer')");
        }
    }.func);

    var pc2 = try pool.checkout();
    defer pc2.release();
    var stmt = try sqlite.Statement.prepare(&pc2.conn.db, "SELECT val FROM test");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqualStrings("outer", stmt.columnText(0).?);
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
