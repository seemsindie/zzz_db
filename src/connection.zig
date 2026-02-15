const std = @import("std");
const backend = @import("backend.zig");
const sqlite = @import("sqlite.zig");

pub const ConnectionState = enum {
    disconnected,
    connected,
    in_transaction,
};

pub fn Connection(comptime Backend: type) type {
    comptime backend.validate(Backend);

    return struct {
        const Self = @This();

        db: Backend.Db,
        config: Backend.Config,
        state: ConnectionState,

        pub fn open(config: Backend.Config) !Self {
            var db = try Backend.Db.open(config.database);
            errdefer db.close();

            if (Backend.dialect == .sqlite) {
                db.setBusyTimeout(config.busy_timeout_ms);
                if (config.enable_wal) {
                    db.enableWAL() catch {};
                }
                for (config.pragmas) |pragma| {
                    try db.exec(pragma);
                }
            }

            return .{
                .db = db,
                .config = config,
                .state = .connected,
            };
        }

        pub fn close(self: *Self) void {
            self.db.close();
            self.state = .disconnected;
        }

        pub fn isAlive(self: *Self) bool {
            if (self.state == .disconnected) return false;
            return self.db.isAlive();
        }

        pub fn reconnect(self: *Self) !void {
            if (self.state != .disconnected) {
                self.db.close();
            }
            var db = try Backend.Db.open(self.config.database);
            errdefer db.close();

            if (Backend.dialect == .sqlite) {
                db.setBusyTimeout(self.config.busy_timeout_ms);
                if (self.config.enable_wal) {
                    db.enableWAL() catch {};
                }
                for (self.config.pragmas) |pragma| {
                    try db.exec(pragma);
                }
            }

            self.db = db;
            self.state = .connected;
        }

        pub fn exec(self: *Self, sql: [:0]const u8) !void {
            return self.db.exec(sql);
        }
    };
}

// ── Tests ──────────────────────────────────────────────────────────────

test "open and close connection" {
    var conn = try Connection(sqlite).open(.{});
    defer conn.close();
    try std.testing.expectEqual(ConnectionState.connected, conn.state);
}

test "isAlive" {
    var conn = try Connection(sqlite).open(.{});
    try std.testing.expect(conn.isAlive());
    conn.close();
    try std.testing.expect(!conn.isAlive());
}

test "reconnect" {
    var conn = try Connection(sqlite).open(.{});
    conn.close();
    try std.testing.expect(!conn.isAlive());
    try conn.reconnect();
    try std.testing.expect(conn.isAlive());
    conn.close();
}

test "exec" {
    var conn = try Connection(sqlite).open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    try conn.exec("INSERT INTO test (name) VALUES ('alice')");

    // Verify using direct sqlite.Statement
    var stmt = try sqlite.Statement.prepare(&conn.db, "SELECT name FROM test WHERE id = 1");
    defer stmt.finalize();
    const has_row = try stmt.step();
    try std.testing.expect(has_row);
    try std.testing.expectEqualStrings("alice", stmt.columnText(0).?);
}

test "lastInsertRowId and changes via db" {
    var conn = try Connection(sqlite).open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    try conn.exec("INSERT INTO test (name) VALUES ('alice')");
    try std.testing.expectEqual(@as(i64, 1), conn.db.lastInsertRowId());
    try std.testing.expectEqual(@as(i32, 1), conn.db.changes());
}
