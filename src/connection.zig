const std = @import("std");
const sqlite = @import("sqlite.zig");

pub const ConnectionConfig = struct {
    database: [:0]const u8 = ":memory:",
    busy_timeout_ms: c_int = 5000,
    enable_wal: bool = true,
    pragmas: []const [:0]const u8 = &.{
        "PRAGMA foreign_keys = ON",
    },
};

pub const ConnectionState = enum {
    disconnected,
    connected,
    in_transaction,
};

pub const Connection = struct {
    db: sqlite.Db,
    config: ConnectionConfig,
    state: ConnectionState,

    pub fn open(config: ConnectionConfig) !Connection {
        var db = try sqlite.Db.open(config.database);
        errdefer db.close();

        db.setBusyTimeout(config.busy_timeout_ms);

        if (config.enable_wal) {
            db.enableWAL() catch {};
        }

        for (config.pragmas) |pragma| {
            try db.exec(pragma);
        }

        return Connection{
            .db = db,
            .config = config,
            .state = .connected,
        };
    }

    pub fn close(self: *Connection) void {
        self.db.close();
        self.state = .disconnected;
    }

    pub fn isAlive(self: *Connection) bool {
        if (self.state == .disconnected) return false;
        return self.db.isAlive();
    }

    pub fn reconnect(self: *Connection) !void {
        if (self.state != .disconnected) {
            self.db.close();
        }
        var db = try sqlite.Db.open(self.config.database);
        errdefer db.close();

        db.setBusyTimeout(self.config.busy_timeout_ms);

        if (self.config.enable_wal) {
            db.enableWAL() catch {};
        }

        for (self.config.pragmas) |pragma| {
            try db.exec(pragma);
        }

        self.db = db;
        self.state = .connected;
    }

    pub fn prepare(self: *Connection, sql: [:0]const u8) !sqlite.Statement {
        return sqlite.Statement.prepare(&self.db, sql);
    }

    pub fn exec(self: *Connection, sql: [:0]const u8) !void {
        return self.db.exec(sql);
    }

    pub fn lastInsertRowId(self: *const Connection) i64 {
        return self.db.lastInsertRowId();
    }

    pub fn changes(self: *const Connection) i32 {
        return self.db.changes();
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "open and close connection" {
    var conn = try Connection.open(.{});
    defer conn.close();
    try std.testing.expectEqual(ConnectionState.connected, conn.state);
}

test "isAlive" {
    var conn = try Connection.open(.{});
    try std.testing.expect(conn.isAlive());
    conn.close();
    try std.testing.expect(!conn.isAlive());
}

test "reconnect" {
    var conn = try Connection.open(.{});
    conn.close();
    try std.testing.expect(!conn.isAlive());
    try conn.reconnect();
    try std.testing.expect(conn.isAlive());
    conn.close();
}

test "exec and prepare" {
    var conn = try Connection.open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    try conn.exec("INSERT INTO test (name) VALUES ('alice')");

    var stmt = try conn.prepare("SELECT name FROM test WHERE id = 1");
    defer stmt.finalize();
    const has_row = try stmt.step();
    try std.testing.expect(has_row);
    try std.testing.expectEqualStrings("alice", stmt.columnText(0).?);
}

test "lastInsertRowId and changes" {
    var conn = try Connection.open(.{});
    defer conn.close();

    try conn.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    try conn.exec("INSERT INTO test (name) VALUES ('alice')");
    try std.testing.expectEqual(@as(i64, 1), conn.lastInsertRowId());
    try std.testing.expectEqual(@as(i32, 1), conn.changes());
}
