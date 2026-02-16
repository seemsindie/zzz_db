const std = @import("std");
const backend = @import("backend.zig");

pub const c = @cImport({
    @cInclude("sqlite3.h");
});

pub const dialect: backend.Dialect = .sqlite;

pub const Config = struct {
    database: [:0]const u8 = ":memory:",
    busy_timeout_ms: c_int = 5000,
    enable_wal: bool = true,
    pragmas: []const [:0]const u8 = &.{
        "PRAGMA foreign_keys = ON",
    },
};

/// SQLITE_STATIC is defined as ((sqlite3_destructor_type)-1) in C.
// Use SQLITE_STATIC (null) as the destructor — our bound data always outlives step().
// SQLITE_TRANSIENT (-1 cast to function pointer) is problematic in Zig due to alignment
// requirements on function pointers. SQLITE_STATIC works correctly for our usage because
// we always bind values immediately before stepping and never free them in between.
const SQLITE_STATIC: c.sqlite3_destructor_type = null;

pub const SqliteError = error{
    CantOpen,
    Busy,
    Locked,
    Corrupt,
    Constraint,
    Misuse,
    NoMem,
    IoErr,
    Auth,
    Range,
    Schema,
    InternalError,
    PrepareError,
    StepError,
    BindError,
};

pub fn resultToError(rc: c_int) SqliteError {
    return switch (rc) {
        c.SQLITE_CANTOPEN => error.CantOpen,
        c.SQLITE_BUSY => error.Busy,
        c.SQLITE_LOCKED => error.Locked,
        c.SQLITE_CORRUPT, c.SQLITE_NOTADB => error.Corrupt,
        c.SQLITE_CONSTRAINT => error.Constraint,
        c.SQLITE_MISUSE => error.Misuse,
        c.SQLITE_NOMEM => error.NoMem,
        c.SQLITE_IOERR => error.IoErr,
        c.SQLITE_AUTH => error.Auth,
        c.SQLITE_RANGE => error.Range,
        c.SQLITE_SCHEMA => error.Schema,
        else => error.InternalError,
    };
}

pub const Db = struct {
    handle: *c.sqlite3,

    pub fn open(path: [:0]const u8) SqliteError!Db {
        var handle: ?*c.sqlite3 = null;
        const flags = c.SQLITE_OPEN_READWRITE | c.SQLITE_OPEN_CREATE | c.SQLITE_OPEN_NOMUTEX;
        const rc = c.sqlite3_open_v2(path.ptr, &handle, flags, null);
        if (rc != c.SQLITE_OK) {
            if (handle) |h| _ = c.sqlite3_close(h);
            return resultToError(rc);
        }
        return Db{ .handle = handle.? };
    }

    pub fn close(self: *Db) void {
        _ = c.sqlite3_close(self.handle);
    }

    pub fn exec(self: *Db, sql: [:0]const u8) SqliteError!void {
        const rc = c.sqlite3_exec(self.handle, sql.ptr, null, null, null);
        if (rc != c.SQLITE_OK) return resultToError(rc);
    }

    pub fn errmsg(self: *const Db) [*:0]const u8 {
        return c.sqlite3_errmsg(self.handle);
    }

    pub fn lastInsertRowId(self: *const Db) i64 {
        return c.sqlite3_last_insert_rowid(self.handle);
    }

    pub fn changes(self: *const Db) i32 {
        return @intCast(c.sqlite3_changes(self.handle));
    }

    pub fn isAlive(self: *Db) bool {
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(self.handle, "SELECT 1", -1, &stmt, null);
        if (rc != c.SQLITE_OK) return false;
        defer _ = c.sqlite3_finalize(stmt);
        const step_rc = c.sqlite3_step(stmt.?);
        return step_rc == c.SQLITE_ROW;
    }

    pub fn setBusyTimeout(self: *Db, ms: c_int) void {
        _ = c.sqlite3_busy_timeout(self.handle, ms);
    }

    pub fn enableWAL(self: *Db) SqliteError!void {
        try self.exec("PRAGMA journal_mode=WAL");
    }
};

pub const Statement = struct {
    handle: *c.sqlite3_stmt,

    pub fn prepare(db: *Db, sql: [:0]const u8) SqliteError!Statement {
        var stmt: ?*c.sqlite3_stmt = null;
        const rc = c.sqlite3_prepare_v2(db.handle, sql.ptr, @intCast(sql.len), &stmt, null);
        if (rc != c.SQLITE_OK) return error.PrepareError;
        return Statement{ .handle = stmt.? };
    }

    pub fn finalize(self: *Statement) void {
        _ = c.sqlite3_finalize(self.handle);
    }

    pub fn reset(self: *Statement) void {
        _ = c.sqlite3_reset(self.handle);
    }

    /// Step the statement. Returns true if a row is available (SQLITE_ROW),
    /// false when done (SQLITE_DONE).
    pub fn step(self: *Statement) SqliteError!bool {
        const rc = c.sqlite3_step(self.handle);
        if (rc == c.SQLITE_ROW) return true;
        if (rc == c.SQLITE_DONE) return false;
        return error.StepError;
    }

    // ── Bind (1-based index) ───────────────────────────────────────────

    pub fn bindInt64(self: *Statement, col: c_int, value: i64) SqliteError!void {
        const rc = c.sqlite3_bind_int64(self.handle, col, value);
        if (rc != c.SQLITE_OK) return error.BindError;
    }

    pub fn bindDouble(self: *Statement, col: c_int, value: f64) SqliteError!void {
        const rc = c.sqlite3_bind_double(self.handle, col, value);
        if (rc != c.SQLITE_OK) return error.BindError;
    }

    pub fn bindText(self: *Statement, col: c_int, value: []const u8) SqliteError!void {
        const rc = c.sqlite3_bind_text(self.handle, col, value.ptr, @intCast(value.len), SQLITE_STATIC);
        if (rc != c.SQLITE_OK) return error.BindError;
    }

    pub fn bindBlob(self: *Statement, col: c_int, value: []const u8) SqliteError!void {
        const rc = c.sqlite3_bind_blob(self.handle, col, value.ptr, @intCast(value.len), SQLITE_STATIC);
        if (rc != c.SQLITE_OK) return error.BindError;
    }

    pub fn bindNull(self: *Statement, col: c_int) SqliteError!void {
        const rc = c.sqlite3_bind_null(self.handle, col);
        if (rc != c.SQLITE_OK) return error.BindError;
    }

    // ── Read (0-based index) ───────────────────────────────────────────

    pub fn columnCount(self: *const Statement) c_int {
        return c.sqlite3_column_count(self.handle);
    }

    pub fn columnName(self: *const Statement, col: c_int) ?[*:0]const u8 {
        return c.sqlite3_column_name(self.handle, col);
    }

    pub fn columnType(self: *const Statement, col: c_int) c_int {
        return c.sqlite3_column_type(self.handle, col);
    }

    pub fn columnInt64(self: *const Statement, col: c_int) i64 {
        return c.sqlite3_column_int64(self.handle, col);
    }

    pub fn columnDouble(self: *const Statement, col: c_int) f64 {
        return c.sqlite3_column_double(self.handle, col);
    }

    pub fn columnText(self: *const Statement, col: c_int) ?[]const u8 {
        const ptr = c.sqlite3_column_text(self.handle, col);
        if (ptr == null) return null;
        const len = c.sqlite3_column_bytes(self.handle, col);
        if (len <= 0) return "";
        return ptr[0..@intCast(len)];
    }

    pub fn columnBlob(self: *const Statement, col: c_int) ?[]const u8 {
        const ptr: ?[*]const u8 = @ptrCast(c.sqlite3_column_blob(self.handle, col));
        if (ptr == null) return null;
        const len = c.sqlite3_column_bytes(self.handle, col);
        if (len <= 0) return "";
        return ptr.?[0..@intCast(len)];
    }

    pub fn columnIsNull(self: *const Statement, col: c_int) bool {
        return c.sqlite3_column_type(self.handle, col) == c.SQLITE_NULL;
    }
};

// ── Backend Interface Types ────────────────────────────────────────────

pub const ResultSet = struct {
    stmt: Statement,

    pub fn query(db: *Db, sql: [:0]const u8, bind_values: []const ?[]const u8) !ResultSet {
        var stmt = try Statement.prepare(db, sql);
        errdefer stmt.finalize();

        for (bind_values, 0..) |val, i| {
            if (val) |v| {
                try stmt.bindText(@intCast(i + 1), v);
            } else {
                try stmt.bindNull(@intCast(i + 1));
            }
        }

        return .{ .stmt = stmt };
    }

    pub fn next(self: *ResultSet) !bool {
        return self.stmt.step();
    }

    pub fn columnText(self: *const ResultSet, col: c_int) ?[]const u8 {
        return self.stmt.columnText(col);
    }

    pub fn columnInt64(self: *const ResultSet, col: c_int) i64 {
        return self.stmt.columnInt64(col);
    }

    pub fn columnDouble(self: *const ResultSet, col: c_int) f64 {
        return self.stmt.columnDouble(col);
    }

    pub fn columnIsNull(self: *const ResultSet, col: c_int) bool {
        return self.stmt.columnIsNull(col);
    }

    pub fn deinit(self: *ResultSet) void {
        self.stmt.finalize();
    }
};

pub const ExecResult = struct {
    db: *Db,
    rows_affected: i32,
    last_id: i64,

    pub fn exec(db: *Db, sql: [:0]const u8, bind_values: []const ?[]const u8) !ExecResult {
        var stmt = try Statement.prepare(db, sql);
        defer stmt.finalize();

        for (bind_values, 0..) |val, i| {
            if (val) |v| {
                try stmt.bindText(@intCast(i + 1), v);
            } else {
                try stmt.bindNull(@intCast(i + 1));
            }
        }

        _ = try stmt.step();

        return .{
            .db = db,
            .rows_affected = db.changes(),
            .last_id = db.lastInsertRowId(),
        };
    }

    pub fn lastInsertId(self: *const ExecResult) i64 {
        return self.last_id;
    }

    pub fn rowsAffected(self: *const ExecResult) i32 {
        return self.rows_affected;
    }

    pub fn deinit(self: *ExecResult) void {
        _ = self;
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "open and close in-memory db" {
    var db = try Db.open(":memory:");
    defer db.close();
}

test "exec creates table" {
    var db = try Db.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    try db.exec("INSERT INTO test (name) VALUES ('alice')");
}

test "prepare, bind, step, read" {
    var db = try Db.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, score REAL)");
    try db.exec("INSERT INTO test (name, score) VALUES ('alice', 95.5)");

    var stmt = try Statement.prepare(&db, "SELECT id, name, score FROM test WHERE name = ?");
    defer stmt.finalize();

    try stmt.bindText(1, "alice");
    const has_row = try stmt.step();
    try std.testing.expect(has_row);

    const id = stmt.columnInt64(0);
    try std.testing.expectEqual(@as(i64, 1), id);

    const name = stmt.columnText(1);
    try std.testing.expectEqualStrings("alice", name.?);

    const score = stmt.columnDouble(2);
    try std.testing.expectApproxEqAbs(@as(f64, 95.5), score, 0.001);

    const done = try stmt.step();
    try std.testing.expect(!done);
}

test "lastInsertRowId" {
    var db = try Db.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    try db.exec("INSERT INTO test (name) VALUES ('bob')");
    try std.testing.expectEqual(@as(i64, 1), db.lastInsertRowId());
    try db.exec("INSERT INTO test (name) VALUES ('carol')");
    try std.testing.expectEqual(@as(i64, 2), db.lastInsertRowId());
}

test "isAlive" {
    var db = try Db.open(":memory:");
    try std.testing.expect(db.isAlive());
    db.close();
}

test "open invalid path errors" {
    const result = Db.open("/nonexistent/path/db.sqlite");
    try std.testing.expectError(error.CantOpen, result);
}

test "changes count" {
    var db = try Db.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    try db.exec("INSERT INTO test (name) VALUES ('alice')");
    try std.testing.expectEqual(@as(i32, 1), db.changes());
    try db.exec("INSERT INTO test (name) VALUES ('bob')");
    try db.exec("INSERT INTO test (name) VALUES ('carol')");
    try db.exec("DELETE FROM test WHERE name != 'alice'");
    try std.testing.expectEqual(@as(i32, 2), db.changes());
}

test "bind and read null" {
    var db = try Db.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)");

    var insert = try Statement.prepare(&db, "INSERT INTO test (value) VALUES (?)");
    defer insert.finalize();
    try insert.bindNull(1);
    _ = try insert.step();

    var sel = try Statement.prepare(&db, "SELECT value FROM test WHERE id = 1");
    defer sel.finalize();
    _ = try sel.step();
    try std.testing.expect(sel.columnIsNull(0));
}

test "statement reset" {
    var db = try Db.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    try db.exec("INSERT INTO test (name) VALUES ('alice')");
    try db.exec("INSERT INTO test (name) VALUES ('bob')");

    var stmt = try Statement.prepare(&db, "SELECT name FROM test WHERE name = ?");
    defer stmt.finalize();

    try stmt.bindText(1, "alice");
    _ = try stmt.step();
    const name1 = stmt.columnText(1 - 1);
    try std.testing.expectEqualStrings("alice", name1.?);

    stmt.reset();
    try stmt.bindText(1, "bob");
    _ = try stmt.step();
    const name2 = stmt.columnText(0);
    try std.testing.expectEqualStrings("bob", name2.?);
}

test "ResultSet query and iterate" {
    var db = try Db.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, score REAL)");
    try db.exec("INSERT INTO test (name, score) VALUES ('alice', 95.5)");
    try db.exec("INSERT INTO test (name, score) VALUES ('bob', 87.0)");

    const bind_vals: []const ?[]const u8 = &.{};
    var rs = try ResultSet.query(&db, "SELECT id, name, score FROM test ORDER BY id", bind_vals);
    defer rs.deinit();

    try std.testing.expect(try rs.next());
    try std.testing.expectEqual(@as(i64, 1), rs.columnInt64(0));
    try std.testing.expectEqualStrings("alice", rs.columnText(1).?);
    try std.testing.expectApproxEqAbs(@as(f64, 95.5), rs.columnDouble(2), 0.001);

    try std.testing.expect(try rs.next());
    try std.testing.expectEqualStrings("bob", rs.columnText(1).?);

    try std.testing.expect(!(try rs.next()));
}

test "ResultSet with bind values" {
    var db = try Db.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    try db.exec("INSERT INTO test (name) VALUES ('alice')");
    try db.exec("INSERT INTO test (name) VALUES ('bob')");

    const bind_vals: []const ?[]const u8 = &.{"alice"};
    var rs = try ResultSet.query(&db, "SELECT name FROM test WHERE name = ?", bind_vals);
    defer rs.deinit();

    try std.testing.expect(try rs.next());
    try std.testing.expectEqualStrings("alice", rs.columnText(0).?);
    try std.testing.expect(!(try rs.next()));
}

test "ExecResult insert" {
    var db = try Db.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");

    const bind_vals: []const ?[]const u8 = &.{"alice"};
    var er = try ExecResult.exec(&db, "INSERT INTO test (name) VALUES (?)", bind_vals);
    defer er.deinit();

    try std.testing.expectEqual(@as(i64, 1), er.lastInsertId());
    try std.testing.expectEqual(@as(i32, 1), er.rowsAffected());
}

test "ExecResult with null bind" {
    var db = try Db.open(":memory:");
    defer db.close();
    try db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)");

    const bind_vals: []const ?[]const u8 = &.{null};
    var er = try ExecResult.exec(&db, "INSERT INTO test (value) VALUES (?)", bind_vals);
    defer er.deinit();

    try std.testing.expectEqual(@as(i64, 1), er.lastInsertId());
}
