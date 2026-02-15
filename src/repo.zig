const std = @import("std");
const sqlite = @import("sqlite.zig");
const schema_mod = @import("schema.zig");
const query_mod = @import("query.zig");
const Pool = @import("pool.zig").Pool;
const Connection = @import("connection.zig").Connection;

pub const Repo = struct {
    pool: *Pool,

    pub fn init(pool: *Pool) Repo {
        return .{ .pool = pool };
    }

    /// Execute a query and return all matching rows as a slice of T.
    pub fn all(self: Repo, comptime T: type, q: query_mod.Query(T), allocator: std.mem.Allocator) ![]T {
        const sql_result = try q.toSql(allocator);
        defer allocator.free(sql_result.sql);
        defer allocator.free(sql_result.bind_values);

        // Null-terminate the SQL
        const sql_z = try allocator.dupeZ(u8, sql_result.sql);
        defer allocator.free(sql_z);

        var pc = try self.pool.checkout();
        defer pc.release();

        var stmt = try sqlite.Statement.prepare(&pc.conn.db, sql_z);
        defer stmt.finalize();

        // Bind values
        for (sql_result.bind_values, 0..) |val, i| {
            if (val) |v| {
                try stmt.bindText(@intCast(i + 1), v);
            } else {
                try stmt.bindNull(@intCast(i + 1));
            }
        }

        // Collect results
        var results: std.ArrayList(T) = .empty;
        errdefer {
            for (results.items) |*item| {
                freeStructStrings(T, item, allocator);
            }
            results.deinit(allocator);
        }

        while (try stmt.step()) {
            const row = try mapRow(T, &stmt, allocator);
            try results.append(allocator, row);
        }

        return try results.toOwnedSlice(allocator);
    }

    /// Execute a query and return the first matching row, or null.
    pub fn one(self: Repo, comptime T: type, q: query_mod.Query(T), allocator: std.mem.Allocator) !?T {
        const limited = q.limit(1);
        const results = try self.all(T, limited, allocator);
        defer allocator.free(results);
        if (results.len == 0) return null;
        // We keep the first item's strings alive, free the rest
        if (results.len > 1) {
            for (results[1..]) |*item| {
                freeStructStrings(T, item, allocator);
            }
        }
        return results[0];
    }

    /// Get a single record by primary key.
    pub fn get(self: Repo, comptime T: type, id: i64, allocator: std.mem.Allocator) !?T {
        const M = schema_mod.meta(T);
        const sql = comptime "SELECT " ++ M.columns ++ " FROM " ++ M.Table ++ " WHERE " ++ M.PrimaryKey ++ " = ?";
        const sql_z: [:0]const u8 = sql;

        var pc = try self.pool.checkout();
        defer pc.release();

        var stmt = try sqlite.Statement.prepare(&pc.conn.db, sql_z);
        defer stmt.finalize();

        try stmt.bindInt64(1, id);

        if (try stmt.step()) {
            return try mapRow(T, &stmt, allocator);
        }
        return null;
    }

    /// Insert a record. Returns the record with the new primary key set.
    pub fn insert(self: Repo, comptime T: type, record: T, allocator: std.mem.Allocator) !T {
        const M = schema_mod.meta(T);
        const sql = comptime "INSERT INTO " ++ M.Table ++ " (" ++ M.insert_columns ++ ") VALUES (" ++ M.insert_placeholders ++ ")";
        const sql_z: [:0]const u8 = sql;

        var pc = try self.pool.checkout();
        defer pc.release();

        var stmt = try sqlite.Statement.prepare(&pc.conn.db, sql_z);
        defer stmt.finalize();

        // Bind non-PK fields
        comptime var bind_idx: c_int = 1;
        const struct_fields = @typeInfo(T).@"struct".fields;
        inline for (struct_fields) |f| {
            if (!comptime std.mem.eql(u8, f.name, M.PrimaryKey)) {
                const value = @field(record, f.name);
                try bindField(&stmt, bind_idx, f.type, value);
                bind_idx += 1;
            }
        }

        _ = try stmt.step();

        const new_id = pc.conn.lastInsertRowId();

        // Return the record with the new ID
        var result = record;
        @field(result, M.PrimaryKey) = new_id;
        // Dupe any string fields so caller owns them
        inline for (struct_fields) |f| {
            if (comptime isStringType(f.type)) {
                const val = @field(record, f.name);
                @field(result, f.name) = try allocator.dupe(u8, val);
            }
        }
        return result;
    }

    /// Update a record by primary key.
    pub fn update(self: Repo, comptime T: type, record: T, allocator: std.mem.Allocator) !T {
        const M = schema_mod.meta(T);

        // Build "SET col1 = ?, col2 = ?, ..." for non-PK fields
        const set_clause = comptime blk: {
            var buf: []const u8 = "";
            var first = true;
            for (@typeInfo(T).@"struct".fields) |f| {
                if (!std.mem.eql(u8, f.name, M.PrimaryKey)) {
                    if (!first) buf = buf ++ ", ";
                    buf = buf ++ f.name ++ " = ?";
                    first = false;
                }
            }
            break :blk buf;
        };

        const sql = comptime "UPDATE " ++ M.Table ++ " SET " ++ set_clause ++ " WHERE " ++ M.PrimaryKey ++ " = ?";
        const sql_z: [:0]const u8 = sql;

        var pc = try self.pool.checkout();
        defer pc.release();

        var stmt = try sqlite.Statement.prepare(&pc.conn.db, sql_z);
        defer stmt.finalize();

        // Bind SET values
        comptime var bind_idx: c_int = 1;
        const struct_fields = @typeInfo(T).@"struct".fields;
        inline for (struct_fields) |f| {
            if (!comptime std.mem.eql(u8, f.name, M.PrimaryKey)) {
                const value = @field(record, f.name);
                try bindField(&stmt, bind_idx, f.type, value);
                bind_idx += 1;
            }
        }

        // Bind WHERE pk = ?
        try stmt.bindInt64(bind_idx, @field(record, M.PrimaryKey));

        _ = try stmt.step();

        // Return the record with duped strings
        var result = record;
        inline for (struct_fields) |f| {
            if (comptime isStringType(f.type)) {
                @field(result, f.name) = try allocator.dupe(u8, @field(record, f.name));
            }
        }
        return result;
    }

    /// Delete a record by primary key.
    pub fn delete(self: Repo, comptime T: type, record: T) !void {
        const M = schema_mod.meta(T);
        const sql = comptime "DELETE FROM " ++ M.Table ++ " WHERE " ++ M.PrimaryKey ++ " = ?";
        const sql_z: [:0]const u8 = sql;

        var pc = try self.pool.checkout();
        defer pc.release();

        var stmt = try sqlite.Statement.prepare(&pc.conn.db, sql_z);
        defer stmt.finalize();

        try stmt.bindInt64(1, @field(record, M.PrimaryKey));
        _ = try stmt.step();
    }

    /// Check if any records match the query.
    pub fn exists(self: Repo, comptime T: type, q: query_mod.Query(T), allocator: std.mem.Allocator) !bool {
        const sql_result = try q.toCountSql(allocator);
        defer allocator.free(sql_result.sql);
        defer allocator.free(sql_result.bind_values);

        const sql_z = try allocator.dupeZ(u8, sql_result.sql);
        defer allocator.free(sql_z);

        var pc = try self.pool.checkout();
        defer pc.release();

        var stmt = try sqlite.Statement.prepare(&pc.conn.db, sql_z);
        defer stmt.finalize();

        for (sql_result.bind_values, 0..) |val, i| {
            if (val) |v| {
                try stmt.bindText(@intCast(i + 1), v);
            } else {
                try stmt.bindNull(@intCast(i + 1));
            }
        }

        if (try stmt.step()) {
            return stmt.columnInt64(0) > 0;
        }
        return false;
    }
};

// ── Internal helpers ───────────────────────────────────────────────────

fn isStringType(comptime T: type) bool {
    return T == []const u8;
}

fn isOptionalString(comptime T: type) bool {
    const info = @typeInfo(T);
    if (info == .optional) {
        return isStringType(info.optional.child);
    }
    return false;
}

fn bindField(stmt: *sqlite.Statement, col: c_int, comptime FieldType: type, value: FieldType) !void {
    const info = @typeInfo(FieldType);
    if (info == .optional) {
        if (value) |v| {
            try bindField(stmt, col, info.optional.child, v);
        } else {
            try stmt.bindNull(col);
        }
        return;
    }

    if (FieldType == []const u8) {
        try stmt.bindText(col, value);
    } else if (FieldType == i64 or FieldType == i32 or FieldType == u32 or FieldType == u64 or FieldType == i16 or FieldType == u16) {
        try stmt.bindInt64(col, @intCast(value));
    } else if (FieldType == f64 or FieldType == f32) {
        try stmt.bindDouble(col, @floatCast(value));
    } else if (FieldType == bool) {
        try stmt.bindInt64(col, if (value) 1 else 0);
    } else {
        @compileError("Unsupported field type for binding: " ++ @typeName(FieldType));
    }
}

fn readColumn(comptime T: type, stmt: *sqlite.Statement, col: c_int, allocator: std.mem.Allocator) !T {
    const info = @typeInfo(T);
    if (info == .optional) {
        if (stmt.columnIsNull(col)) return null;
        return try readColumn(info.optional.child, stmt, col, allocator);
    }

    if (T == []const u8) {
        if (stmt.columnText(col)) |text| {
            return try allocator.dupe(u8, text);
        }
        return try allocator.dupe(u8, "");
    } else if (T == i64) {
        return stmt.columnInt64(col);
    } else if (T == i32) {
        return @intCast(stmt.columnInt64(col));
    } else if (T == f64) {
        return stmt.columnDouble(col);
    } else if (T == f32) {
        return @floatCast(stmt.columnDouble(col));
    } else if (T == bool) {
        return stmt.columnInt64(col) != 0;
    } else {
        @compileError("Unsupported column type: " ++ @typeName(T));
    }
}

fn mapRow(comptime T: type, stmt: *sqlite.Statement, allocator: std.mem.Allocator) !T {
    var result: T = undefined;
    const struct_fields = @typeInfo(T).@"struct".fields;

    inline for (struct_fields, 0..) |f, i| {
        @field(result, f.name) = readColumn(f.type, stmt, @intCast(i), allocator) catch |err| {
            // Clean up any previously allocated strings
            inline for (struct_fields, 0..) |f2, j| {
                if (j < i and comptime (isStringType(f2.type) or isOptionalString(f2.type))) {
                    if (comptime isOptionalString(f2.type)) {
                        if (@field(result, f2.name)) |s| allocator.free(s);
                    } else if (comptime isStringType(f2.type)) {
                        allocator.free(@field(result, f2.name));
                    }
                }
            }
            return err;
        };
    }

    return result;
}

fn freeStructStrings(comptime T: type, item: *T, allocator: std.mem.Allocator) void {
    const struct_fields = @typeInfo(T).@"struct".fields;
    inline for (struct_fields) |f| {
        if (comptime isOptionalString(f.type)) {
            if (@field(item, f.name)) |s| allocator.free(s);
        } else if (comptime isStringType(f.type)) {
            allocator.free(@field(item, f.name));
        }
    }
}

/// Free a slice of structs returned by Repo.all, freeing all owned string fields.
pub fn freeAll(comptime T: type, items: []T, allocator: std.mem.Allocator) void {
    for (items) |*item| {
        freeStructStrings(T, item, allocator);
    }
    allocator.free(items);
}

/// Free a single struct returned by Repo.get/one/insert/update.
pub fn freeOne(comptime T: type, item: *T, allocator: std.mem.Allocator) void {
    freeStructStrings(T, item, allocator);
}

// ── Tests ──────────────────────────────────────────────────────────────

const TestUser = struct {
    id: i64,
    name: []const u8,
    email: []const u8,
    inserted_at: i64 = 0,
    updated_at: i64 = 0,

    pub const Meta = schema_mod.define(@This(), .{
        .table = "users",
        .primary_key = "id",
        .timestamps = true,
    });
};

const TestContext = struct {
    pool: Pool,

    fn setup() !TestContext {
        var pool = try Pool.init(.{ .size = 1 });
        errdefer pool.deinit();

        // Create table
        var pc = try pool.checkout();
        try pc.conn.exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, inserted_at BIGINT, updated_at BIGINT)");
        pc.release();

        return .{ .pool = pool };
    }

    fn deinit(self: *TestContext) void {
        self.pool.deinit();
    }

    fn repo(self: *TestContext) Repo {
        return Repo.init(&self.pool);
    }
};

test "insert and get" {
    var ctx = try TestContext.setup();
    defer ctx.deinit();
    var r = ctx.repo();

    var user = try r.insert(TestUser, .{
        .id = 0,
        .name = "Alice",
        .email = "alice@example.com",
    }, std.testing.allocator);
    defer freeOne(TestUser, &user, std.testing.allocator);

    try std.testing.expect(user.id > 0);
    try std.testing.expectEqualStrings("Alice", user.name);

    var fetched = (try r.get(TestUser, user.id, std.testing.allocator)).?;
    defer freeOne(TestUser, &fetched, std.testing.allocator);

    try std.testing.expectEqualStrings("Alice", fetched.name);
    try std.testing.expectEqualStrings("alice@example.com", fetched.email);
}

test "all returns slice" {
    var ctx = try TestContext.setup();
    defer ctx.deinit();
    var r = ctx.repo();

    var ins1 = try r.insert(TestUser, .{ .id = 0, .name = "Alice", .email = "a@e.com" }, std.testing.allocator);
    defer freeOne(TestUser, &ins1, std.testing.allocator);
    var ins2 = try r.insert(TestUser, .{ .id = 0, .name = "Bob", .email = "b@e.com" }, std.testing.allocator);
    defer freeOne(TestUser, &ins2, std.testing.allocator);

    const q = query_mod.Query(TestUser).init();
    const users = try r.all(TestUser, q, std.testing.allocator);
    defer freeAll(TestUser, users, std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), users.len);
}

test "one returns optional" {
    var ctx = try TestContext.setup();
    defer ctx.deinit();
    var r = ctx.repo();

    var ins1 = try r.insert(TestUser, .{ .id = 0, .name = "Alice", .email = "a@e.com" }, std.testing.allocator);
    defer freeOne(TestUser, &ins1, std.testing.allocator);

    const q = query_mod.Query(TestUser).init().where("name", .eq, "Alice");
    var user = (try r.one(TestUser, q, std.testing.allocator)).?;
    defer freeOne(TestUser, &user, std.testing.allocator);

    try std.testing.expectEqualStrings("Alice", user.name);
}

test "one returns null for no match" {
    var ctx = try TestContext.setup();
    defer ctx.deinit();
    var r = ctx.repo();

    const q = query_mod.Query(TestUser).init().where("name", .eq, "NonExistent");
    const user = try r.one(TestUser, q, std.testing.allocator);
    try std.testing.expect(user == null);
}

test "update" {
    var ctx = try TestContext.setup();
    defer ctx.deinit();
    var r = ctx.repo();

    var user = try r.insert(TestUser, .{ .id = 0, .name = "Alice", .email = "a@e.com" }, std.testing.allocator);
    defer freeOne(TestUser, &user, std.testing.allocator);

    var updated = try r.update(TestUser, .{
        .id = user.id,
        .name = "Alice Updated",
        .email = "new@e.com",
        .inserted_at = user.inserted_at,
        .updated_at = user.updated_at,
    }, std.testing.allocator);
    defer freeOne(TestUser, &updated, std.testing.allocator);

    var fetched = (try r.get(TestUser, user.id, std.testing.allocator)).?;
    defer freeOne(TestUser, &fetched, std.testing.allocator);

    try std.testing.expectEqualStrings("Alice Updated", fetched.name);
    try std.testing.expectEqualStrings("new@e.com", fetched.email);
}

test "delete" {
    var ctx = try TestContext.setup();
    defer ctx.deinit();
    var r = ctx.repo();

    var user = try r.insert(TestUser, .{ .id = 0, .name = "ToDelete", .email = "d@e.com" }, std.testing.allocator);
    defer freeOne(TestUser, &user, std.testing.allocator);

    try r.delete(TestUser, user);

    const fetched = try r.get(TestUser, user.id, std.testing.allocator);
    try std.testing.expect(fetched == null);
}

test "exists" {
    var ctx = try TestContext.setup();
    defer ctx.deinit();
    var r = ctx.repo();

    var ins1 = try r.insert(TestUser, .{ .id = 0, .name = "Alice", .email = "a@e.com" }, std.testing.allocator);
    defer freeOne(TestUser, &ins1, std.testing.allocator);

    const q1 = query_mod.Query(TestUser).init().where("name", .eq, "Alice");
    try std.testing.expect(try r.exists(TestUser, q1, std.testing.allocator));

    const q2 = query_mod.Query(TestUser).init().where("name", .eq, "NonExistent");
    try std.testing.expect(!(try r.exists(TestUser, q2, std.testing.allocator)));
}

test "all with WHERE" {
    var ctx = try TestContext.setup();
    defer ctx.deinit();
    var r = ctx.repo();

    var ins1 = try r.insert(TestUser, .{ .id = 0, .name = "Alice", .email = "a@e.com" }, std.testing.allocator);
    defer freeOne(TestUser, &ins1, std.testing.allocator);
    var ins2 = try r.insert(TestUser, .{ .id = 0, .name = "Bob", .email = "b@e.com" }, std.testing.allocator);
    defer freeOne(TestUser, &ins2, std.testing.allocator);

    const q = query_mod.Query(TestUser).init().where("name", .eq, "Alice");
    const users = try r.all(TestUser, q, std.testing.allocator);
    defer freeAll(TestUser, users, std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), users.len);
    try std.testing.expectEqualStrings("Alice", users[0].name);
}

test "get null for missing ID" {
    var ctx = try TestContext.setup();
    defer ctx.deinit();
    var r = ctx.repo();

    const fetched = try r.get(TestUser, 99999, std.testing.allocator);
    try std.testing.expect(fetched == null);
}
