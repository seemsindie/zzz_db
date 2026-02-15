const std = @import("std");
const backend_mod = @import("backend.zig");
const schema_mod = @import("schema.zig");
const query_mod = @import("query.zig");
const pool_mod = @import("pool.zig");
const sqlite = @import("sqlite.zig");

pub fn Repo(comptime Backend: type) type {
    comptime backend_mod.validate(Backend);
    const PoolType = pool_mod.Pool(Backend);

    return struct {
        const Self = @This();

        pool: *PoolType,

        pub fn init(pool: *PoolType) Self {
            return .{ .pool = pool };
        }

        /// Execute a query and return all matching rows as a slice of T.
        pub fn all(self: Self, comptime T: type, q: query_mod.Query(T), allocator: std.mem.Allocator) ![]T {
            const sql_result = try q.toSql(allocator, Backend.dialect);
            defer allocator.free(sql_result.sql);
            defer allocator.free(sql_result.bind_values);

            // Null-terminate the SQL
            const sql_z = try allocator.dupeZ(u8, sql_result.sql);
            defer allocator.free(sql_z);

            var pc = try self.pool.checkout();
            defer pc.release();

            var rs = try Backend.ResultSet.query(&pc.conn.db, sql_z, sql_result.bind_values);
            defer rs.deinit();

            // Collect results
            var results: std.ArrayList(T) = .empty;
            errdefer {
                for (results.items) |*item| {
                    freeStructStrings(T, item, allocator);
                }
                results.deinit(allocator);
            }

            while (try rs.next()) {
                const row = try mapRow(T, Backend, &rs, allocator);
                try results.append(allocator, row);
            }

            return try results.toOwnedSlice(allocator);
        }

        /// Execute a query and return the first matching row, or null.
        pub fn one(self: Self, comptime T: type, q: query_mod.Query(T), allocator: std.mem.Allocator) !?T {
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
        pub fn get(self: Self, comptime T: type, id: i64, allocator: std.mem.Allocator) !?T {
            const M = schema_mod.meta(T);
            const sql = comptime "SELECT " ++ M.columns ++ " FROM " ++ M.Table ++ " WHERE " ++ M.PrimaryKey ++
                (if (Backend.dialect == .postgres) " = $1" else " = ?");
            const sql_z: [:0]const u8 = sql;

            var id_buf: [32]u8 = undefined;
            const id_str = std.fmt.bufPrint(&id_buf, "{d}", .{id}) catch return null;

            var pc = try self.pool.checkout();
            defer pc.release();

            const bind_vals: []const ?[]const u8 = &.{id_str};
            var rs = try Backend.ResultSet.query(&pc.conn.db, sql_z, bind_vals);
            defer rs.deinit();

            if (try rs.next()) {
                return try mapRow(T, Backend, &rs, allocator);
            }
            return null;
        }

        /// Insert a record. Returns the record with the new primary key set.
        pub fn insert(self: Self, comptime T: type, record: T, allocator: std.mem.Allocator) !T {
            const M = schema_mod.meta(T);
            const placeholders = comptime if (Backend.dialect == .postgres) M.insert_placeholders_pg else M.insert_placeholders;
            const sql = comptime "INSERT INTO " ++ M.Table ++ " (" ++ M.insert_columns ++ ") VALUES (" ++ placeholders ++ ")" ++
                (if (Backend.dialect == .postgres) " RETURNING " ++ M.PrimaryKey else "");
            const sql_z: [:0]const u8 = sql;

            // Convert all non-PK fields to text for binding
            const struct_fields = @typeInfo(T).@"struct".fields;
            comptime var insert_field_count: usize = 0;
            comptime {
                for (struct_fields) |f| {
                    if (!std.mem.eql(u8, f.name, M.PrimaryKey)) {
                        insert_field_count += 1;
                    }
                }
            }

            var text_bufs: [insert_field_count][64]u8 = undefined;
            var bind_vals: [insert_field_count]?[]const u8 = undefined;

            comptime var bind_idx: usize = 0;
            inline for (struct_fields) |f| {
                if (!comptime std.mem.eql(u8, f.name, M.PrimaryKey)) {
                    const value = @field(record, f.name);
                    bind_vals[bind_idx] = fieldToText(&text_bufs[bind_idx], f.type, value);
                    bind_idx += 1;
                }
            }

            var pc = try self.pool.checkout();
            defer pc.release();

            var exec_result = try Backend.ExecResult.exec(&pc.conn.db, sql_z, &bind_vals);
            defer exec_result.deinit();

            const new_id = exec_result.lastInsertId();

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
        pub fn update(self: Self, comptime T: type, record: T, allocator: std.mem.Allocator) !T {
            const M = schema_mod.meta(T);

            // Build "SET col1 = ?, col2 = ?, ..." for non-PK fields with dialect-aware placeholders
            const struct_fields = @typeInfo(T).@"struct".fields;
            const set_clause = comptime blk: {
                var buf: []const u8 = "";
                var first = true;
                var idx: usize = 1;
                for (struct_fields) |f| {
                    if (!std.mem.eql(u8, f.name, M.PrimaryKey)) {
                        if (!first) buf = buf ++ ", ";
                        buf = buf ++ f.name ++ if (Backend.dialect == .postgres) (" = $" ++ schema_mod.intToStr(idx)) else " = ?";
                        first = false;
                        idx += 1;
                    }
                }
                break :blk .{ buf, idx };
            };

            const pk_placeholder = comptime if (Backend.dialect == .postgres) " = $" ++ schema_mod.intToStr(set_clause[1]) else " = ?";
            const sql = comptime "UPDATE " ++ M.Table ++ " SET " ++ set_clause[0] ++ " WHERE " ++ M.PrimaryKey ++ pk_placeholder;
            const sql_z: [:0]const u8 = sql;

            // Count non-PK fields + 1 for the WHERE PK bind
            comptime var insert_field_count: usize = 0;
            comptime {
                for (struct_fields) |f| {
                    if (!std.mem.eql(u8, f.name, M.PrimaryKey)) {
                        insert_field_count += 1;
                    }
                }
            }
            const total_binds = insert_field_count + 1;

            var text_bufs: [total_binds][64]u8 = undefined;
            var bind_vals: [total_binds]?[]const u8 = undefined;

            comptime var bind_idx: usize = 0;
            inline for (struct_fields) |f| {
                if (!comptime std.mem.eql(u8, f.name, M.PrimaryKey)) {
                    const value = @field(record, f.name);
                    bind_vals[bind_idx] = fieldToText(&text_bufs[bind_idx], f.type, value);
                    bind_idx += 1;
                }
            }

            // Bind the PK value for the WHERE clause
            const pk_val = @field(record, M.PrimaryKey);
            bind_vals[bind_idx] = fieldToText(&text_bufs[bind_idx], @TypeOf(pk_val), pk_val);

            var pc = try self.pool.checkout();
            defer pc.release();

            var exec_result = try Backend.ExecResult.exec(&pc.conn.db, sql_z, &bind_vals);
            defer exec_result.deinit();

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
        pub fn delete(self: Self, comptime T: type, record: T) !void {
            const M = schema_mod.meta(T);
            const sql = comptime "DELETE FROM " ++ M.Table ++ " WHERE " ++ M.PrimaryKey ++
                (if (Backend.dialect == .postgres) " = $1" else " = ?");
            const sql_z: [:0]const u8 = sql;

            var id_buf: [64]u8 = undefined;
            const pk_val = @field(record, M.PrimaryKey);
            const id_str = fieldToText(&id_buf, @TypeOf(pk_val), pk_val);

            var pc = try self.pool.checkout();
            defer pc.release();

            const bind_vals: []const ?[]const u8 = &.{id_str};
            var exec_result = try Backend.ExecResult.exec(&pc.conn.db, sql_z, bind_vals);
            defer exec_result.deinit();
        }

        /// Check if any records match the query.
        pub fn exists(self: Self, comptime T: type, q: query_mod.Query(T), allocator: std.mem.Allocator) !bool {
            const sql_result = try q.toCountSql(allocator, Backend.dialect);
            defer allocator.free(sql_result.sql);
            defer allocator.free(sql_result.bind_values);

            const sql_z = try allocator.dupeZ(u8, sql_result.sql);
            defer allocator.free(sql_z);

            var pc = try self.pool.checkout();
            defer pc.release();

            var rs = try Backend.ResultSet.query(&pc.conn.db, sql_z, sql_result.bind_values);
            defer rs.deinit();

            if (try rs.next()) {
                return rs.columnInt64(0) > 0;
            }
            return false;
        }
    };
}

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

fn fieldToText(buf: *[64]u8, comptime FieldType: type, value: FieldType) ?[]const u8 {
    const info = @typeInfo(FieldType);
    if (info == .optional) {
        if (value) |v| {
            return fieldToText(buf, info.optional.child, v);
        } else {
            return null;
        }
    }

    if (FieldType == []const u8) {
        return value;
    } else if (FieldType == i64 or FieldType == i32 or FieldType == u32 or FieldType == u64 or FieldType == i16 or FieldType == u16) {
        return std.fmt.bufPrint(buf, "{d}", .{@as(i64, @intCast(value))}) catch return null;
    } else if (FieldType == f64 or FieldType == f32) {
        return std.fmt.bufPrint(buf, "{d}", .{@as(f64, @floatCast(value))}) catch return null;
    } else if (FieldType == bool) {
        return if (value) "1" else "0";
    } else {
        @compileError("Unsupported field type for fieldToText: " ++ @typeName(FieldType));
    }
}

fn readColumn(comptime T: type, comptime Backend: type, rs: *Backend.ResultSet, col: c_int, allocator: std.mem.Allocator) !T {
    const info = @typeInfo(T);
    if (info == .optional) {
        if (rs.columnIsNull(col)) return null;
        return try readColumn(info.optional.child, Backend, rs, col, allocator);
    }

    if (T == []const u8) {
        if (rs.columnText(col)) |text| {
            return try allocator.dupe(u8, text);
        }
        return try allocator.dupe(u8, "");
    } else if (T == i64) {
        return rs.columnInt64(col);
    } else if (T == i32) {
        return @intCast(rs.columnInt64(col));
    } else if (T == f64) {
        return rs.columnDouble(col);
    } else if (T == f32) {
        return @floatCast(rs.columnDouble(col));
    } else if (T == bool) {
        return rs.columnInt64(col) != 0;
    } else {
        @compileError("Unsupported column type: " ++ @typeName(T));
    }
}

fn mapRow(comptime T: type, comptime Backend: type, rs: *Backend.ResultSet, allocator: std.mem.Allocator) !T {
    var result: T = undefined;
    const struct_fields = @typeInfo(T).@"struct".fields;

    inline for (struct_fields, 0..) |f, i| {
        @field(result, f.name) = readColumn(f.type, Backend, rs, @intCast(i), allocator) catch |err| {
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
    pool: pool_mod.Pool(sqlite),

    fn setup() !TestContext {
        var pool = try pool_mod.Pool(sqlite).init(.{ .size = 1 });
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

    fn repo(self: *TestContext) Repo(sqlite) {
        return Repo(sqlite).init(&self.pool);
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
