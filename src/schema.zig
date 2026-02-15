const std = @import("std");
const backend = @import("backend.zig");
pub const Dialect = backend.Dialect;

pub fn intToStr(comptime n: usize) []const u8 {
    if (n == 0) return "0";
    // Decompose digits manually — comptime requires inline loops
    const d0: u8 = @intCast(n % 10);
    const r0 = n / 10;
    if (r0 == 0) return &[_]u8{'0' + d0};
    const d1: u8 = @intCast(r0 % 10);
    const r1 = r0 / 10;
    if (r1 == 0) return &[_]u8{ '0' + d1, '0' + d0 };
    const d2: u8 = @intCast(r1 % 10);
    const r2 = r1 / 10;
    if (r2 == 0) return &[_]u8{ '0' + d2, '0' + d1, '0' + d0 };
    const d3: u8 = @intCast(r2 % 10);
    return &[_]u8{ '0' + d3, '0' + d2, '0' + d1, '0' + d0 };
}

pub const SqlType = enum {
    integer,
    bigint,
    real,
    text,
    boolean,
    blob,
};

pub const FieldInfo = struct {
    name: []const u8,
    sql_type: SqlType,
    nullable: bool,
    is_pk: bool,
    is_timestamp: bool,
};

pub const SchemaOpts = struct {
    table: [:0]const u8,
    primary_key: []const u8 = "id",
    timestamps: bool = true,
};

pub fn zigTypeToSql(comptime T: type) SqlType {
    const info = @typeInfo(T);
    switch (info) {
        .optional => |opt| return zigTypeToSql(opt.child),
        .int => |int_info| {
            if (int_info.bits <= 32) return .integer;
            return .bigint;
        },
        .float => return .real,
        .bool => return .boolean,
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) return .text;
            return .blob;
        },
        else => return .blob,
    }
}

fn sqlTypeStr(comptime st: SqlType) []const u8 {
    return switch (st) {
        .integer => "INTEGER",
        .bigint => "BIGINT",
        .real => "REAL",
        .text => "TEXT",
        .boolean => "BOOLEAN",
        .blob => "BLOB",
    };
}

fn sqlTypeStrPg(comptime st: SqlType) []const u8 {
    return switch (st) {
        .integer => "INTEGER",
        .bigint => "BIGINT",
        .real => "DOUBLE PRECISION",
        .text => "TEXT",
        .boolean => "BOOLEAN",
        .blob => "BYTEA",
    };
}

pub fn define(comptime T: type, comptime opts: SchemaOpts) type {
    const struct_info = @typeInfo(T).@"struct";
    const struct_fields = struct_info.fields;
    const N = struct_fields.len;

    // Build field info array
    comptime var fields: [N]FieldInfo = undefined;
    comptime var field_names_arr: [N][]const u8 = undefined;
    comptime var columns_parts: [N][]const u8 = undefined;
    comptime var insert_cols_count: usize = 0;
    comptime var insert_col_parts: [N][]const u8 = undefined;
    comptime var insert_placeholder_parts: [N][]const u8 = undefined;

    comptime {
        for (struct_fields, 0..) |f, i| {
            const is_pk = std.mem.eql(u8, f.name, opts.primary_key);
            const is_ts = opts.timestamps and (std.mem.eql(u8, f.name, "inserted_at") or std.mem.eql(u8, f.name, "updated_at"));
            const is_nullable = @typeInfo(f.type) == .optional;
            const sql_t = zigTypeToSql(f.type);

            fields[i] = .{
                .name = f.name,
                .sql_type = sql_t,
                .nullable = is_nullable,
                .is_pk = is_pk,
                .is_timestamp = is_ts,
            };

            field_names_arr[i] = f.name;
            columns_parts[i] = f.name;

            if (!is_pk) {
                insert_col_parts[insert_cols_count] = f.name;
                insert_placeholder_parts[insert_cols_count] = "?";
                insert_cols_count += 1;
            }
        }
    }

    // Build columns_sql: "id, name, email, inserted_at, updated_at"
    const columns_sql = comptime blk: {
        var buf: []const u8 = "";
        for (columns_parts, 0..) |part, i| {
            if (i > 0) buf = buf ++ ", ";
            buf = buf ++ part;
        }
        break :blk buf;
    };

    // Build insert_columns_sql: "name, email, inserted_at, updated_at"
    const insert_columns_sql = comptime blk: {
        var buf: []const u8 = "";
        for (0..insert_cols_count) |i| {
            if (i > 0) buf = buf ++ ", ";
            buf = buf ++ insert_col_parts[i];
        }
        break :blk buf;
    };

    // Build insert_placeholders_sql: "?, ?, ?, ?" (SQLite)
    const insert_placeholders_sql = comptime blk: {
        var buf: []const u8 = "";
        for (0..insert_cols_count) |i| {
            if (i > 0) buf = buf ++ ", ";
            buf = buf ++ "?";
        }
        break :blk buf;
    };

    // Build insert_placeholders_pg_sql: "$1, $2, $3, $4" (PostgreSQL)
    const insert_placeholders_pg_sql = comptime blk: {
        var buf: []const u8 = "";
        for (0..insert_cols_count) |i| {
            if (i > 0) buf = buf ++ ", ";
            buf = buf ++ "$" ++ intToStr(i + 1);
        }
        break :blk buf;
    };

    // Build CREATE TABLE SQL (SQLite)
    // Note: SQLite requires INTEGER (not BIGINT) for PRIMARY KEY AUTOINCREMENT.
    // SQLite's INTEGER is always 64-bit so this is correct for i64 PKs.
    const create_table_sql: [:0]const u8 = comptime blk: {
        var sql: []const u8 = "CREATE TABLE IF NOT EXISTS " ++ opts.table ++ " (";
        for (struct_fields, 0..) |f, i| {
            if (i > 0) sql = sql ++ ", ";
            const is_pk = std.mem.eql(u8, f.name, opts.primary_key);
            const sql_t = zigTypeToSql(f.type);
            if (is_pk and (sql_t == .integer or sql_t == .bigint)) {
                sql = sql ++ f.name ++ " INTEGER PRIMARY KEY AUTOINCREMENT";
            } else {
                sql = sql ++ f.name ++ " " ++ sqlTypeStr(sql_t);
                if (is_pk) sql = sql ++ " PRIMARY KEY";
            }
        }
        sql = sql ++ ")";
        break :blk sql ++ "";
    };

    // Build CREATE TABLE SQL (PostgreSQL)
    const create_table_pg_sql: [:0]const u8 = comptime blk: {
        var sql: []const u8 = "CREATE TABLE IF NOT EXISTS " ++ opts.table ++ " (";
        for (struct_fields, 0..) |f, i| {
            if (i > 0) sql = sql ++ ", ";
            const is_pk = std.mem.eql(u8, f.name, opts.primary_key);
            const sql_t = zigTypeToSql(f.type);
            if (is_pk and (sql_t == .integer or sql_t == .bigint)) {
                sql = sql ++ f.name ++ " BIGSERIAL PRIMARY KEY";
            } else {
                sql = sql ++ f.name ++ " " ++ sqlTypeStrPg(sql_t);
                if (is_pk) sql = sql ++ " PRIMARY KEY";
            }
        }
        sql = sql ++ ")";
        break :blk sql ++ "";
    };

    return struct {
        pub const Table: [:0]const u8 = opts.table;
        pub const PrimaryKey: []const u8 = opts.primary_key;
        pub const Timestamps: bool = opts.timestamps;
        pub const Struct = T;
        pub const field_count: usize = N;
        pub const field_info: [N]FieldInfo = fields;
        pub const field_names: [N][]const u8 = field_names_arr;
        pub const columns: []const u8 = columns_sql;
        pub const insert_columns: []const u8 = insert_columns_sql;
        pub const insert_placeholders: []const u8 = insert_placeholders_sql;
        pub const insert_placeholders_pg: []const u8 = insert_placeholders_pg_sql;
        pub const insert_field_count: usize = insert_cols_count;
        pub const create_table: [:0]const u8 = create_table_sql;
        pub const create_table_pg: [:0]const u8 = create_table_pg_sql;

        pub fn createTable(comptime d: Dialect) [:0]const u8 {
            return switch (d) {
                .sqlite => create_table_sql,
                .postgres => create_table_pg_sql,
            };
        }
    };
}

/// Retrieve the Meta type from a schema struct. Compile error if missing.
pub fn meta(comptime T: type) type {
    if (!@hasDecl(T, "Meta")) {
        @compileError("Type " ++ @typeName(T) ++ " does not have a Meta declaration. Use Schema.define() to create one.");
    }
    return T.Meta;
}

// ── Tests ──────────────────────────────────────────────────────────────

const TestUser = struct {
    id: i64,
    name: []const u8,
    email: []const u8,
    inserted_at: i64 = 0,
    updated_at: i64 = 0,

    pub const Meta = define(@This(), .{
        .table = "users",
        .primary_key = "id",
        .timestamps = true,
    });
};

test "zigTypeToSql mapping" {
    try std.testing.expectEqual(SqlType.bigint, zigTypeToSql(i64));
    try std.testing.expectEqual(SqlType.integer, zigTypeToSql(i32));
    try std.testing.expectEqual(SqlType.real, zigTypeToSql(f64));
    try std.testing.expectEqual(SqlType.text, zigTypeToSql([]const u8));
    try std.testing.expectEqual(SqlType.boolean, zigTypeToSql(bool));
    // Optional unwraps to inner type
    try std.testing.expectEqual(SqlType.text, zigTypeToSql(?[]const u8));
}

test "field metadata" {
    const M = TestUser.Meta;
    try std.testing.expectEqual(@as(usize, 5), M.field_count);

    // id is primary key
    try std.testing.expect(M.field_info[0].is_pk);
    try std.testing.expectEqualStrings("id", M.field_info[0].name);

    // name is text, not pk
    try std.testing.expectEqual(SqlType.text, M.field_info[1].sql_type);
    try std.testing.expect(!M.field_info[1].is_pk);

    // timestamps
    try std.testing.expect(M.field_info[3].is_timestamp);
    try std.testing.expect(M.field_info[4].is_timestamp);
}

test "CREATE TABLE SQL" {
    const M = TestUser.Meta;
    const expected = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, inserted_at BIGINT, updated_at BIGINT)";
    try std.testing.expectEqualStrings(expected, M.create_table);
}

test "SELECT columns" {
    const M = TestUser.Meta;
    try std.testing.expectEqualStrings("id, name, email, inserted_at, updated_at", M.columns);
}

test "INSERT columns" {
    const M = TestUser.Meta;
    try std.testing.expectEqualStrings("name, email, inserted_at, updated_at", M.insert_columns);
    try std.testing.expectEqualStrings("?, ?, ?, ?", M.insert_placeholders);
}

test "meta helper" {
    const M = meta(TestUser);
    try std.testing.expectEqualStrings("users", M.Table);
}

test "CREATE TABLE SQL (postgres)" {
    const M = TestUser.Meta;
    const expected = "CREATE TABLE IF NOT EXISTS users (id BIGSERIAL PRIMARY KEY, name TEXT, email TEXT, inserted_at BIGINT, updated_at BIGINT)";
    try std.testing.expectEqualStrings(expected, M.create_table_pg);
}

test "INSERT placeholders (postgres)" {
    const M = TestUser.Meta;
    try std.testing.expectEqualStrings("$1, $2, $3, $4", M.insert_placeholders_pg);
}

test "createTable dispatches by dialect" {
    const M = TestUser.Meta;
    try std.testing.expectEqualStrings(M.create_table, M.createTable(.sqlite));
    try std.testing.expectEqualStrings(M.create_table_pg, M.createTable(.postgres));
}

test "intToStr" {
    try std.testing.expectEqualStrings("1", intToStr(1));
    try std.testing.expectEqualStrings("10", intToStr(10));
    try std.testing.expectEqualStrings("42", intToStr(42));
    try std.testing.expectEqualStrings("0", intToStr(0));
}
