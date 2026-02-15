const std = @import("std");
const schema_mod = @import("schema.zig");

pub const Op = enum {
    eq,
    neq,
    gt,
    lt,
    gte,
    lte,
    like,
    is_null,
    is_not_null,
};

pub const Order = enum {
    asc,
    desc,
};

const WhereClause = struct {
    field: []const u8 = "",
    op: Op = .eq,
    value: ?[]const u8 = null,
    is_or: bool = false,
    active: bool = false,
};

const OrderClause = struct {
    field: []const u8 = "",
    direction: Order = .asc,
    active: bool = false,
};

pub fn Query(comptime T: type) type {
    const M = schema_mod.meta(T);

    return struct {
        const Self = @This();
        const max_where = 16;
        const max_order = 4;

        where_clauses: [max_where]WhereClause = [_]WhereClause{.{}} ** max_where,
        where_count: usize = 0,
        order_clauses: [max_order]OrderClause = [_]OrderClause{.{}} ** max_order,
        order_count: usize = 0,
        limit_val: ?u32 = null,
        offset_val: ?u32 = null,
        select_fields: ?[]const u8 = null,
        raw_fragment: ?[]const u8 = null,

        pub fn init() Self {
            return .{};
        }

        pub fn select(self: Self, fields: []const u8) Self {
            var q = self;
            q.select_fields = fields;
            return q;
        }

        pub fn where(self: Self, field: []const u8, op: Op, value: ?[]const u8) Self {
            var q = self;
            if (q.where_count < max_where) {
                q.where_clauses[q.where_count] = .{
                    .field = field,
                    .op = op,
                    .value = value,
                    .is_or = false,
                    .active = true,
                };
                q.where_count += 1;
            }
            return q;
        }

        pub fn orWhere(self: Self, field: []const u8, op: Op, value: ?[]const u8) Self {
            var q = self;
            if (q.where_count < max_where) {
                q.where_clauses[q.where_count] = .{
                    .field = field,
                    .op = op,
                    .value = value,
                    .is_or = true,
                    .active = true,
                };
                q.where_count += 1;
            }
            return q;
        }

        pub fn orderBy(self: Self, field: []const u8, direction: Order) Self {
            var q = self;
            if (q.order_count < max_order) {
                q.order_clauses[q.order_count] = .{
                    .field = field,
                    .direction = direction,
                    .active = true,
                };
                q.order_count += 1;
            }
            return q;
        }

        pub fn limit(self: Self, n: u32) Self {
            var q = self;
            q.limit_val = n;
            return q;
        }

        pub fn offset(self: Self, n: u32) Self {
            var q = self;
            q.offset_val = n;
            return q;
        }

        pub fn raw(self: Self, fragment: []const u8) Self {
            var q = self;
            q.raw_fragment = fragment;
            return q;
        }

        pub const SqlResult = struct {
            sql: []u8,
            bind_values: []const ?[]const u8,
        };

        pub fn toSql(self: Self, allocator: std.mem.Allocator) !SqlResult {
            var parts: std.ArrayList(u8) = .empty;
            errdefer parts.deinit(allocator);

            var bind_list: std.ArrayList(?[]const u8) = .empty;
            errdefer bind_list.deinit(allocator);

            // SELECT
            try parts.appendSlice(allocator, "SELECT ");
            if (self.select_fields) |fields| {
                try parts.appendSlice(allocator, fields);
            } else {
                try parts.appendSlice(allocator, M.columns);
            }
            try parts.appendSlice(allocator, " FROM ");
            try parts.appendSlice(allocator, M.Table);

            // WHERE
            try self.appendWhere(&parts, &bind_list, allocator);

            // ORDER BY
            if (self.order_count > 0) {
                try parts.appendSlice(allocator, " ORDER BY ");
                for (0..self.order_count) |i| {
                    if (i > 0) try parts.appendSlice(allocator, ", ");
                    try parts.appendSlice(allocator, self.order_clauses[i].field);
                    try parts.appendSlice(allocator, if (self.order_clauses[i].direction == .asc) " ASC" else " DESC");
                }
            }

            // LIMIT / OFFSET
            if (self.limit_val) |lim| {
                try parts.appendSlice(allocator, " LIMIT ");
                var buf: [16]u8 = undefined;
                const s = std.fmt.bufPrint(&buf, "{d}", .{lim}) catch "0";
                try parts.appendSlice(allocator, s);
            }
            if (self.offset_val) |off| {
                try parts.appendSlice(allocator, " OFFSET ");
                var buf: [16]u8 = undefined;
                const s = std.fmt.bufPrint(&buf, "{d}", .{off}) catch "0";
                try parts.appendSlice(allocator, s);
            }

            // Raw fragment
            if (self.raw_fragment) |frag| {
                try parts.appendSlice(allocator, " ");
                try parts.appendSlice(allocator, frag);
            }

            return .{
                .sql = try parts.toOwnedSlice(allocator),
                .bind_values = try bind_list.toOwnedSlice(allocator),
            };
        }

        pub fn toCountSql(self: Self, allocator: std.mem.Allocator) !SqlResult {
            var parts: std.ArrayList(u8) = .empty;
            errdefer parts.deinit(allocator);

            var bind_list: std.ArrayList(?[]const u8) = .empty;
            errdefer bind_list.deinit(allocator);

            try parts.appendSlice(allocator, "SELECT COUNT(*) FROM ");
            try parts.appendSlice(allocator, M.Table);

            // WHERE
            try self.appendWhere(&parts, &bind_list, allocator);

            return .{
                .sql = try parts.toOwnedSlice(allocator),
                .bind_values = try bind_list.toOwnedSlice(allocator),
            };
        }

        fn appendWhere(self: Self, parts: *std.ArrayList(u8), bind_list: *std.ArrayList(?[]const u8), allocator: std.mem.Allocator) !void {
            if (self.where_count > 0) {
                try parts.appendSlice(allocator, " WHERE ");
                for (0..self.where_count) |i| {
                    const wc = self.where_clauses[i];
                    if (i > 0) {
                        try parts.appendSlice(allocator, if (wc.is_or) " OR " else " AND ");
                    }
                    try parts.appendSlice(allocator, wc.field);
                    switch (wc.op) {
                        .eq => try parts.appendSlice(allocator, " = ?"),
                        .neq => try parts.appendSlice(allocator, " != ?"),
                        .gt => try parts.appendSlice(allocator, " > ?"),
                        .lt => try parts.appendSlice(allocator, " < ?"),
                        .gte => try parts.appendSlice(allocator, " >= ?"),
                        .lte => try parts.appendSlice(allocator, " <= ?"),
                        .like => try parts.appendSlice(allocator, " LIKE ?"),
                        .is_null => {
                            try parts.appendSlice(allocator, " IS NULL");
                            continue;
                        },
                        .is_not_null => {
                            try parts.appendSlice(allocator, " IS NOT NULL");
                            continue;
                        },
                    }
                    try bind_list.append(allocator, wc.value);
                }
            }
        }
    };
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

test "SELECT all" {
    const q = Query(TestUser).init();
    const result = try q.toSql(std.testing.allocator);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users", result.sql);
    try std.testing.expectEqual(@as(usize, 0), result.bind_values.len);
}

test "WHERE clause" {
    const q = Query(TestUser).init().where("name", .eq, "alice");
    const result = try q.toSql(std.testing.allocator);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users WHERE name = ?", result.sql);
    try std.testing.expectEqual(@as(usize, 1), result.bind_values.len);
    try std.testing.expectEqualStrings("alice", result.bind_values[0].?);
}

test "AND/OR composition" {
    const q = Query(TestUser).init()
        .where("name", .eq, "alice")
        .orWhere("name", .eq, "bob");
    const result = try q.toSql(std.testing.allocator);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users WHERE name = ? OR name = ?", result.sql);
    try std.testing.expectEqual(@as(usize, 2), result.bind_values.len);
}

test "ORDER BY" {
    const q = Query(TestUser).init().orderBy("name", .asc);
    const result = try q.toSql(std.testing.allocator);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users ORDER BY name ASC", result.sql);
}

test "LIMIT and OFFSET" {
    const q = Query(TestUser).init().limit(10).offset(20);
    const result = try q.toSql(std.testing.allocator);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users LIMIT 10 OFFSET 20", result.sql);
}

test "combined query" {
    const q = Query(TestUser).init()
        .where("email", .like, "%@example.com")
        .orderBy("name", .desc)
        .limit(5);
    const result = try q.toSql(std.testing.allocator);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users WHERE email LIKE ? ORDER BY name DESC LIMIT 5", result.sql);
}

test "toCountSql" {
    const q = Query(TestUser).init().where("name", .eq, "alice");
    const result = try q.toCountSql(std.testing.allocator);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT COUNT(*) FROM users WHERE name = ?", result.sql);
}

test "select fields" {
    const q = Query(TestUser).init().select("name, email");
    const result = try q.toSql(std.testing.allocator);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT name, email FROM users", result.sql);
}

test "raw fragment" {
    const q = Query(TestUser).init().raw("FOR UPDATE");
    const result = try q.toSql(std.testing.allocator);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users FOR UPDATE", result.sql);
}

test "IS NULL and IS NOT NULL" {
    const q = Query(TestUser).init()
        .where("email", .is_null, null)
        .where("name", .is_not_null, null);
    const result = try q.toSql(std.testing.allocator);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users WHERE email IS NULL AND name IS NOT NULL", result.sql);
    try std.testing.expectEqual(@as(usize, 0), result.bind_values.len);
}
