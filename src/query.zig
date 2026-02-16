const std = @import("std");
const schema_mod = @import("schema.zig");
const Dialect = @import("backend.zig").Dialect;

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

pub const JoinType = enum { inner, left, right, full };
pub const Aggregate = enum { count, sum, avg, min, max };

const JoinClause = struct {
    join_type: JoinType = .inner,
    table: []const u8 = "",
    on_left: []const u8 = "",
    on_right: []const u8 = "",
    active: bool = false,
};

const WhereClause = struct {
    field: []const u8 = "",
    op: Op = .eq,
    value: ?[]const u8 = null,
    is_or: bool = false,
    active: bool = false,
    is_raw: bool = false,
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
        const max_joins = 4;
        const max_group = 4;
        const max_having = 4;

        where_clauses: [max_where]WhereClause = [_]WhereClause{.{}} ** max_where,
        where_count: usize = 0,
        order_clauses: [max_order]OrderClause = [_]OrderClause{.{}} ** max_order,
        order_count: usize = 0,
        limit_val: ?u32 = null,
        offset_val: ?u32 = null,
        select_fields: ?[]const u8 = null,
        raw_fragment: ?[]const u8 = null,
        join_clauses: [max_joins]JoinClause = [_]JoinClause{.{}} ** max_joins,
        join_count: usize = 0,
        group_fields: [max_group][]const u8 = [_][]const u8{""} ** max_group,
        group_count: usize = 0,
        having_clauses: [max_having]WhereClause = [_]WhereClause{.{}} ** max_having,
        having_count: usize = 0,

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
                    .is_raw = false,
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
                    .is_raw = false,
                };
                q.where_count += 1;
            }
            return q;
        }

        pub fn whereRaw(self: Self, fragment: []const u8) Self {
            var q = self;
            if (q.where_count < max_where) {
                q.where_clauses[q.where_count] = .{
                    .field = fragment,
                    .op = .eq,
                    .value = null,
                    .is_or = false,
                    .active = true,
                    .is_raw = true,
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

        pub fn join(self: Self, comptime join_type: JoinType, table: []const u8, on_left: []const u8, on_right: []const u8) Self {
            var q = self;
            if (q.join_count < max_joins) {
                q.join_clauses[q.join_count] = .{
                    .join_type = join_type,
                    .table = table,
                    .on_left = on_left,
                    .on_right = on_right,
                    .active = true,
                };
                q.join_count += 1;
            }
            return q;
        }

        pub fn innerJoin(self: Self, table: []const u8, on_left: []const u8, on_right: []const u8) Self {
            return self.join(.inner, table, on_left, on_right);
        }

        pub fn leftJoin(self: Self, table: []const u8, on_left: []const u8, on_right: []const u8) Self {
            return self.join(.left, table, on_left, on_right);
        }

        pub fn groupBy(self: Self, field: []const u8) Self {
            var q = self;
            if (q.group_count < max_group) {
                q.group_fields[q.group_count] = field;
                q.group_count += 1;
            }
            return q;
        }

        pub fn having(self: Self, field: []const u8, op: Op, value: ?[]const u8) Self {
            var q = self;
            if (q.having_count < max_having) {
                q.having_clauses[q.having_count] = .{
                    .field = field,
                    .op = op,
                    .value = value,
                    .is_or = false,
                    .active = true,
                    .is_raw = false,
                };
                q.having_count += 1;
            }
            return q;
        }

        pub fn merge(self: Self, other: Self) Self {
            var q = self;

            // Merge where clauses
            for (0..other.where_count) |i| {
                if (q.where_count < max_where) {
                    q.where_clauses[q.where_count] = other.where_clauses[i];
                    q.where_count += 1;
                }
            }

            // Merge order clauses
            for (0..other.order_count) |i| {
                if (q.order_count < max_order) {
                    q.order_clauses[q.order_count] = other.order_clauses[i];
                    q.order_count += 1;
                }
            }

            // Merge join clauses
            for (0..other.join_count) |i| {
                if (q.join_count < max_joins) {
                    q.join_clauses[q.join_count] = other.join_clauses[i];
                    q.join_count += 1;
                }
            }

            // Merge group by
            for (0..other.group_count) |i| {
                if (q.group_count < max_group) {
                    q.group_fields[q.group_count] = other.group_fields[i];
                    q.group_count += 1;
                }
            }

            // Merge having clauses
            for (0..other.having_count) |i| {
                if (q.having_count < max_having) {
                    q.having_clauses[q.having_count] = other.having_clauses[i];
                    q.having_count += 1;
                }
            }

            // Take limit/offset from other if set
            if (other.limit_val) |lim| q.limit_val = lim;
            if (other.offset_val) |off| q.offset_val = off;
            if (other.select_fields) |sf| q.select_fields = sf;
            if (other.raw_fragment) |rf| q.raw_fragment = rf;

            return q;
        }

        pub const SqlResult = struct {
            sql: []u8,
            bind_values: []const ?[]const u8,
        };

        pub fn toSql(self: Self, allocator: std.mem.Allocator, dialect: Dialect) !SqlResult {
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

            // JOINs
            try self.appendJoins(&parts, allocator);

            // WHERE
            var param_idx: usize = 1;
            try self.appendWhere(&parts, &bind_list, allocator, dialect, &param_idx);

            // GROUP BY
            try self.appendGroupBy(&parts, allocator);

            // HAVING
            try self.appendHaving(&parts, &bind_list, allocator, dialect, &param_idx);

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

        pub fn toCountSql(self: Self, allocator: std.mem.Allocator, dialect: Dialect) !SqlResult {
            var parts: std.ArrayList(u8) = .empty;
            errdefer parts.deinit(allocator);

            var bind_list: std.ArrayList(?[]const u8) = .empty;
            errdefer bind_list.deinit(allocator);

            try parts.appendSlice(allocator, "SELECT COUNT(*) FROM ");
            try parts.appendSlice(allocator, M.Table);

            // JOINs
            try self.appendJoins(&parts, allocator);

            // WHERE
            var param_idx: usize = 1;
            try self.appendWhere(&parts, &bind_list, allocator, dialect, &param_idx);

            // GROUP BY
            try self.appendGroupBy(&parts, allocator);

            // HAVING
            try self.appendHaving(&parts, &bind_list, allocator, dialect, &param_idx);

            return .{
                .sql = try parts.toOwnedSlice(allocator),
                .bind_values = try bind_list.toOwnedSlice(allocator),
            };
        }

        pub fn toAggregateSql(self: Self, agg: Aggregate, field: []const u8, allocator: std.mem.Allocator, dialect: Dialect) !SqlResult {
            var parts: std.ArrayList(u8) = .empty;
            errdefer parts.deinit(allocator);

            var bind_list: std.ArrayList(?[]const u8) = .empty;
            errdefer bind_list.deinit(allocator);

            try parts.appendSlice(allocator, "SELECT ");
            const agg_str = switch (agg) {
                .count => "COUNT(",
                .sum => "SUM(",
                .avg => "AVG(",
                .min => "MIN(",
                .max => "MAX(",
            };
            try parts.appendSlice(allocator, agg_str);
            try parts.appendSlice(allocator, field);
            try parts.appendSlice(allocator, ") FROM ");
            try parts.appendSlice(allocator, M.Table);

            // JOINs
            try self.appendJoins(&parts, allocator);

            // WHERE
            var param_idx: usize = 1;
            try self.appendWhere(&parts, &bind_list, allocator, dialect, &param_idx);

            // GROUP BY
            try self.appendGroupBy(&parts, allocator);

            // HAVING
            try self.appendHaving(&parts, &bind_list, allocator, dialect, &param_idx);

            return .{
                .sql = try parts.toOwnedSlice(allocator),
                .bind_values = try bind_list.toOwnedSlice(allocator),
            };
        }

        fn appendJoins(self: Self, parts: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
            for (0..self.join_count) |i| {
                const jc = self.join_clauses[i];
                const join_str = switch (jc.join_type) {
                    .inner => " INNER JOIN ",
                    .left => " LEFT JOIN ",
                    .right => " RIGHT JOIN ",
                    .full => " FULL JOIN ",
                };
                try parts.appendSlice(allocator, join_str);
                try parts.appendSlice(allocator, jc.table);
                try parts.appendSlice(allocator, " ON ");
                try parts.appendSlice(allocator, jc.on_left);
                try parts.appendSlice(allocator, " = ");
                try parts.appendSlice(allocator, jc.on_right);
            }
        }

        fn appendGroupBy(self: Self, parts: *std.ArrayList(u8), allocator: std.mem.Allocator) !void {
            if (self.group_count > 0) {
                try parts.appendSlice(allocator, " GROUP BY ");
                for (0..self.group_count) |i| {
                    if (i > 0) try parts.appendSlice(allocator, ", ");
                    try parts.appendSlice(allocator, self.group_fields[i]);
                }
            }
        }

        fn appendHaving(self: Self, parts: *std.ArrayList(u8), bind_list: *std.ArrayList(?[]const u8), allocator: std.mem.Allocator, dialect: Dialect, param_idx: *usize) !void {
            if (self.having_count > 0) {
                try parts.appendSlice(allocator, " HAVING ");
                for (0..self.having_count) |i| {
                    const hc = self.having_clauses[i];
                    if (i > 0) {
                        try parts.appendSlice(allocator, if (hc.is_or) " OR " else " AND ");
                    }
                    try parts.appendSlice(allocator, hc.field);

                    switch (hc.op) {
                        .is_null => {
                            try parts.appendSlice(allocator, " IS NULL");
                            continue;
                        },
                        .is_not_null => {
                            try parts.appendSlice(allocator, " IS NOT NULL");
                            continue;
                        },
                        else => {},
                    }

                    const op_str = switch (hc.op) {
                        .eq => " = ",
                        .neq => " != ",
                        .gt => " > ",
                        .lt => " < ",
                        .gte => " >= ",
                        .lte => " <= ",
                        .like => " LIKE ",
                        .is_null, .is_not_null => unreachable,
                    };
                    try parts.appendSlice(allocator, op_str);

                    if (dialect == .postgres) {
                        try parts.append(allocator, '$');
                        var idx_buf: [16]u8 = undefined;
                        const idx_str = std.fmt.bufPrint(&idx_buf, "{d}", .{param_idx.*}) catch "0";
                        try parts.appendSlice(allocator, idx_str);
                    } else {
                        try parts.append(allocator, '?');
                    }

                    try bind_list.append(allocator, hc.value);
                    param_idx.* += 1;
                }
            }
        }

        fn appendWhere(self: Self, parts: *std.ArrayList(u8), bind_list: *std.ArrayList(?[]const u8), allocator: std.mem.Allocator, dialect: Dialect, param_idx: *usize) !void {
            if (self.where_count > 0) {
                try parts.appendSlice(allocator, " WHERE ");
                for (0..self.where_count) |i| {
                    const wc = self.where_clauses[i];
                    if (i > 0) {
                        try parts.appendSlice(allocator, if (wc.is_or) " OR " else " AND ");
                    }

                    if (wc.is_raw) {
                        try parts.appendSlice(allocator, wc.field);
                        continue;
                    }

                    try parts.appendSlice(allocator, wc.field);
                    switch (wc.op) {
                        .is_null => {
                            try parts.appendSlice(allocator, " IS NULL");
                            continue;
                        },
                        .is_not_null => {
                            try parts.appendSlice(allocator, " IS NOT NULL");
                            continue;
                        },
                        else => {},
                    }

                    const op_str = switch (wc.op) {
                        .eq => " = ",
                        .neq => " != ",
                        .gt => " > ",
                        .lt => " < ",
                        .gte => " >= ",
                        .lte => " <= ",
                        .like => " LIKE ",
                        .is_null, .is_not_null => unreachable,
                    };
                    try parts.appendSlice(allocator, op_str);

                    if (dialect == .postgres) {
                        try parts.append(allocator, '$');
                        var idx_buf: [16]u8 = undefined;
                        const idx_str = std.fmt.bufPrint(&idx_buf, "{d}", .{param_idx.*}) catch "0";
                        try parts.appendSlice(allocator, idx_str);
                    } else {
                        try parts.append(allocator, '?');
                    }

                    try bind_list.append(allocator, wc.value);
                    param_idx.* += 1;
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
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users", result.sql);
    try std.testing.expectEqual(@as(usize, 0), result.bind_values.len);
}

test "WHERE clause" {
    const q = Query(TestUser).init().where("name", .eq, "alice");
    const result = try q.toSql(std.testing.allocator, .sqlite);
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
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users WHERE name = ? OR name = ?", result.sql);
    try std.testing.expectEqual(@as(usize, 2), result.bind_values.len);
}

test "ORDER BY" {
    const q = Query(TestUser).init().orderBy("name", .asc);
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users ORDER BY name ASC", result.sql);
}

test "LIMIT and OFFSET" {
    const q = Query(TestUser).init().limit(10).offset(20);
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users LIMIT 10 OFFSET 20", result.sql);
}

test "combined query" {
    const q = Query(TestUser).init()
        .where("email", .like, "%@example.com")
        .orderBy("name", .desc)
        .limit(5);
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users WHERE email LIKE ? ORDER BY name DESC LIMIT 5", result.sql);
}

test "toCountSql" {
    const q = Query(TestUser).init().where("name", .eq, "alice");
    const result = try q.toCountSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT COUNT(*) FROM users WHERE name = ?", result.sql);
}

test "select fields" {
    const q = Query(TestUser).init().select("name, email");
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT name, email FROM users", result.sql);
}

test "raw fragment" {
    const q = Query(TestUser).init().raw("FOR UPDATE");
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users FOR UPDATE", result.sql);
}

test "IS NULL and IS NOT NULL" {
    const q = Query(TestUser).init()
        .where("email", .is_null, null)
        .where("name", .is_not_null, null);
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users WHERE email IS NULL AND name IS NOT NULL", result.sql);
    try std.testing.expectEqual(@as(usize, 0), result.bind_values.len);
}

test "postgres WHERE with $N placeholders" {
    const q = Query(TestUser).init()
        .where("name", .eq, "alice")
        .where("email", .like, "%@example.com");
    const result = try q.toSql(std.testing.allocator, .postgres);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users WHERE name = $1 AND email LIKE $2", result.sql);
    try std.testing.expectEqual(@as(usize, 2), result.bind_values.len);
}

test "postgres toCountSql with $N placeholders" {
    const q = Query(TestUser).init().where("name", .eq, "alice");
    const result = try q.toCountSql(std.testing.allocator, .postgres);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT COUNT(*) FROM users WHERE name = $1", result.sql);
}

// ── New tests for 5c ──────────────────────────────────────────────────

test "innerJoin SQL generation" {
    const q = Query(TestUser).init()
        .select("users.*, posts.title")
        .innerJoin("posts", "users.id", "posts.user_id");
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT users.*, posts.title FROM users INNER JOIN posts ON users.id = posts.user_id", result.sql);
}

test "leftJoin with WHERE" {
    const q = Query(TestUser).init()
        .select("users.*, orders.total")
        .leftJoin("orders", "users.id", "orders.user_id")
        .where("orders.total", .gt, "100");
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT users.*, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE orders.total > ?", result.sql);
    try std.testing.expectEqual(@as(usize, 1), result.bind_values.len);
}

test "groupBy + having" {
    const q = Query(TestUser).init()
        .select("name, COUNT(*) as cnt")
        .groupBy("name")
        .having("COUNT(*)", .gt, "1");
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT name, COUNT(*) as cnt FROM users GROUP BY name HAVING COUNT(*) > ?", result.sql);
    try std.testing.expectEqual(@as(usize, 1), result.bind_values.len);
}

test "toAggregateSql count" {
    const q = Query(TestUser).init().where("name", .eq, "alice");
    const result = try q.toAggregateSql(.count, "*", std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT COUNT(*) FROM users WHERE name = ?", result.sql);
}

test "toAggregateSql sum" {
    const q = Query(TestUser).init();
    const result = try q.toAggregateSql(.sum, "inserted_at", std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT SUM(inserted_at) FROM users", result.sql);
}

test "merge query composition" {
    const base = Query(TestUser).init().where("name", .eq, "alice");
    const paginated = Query(TestUser).init().limit(10).offset(20);
    const combined = base.merge(paginated);
    const result = try combined.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users WHERE name = ? LIMIT 10 OFFSET 20", result.sql);
    try std.testing.expectEqual(@as(usize, 1), result.bind_values.len);
}

test "whereRaw subquery pattern" {
    const q = Query(TestUser).init()
        .whereRaw("id IN (SELECT user_id FROM orders)");
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT id, name, email, inserted_at, updated_at FROM users WHERE id IN (SELECT user_id FROM orders)", result.sql);
    try std.testing.expectEqual(@as(usize, 0), result.bind_values.len);
}

test "postgres HAVING with $N placeholders" {
    const q = Query(TestUser).init()
        .select("name, COUNT(*) as cnt")
        .where("email", .like, "%@example.com")
        .groupBy("name")
        .having("COUNT(*)", .gt, "1");
    const result = try q.toSql(std.testing.allocator, .postgres);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT name, COUNT(*) as cnt FROM users WHERE email LIKE $1 GROUP BY name HAVING COUNT(*) > $2", result.sql);
    try std.testing.expectEqual(@as(usize, 2), result.bind_values.len);
}

test "JOIN + GROUP BY combined" {
    const q = Query(TestUser).init()
        .select("users.name, COUNT(posts.id) as post_count")
        .innerJoin("posts", "users.id", "posts.user_id")
        .groupBy("users.name")
        .having("COUNT(posts.id)", .gte, "5");
    const result = try q.toSql(std.testing.allocator, .sqlite);
    defer std.testing.allocator.free(result.sql);
    defer std.testing.allocator.free(result.bind_values);

    try std.testing.expectEqualStrings("SELECT users.name, COUNT(posts.id) as post_count FROM users INNER JOIN posts ON users.id = posts.user_id GROUP BY users.name HAVING COUNT(posts.id) >= ?", result.sql);
}
