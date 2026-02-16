const std = @import("std");
const Allocator = std.mem.Allocator;
const backend_mod = @import("backend.zig");
const connection_mod = @import("connection.zig");
const transaction_mod = @import("transaction.zig");
const pool_mod = @import("pool.zig");
const repo_mod = @import("repo.zig");
const schema_mod = @import("schema.zig");

/// Wraps a database connection in a transaction that is automatically
/// rolled back when the sandbox is deinitialized. This ensures each
/// test starts with a clean database state without needing to truncate
/// tables or recreate the database.
///
/// Usage:
/// ```
/// var conn = try Connection(sqlite).open(.{});
/// defer conn.close();
/// var sandbox = try TestSandbox(sqlite).begin(&conn);
/// defer sandbox.rollback();
/// // ... test code using conn — all changes rolled back
/// ```
pub fn TestSandbox(comptime Backend: type) type {
    comptime backend_mod.validate(Backend);
    const Conn = connection_mod.Connection(Backend);
    const Txn = transaction_mod.Transaction(Backend);

    return struct {
        conn: *Conn,

        pub fn begin(conn: *Conn) !@This() {
            try Txn.begin(conn);
            return .{ .conn = conn };
        }

        pub fn rollback(self: *@This()) void {
            Txn.rollback(self.conn) catch {};
        }

        pub fn commit(self: *@This()) !void {
            try Txn.commit(self.conn);
        }
    };
}

/// Factory for inserting test records using Schema definitions.
/// Provides a convenient way to create test data with sensible defaults
/// and optional overrides.
///
/// Usage:
/// ```
/// var factory = Factory(sqlite, User).init(&pool);
/// var user = try factory.create(.{ .name = "Alice" }, allocator);
/// defer freeOne(User, &user, allocator);
/// ```
pub fn Factory(comptime Backend: type, comptime Schema: type) type {
    comptime backend_mod.validate(Backend);
    const RepoType = repo_mod.Repo(Backend);
    const PoolType = pool_mod.Pool(Backend);

    return struct {
        repo: RepoType,

        pub fn init(pool: *PoolType) @This() {
            return .{ .repo = RepoType.init(pool) };
        }

        /// Create a single record with default values, applying any overrides.
        /// Fields in `overrides` replace the default zero/empty values.
        pub fn create(self: *@This(), overrides: anytype, allocator: Allocator) !Schema {
            var record: Schema = defaultRecord();
            applyOverrides(&record, overrides);
            return self.repo.insert(Schema, record, allocator);
        }

        /// Create multiple records. Each record gets the same overrides applied.
        pub fn createMany(self: *@This(), count_val: usize, overrides: anytype, allocator: Allocator) ![]Schema {
            var results: std.ArrayList(Schema) = .empty;
            errdefer {
                for (results.items) |*item| {
                    repo_mod.freeOne(Schema, item, allocator);
                }
                results.deinit(allocator);
            }

            for (0..count_val) |_| {
                const record = try self.create(overrides, allocator);
                try results.append(allocator, record);
            }

            return results.toOwnedSlice(allocator);
        }

        /// Build a default record with zero values for all fields.
        fn defaultRecord() Schema {
            var record: Schema = undefined;
            const struct_fields = @typeInfo(Schema).@"struct".fields;
            inline for (struct_fields) |f| {
                if (f.default_value_ptr) |ptr| {
                    const default_ptr: *align(f.alignment) const f.type = @alignCast(@ptrCast(ptr));
                    @field(record, f.name) = default_ptr.*;
                } else if (f.type == []const u8) {
                    @field(record, f.name) = "";
                } else if (f.type == i64 or f.type == i32) {
                    @field(record, f.name) = 0;
                } else if (f.type == f64 or f.type == f32) {
                    @field(record, f.name) = 0.0;
                } else if (f.type == bool) {
                    @field(record, f.name) = false;
                } else if (@typeInfo(f.type) == .optional) {
                    @field(record, f.name) = null;
                } else {
                    @field(record, f.name) = undefined;
                }
            }
            return record;
        }

        /// Apply override values from a struct to the record.
        fn applyOverrides(record: *Schema, overrides: anytype) void {
            const OverrideType = @TypeOf(overrides);
            if (@typeInfo(OverrideType) != .@"struct") return;
            const override_fields = @typeInfo(OverrideType).@"struct".fields;
            inline for (override_fields) |of| {
                if (@hasField(Schema, of.name)) {
                    @field(record, of.name) = @field(overrides, of.name);
                }
            }
        }
    };
}

/// Bulk insert a list of records.
///
/// Usage:
/// ```
/// try seed(sqlite, User, &repo, &.{
///     .{ .id = 0, .name = "Alice", .email = "a@e.com" },
///     .{ .id = 0, .name = "Bob", .email = "b@e.com" },
/// }, allocator);
/// ```
pub fn seed(
    comptime Backend: type,
    comptime Schema: type,
    repo: *repo_mod.Repo(Backend),
    records: []const Schema,
    allocator: Allocator,
) !void {
    for (records) |record| {
        var inserted = try repo.insert(Schema, record, allocator);
        repo_mod.freeOne(Schema, &inserted, allocator);
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

const sqlite = @import("sqlite.zig");
const query_mod = @import("query.zig");

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

fn setupTestDb() !connection_mod.Connection(sqlite) {
    var conn = try connection_mod.Connection(sqlite).open(.{});
    try conn.exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, inserted_at BIGINT DEFAULT 0, updated_at BIGINT DEFAULT 0)");
    return conn;
}

test "TestSandbox rolls back on deinit" {
    var conn = try setupTestDb();
    defer conn.close();

    // Insert inside sandbox
    {
        var sandbox = try TestSandbox(sqlite).begin(&conn);
        defer sandbox.rollback();
        try conn.exec("INSERT INTO users (name, email) VALUES ('sandboxed', 'sb@e.com')");
    }

    // Verify it was rolled back
    var stmt = try sqlite.Statement.prepare(&conn.db, "SELECT COUNT(*) FROM users WHERE name = 'sandboxed'");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 0), stmt.columnInt64(0));
}

test "TestSandbox commit persists" {
    var conn = try setupTestDb();
    defer conn.close();

    {
        var sandbox = try TestSandbox(sqlite).begin(&conn);
        try conn.exec("INSERT INTO users (name, email) VALUES ('committed', 'c@e.com')");
        try sandbox.commit();
    }

    var stmt = try sqlite.Statement.prepare(&conn.db, "SELECT COUNT(*) FROM users WHERE name = 'committed'");
    defer stmt.finalize();
    _ = try stmt.step();
    try std.testing.expectEqual(@as(i64, 1), stmt.columnInt64(0));
}

test "Factory create" {
    var pool = try pool_mod.Pool(sqlite).init(.{ .size = 1 });
    defer pool.deinit();

    var pc = try pool.checkout();
    try pc.conn.exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, inserted_at BIGINT DEFAULT 0, updated_at BIGINT DEFAULT 0)");
    pc.release();

    var factory = Factory(sqlite, TestUser).init(&pool);
    var user = try factory.create(.{ .name = "Alice", .email = "a@e.com" }, std.testing.allocator);
    defer repo_mod.freeOne(TestUser, &user, std.testing.allocator);

    try std.testing.expect(user.id > 0);
    try std.testing.expectEqualStrings("Alice", user.name);
    try std.testing.expectEqualStrings("a@e.com", user.email);
}

test "Factory createMany" {
    var pool = try pool_mod.Pool(sqlite).init(.{ .size = 1 });
    defer pool.deinit();

    var pc = try pool.checkout();
    try pc.conn.exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, inserted_at BIGINT DEFAULT 0, updated_at BIGINT DEFAULT 0)");
    pc.release();

    var factory = Factory(sqlite, TestUser).init(&pool);
    const users = try factory.createMany(3, .{ .name = "User", .email = "u@e.com" }, std.testing.allocator);
    defer {
        for (users) |*u| {
            repo_mod.freeOne(TestUser, u, std.testing.allocator);
        }
        std.testing.allocator.free(users);
    }

    try std.testing.expectEqual(@as(usize, 3), users.len);
    for (users) |u| {
        try std.testing.expect(u.id > 0);
        try std.testing.expectEqualStrings("User", u.name);
    }
}

test "seed inserts multiple records" {
    var pool = try pool_mod.Pool(sqlite).init(.{ .size = 1 });
    defer pool.deinit();

    var pc = try pool.checkout();
    try pc.conn.exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, inserted_at BIGINT DEFAULT 0, updated_at BIGINT DEFAULT 0)");
    pc.release();

    var repo = repo_mod.Repo(sqlite).init(&pool);
    try seed(sqlite, TestUser, &repo, &.{
        .{ .id = 0, .name = "Alice", .email = "a@e.com" },
        .{ .id = 0, .name = "Bob", .email = "b@e.com" },
    }, std.testing.allocator);

    const q = query_mod.Query(TestUser).init();
    const count = try repo.count(TestUser, q, std.testing.allocator);
    try std.testing.expectEqual(@as(i64, 2), count);
}
