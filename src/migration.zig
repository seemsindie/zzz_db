const std = @import("std");
const backend_mod = @import("backend.zig");
const Dialect = backend_mod.Dialect;
const pool_mod = @import("pool.zig");
const connection_mod = @import("connection.zig");
const transaction_mod = @import("transaction.zig");
const sqlite = @import("sqlite.zig");

pub const ColumnType = enum { integer, bigint, text, real, boolean, blob, timestamp };

pub const ColumnDef = struct {
    name: []const u8,
    col_type: ColumnType,
    primary_key: bool = false,
    auto_increment: bool = false,
    not_null: bool = false,
    unique: bool = false,
    default: ?[]const u8 = null,
    references: ?struct { table: []const u8, column: []const u8 } = null,

    pub fn typeStr(self: ColumnDef, dialect: Dialect) []const u8 {
        return switch (self.col_type) {
            .integer => "INTEGER",
            .bigint => if (dialect == .postgres) "BIGINT" else "BIGINT",
            .text => "TEXT",
            .real => if (dialect == .postgres) "DOUBLE PRECISION" else "REAL",
            .boolean => "BOOLEAN",
            .blob => if (dialect == .postgres) "BYTEA" else "BLOB",
            .timestamp => if (dialect == .postgres) "TIMESTAMP" else "BIGINT",
        };
    }
};

pub fn MigrationDef(comptime Backend: type) type {
    comptime backend_mod.validate(Backend);
    return struct {
        version: i64,
        name: []const u8,
        up: *const fn (*MigrationContext(Backend)) anyerror!void,
        down: *const fn (*MigrationContext(Backend)) anyerror!void,
    };
}

pub const MigrationStatus = struct {
    version: i64,
    name: []const u8,
    applied: bool,
};

pub fn MigrationContext(comptime Backend: type) type {
    comptime backend_mod.validate(Backend);
    const Conn = connection_mod.Connection(Backend);

    return struct {
        const Self = @This();
        conn: *Conn,

        pub fn init(conn: *Conn) Self {
            return .{ .conn = conn };
        }

        pub fn createTable(self: *Self, name: []const u8, columns: []const ColumnDef) !void {
            var buf: [4096]u8 = undefined;
            var pos: usize = 0;

            pos += copySlice(&buf, pos, "CREATE TABLE ");
            pos += copySlice(&buf, pos, name);
            pos += copySlice(&buf, pos, " (");

            for (columns, 0..) |col, i| {
                if (i > 0) {
                    pos += copySlice(&buf, pos, ", ");
                }
                pos += copySlice(&buf, pos, col.name);
                pos += copySlice(&buf, pos, " ");

                if (col.primary_key and col.auto_increment and Backend.dialect == .sqlite) {
                    pos += copySlice(&buf, pos, "INTEGER PRIMARY KEY AUTOINCREMENT");
                } else if (col.primary_key and col.auto_increment and Backend.dialect == .postgres) {
                    pos += copySlice(&buf, pos, "BIGSERIAL PRIMARY KEY");
                } else {
                    pos += copySlice(&buf, pos, col.typeStr(Backend.dialect));
                    if (col.primary_key) {
                        pos += copySlice(&buf, pos, " PRIMARY KEY");
                    }
                    if (col.not_null) {
                        pos += copySlice(&buf, pos, " NOT NULL");
                    }
                    if (col.unique) {
                        pos += copySlice(&buf, pos, " UNIQUE");
                    }
                    if (col.default) |def| {
                        pos += copySlice(&buf, pos, " DEFAULT ");
                        pos += copySlice(&buf, pos, def);
                    }
                    if (col.references) |ref| {
                        pos += copySlice(&buf, pos, " REFERENCES ");
                        pos += copySlice(&buf, pos, ref.table);
                        pos += copySlice(&buf, pos, "(");
                        pos += copySlice(&buf, pos, ref.column);
                        pos += copySlice(&buf, pos, ")");
                    }
                }
            }

            pos += copySlice(&buf, pos, ")");
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];
            try self.conn.exec(sql_z);
        }

        pub fn dropTable(self: *Self, name: []const u8) !void {
            var buf: [256]u8 = undefined;
            var pos: usize = 0;
            pos += copySlice(&buf, pos, "DROP TABLE IF EXISTS ");
            pos += copySlice(&buf, pos, name);
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];
            try self.conn.exec(sql_z);
        }

        pub fn addColumn(self: *Self, table: []const u8, col: ColumnDef) !void {
            var buf: [512]u8 = undefined;
            var pos: usize = 0;
            pos += copySlice(&buf, pos, "ALTER TABLE ");
            pos += copySlice(&buf, pos, table);
            pos += copySlice(&buf, pos, " ADD COLUMN ");
            pos += copySlice(&buf, pos, col.name);
            pos += copySlice(&buf, pos, " ");
            pos += copySlice(&buf, pos, col.typeStr(Backend.dialect));
            if (col.not_null) {
                pos += copySlice(&buf, pos, " NOT NULL");
                if (col.default) |def| {
                    pos += copySlice(&buf, pos, " DEFAULT ");
                    pos += copySlice(&buf, pos, def);
                }
            }
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];
            try self.conn.exec(sql_z);
        }

        pub fn dropColumn(self: *Self, table: []const u8, column: []const u8) !void {
            var buf: [256]u8 = undefined;
            var pos: usize = 0;
            pos += copySlice(&buf, pos, "ALTER TABLE ");
            pos += copySlice(&buf, pos, table);
            pos += copySlice(&buf, pos, " DROP COLUMN ");
            pos += copySlice(&buf, pos, column);
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];
            try self.conn.exec(sql_z);
        }

        pub fn renameColumn(self: *Self, table: []const u8, old: []const u8, new: []const u8) !void {
            var buf: [256]u8 = undefined;
            var pos: usize = 0;
            pos += copySlice(&buf, pos, "ALTER TABLE ");
            pos += copySlice(&buf, pos, table);
            pos += copySlice(&buf, pos, " RENAME COLUMN ");
            pos += copySlice(&buf, pos, old);
            pos += copySlice(&buf, pos, " TO ");
            pos += copySlice(&buf, pos, new);
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];
            try self.conn.exec(sql_z);
        }

        pub fn createIndex(self: *Self, name: []const u8, table: []const u8, columns: []const []const u8, unique: bool) !void {
            var buf: [512]u8 = undefined;
            var pos: usize = 0;
            if (unique) {
                pos += copySlice(&buf, pos, "CREATE UNIQUE INDEX ");
            } else {
                pos += copySlice(&buf, pos, "CREATE INDEX ");
            }
            pos += copySlice(&buf, pos, name);
            pos += copySlice(&buf, pos, " ON ");
            pos += copySlice(&buf, pos, table);
            pos += copySlice(&buf, pos, " (");
            for (columns, 0..) |col, ci| {
                if (ci > 0) pos += copySlice(&buf, pos, ", ");
                pos += copySlice(&buf, pos, col);
            }
            pos += copySlice(&buf, pos, ")");
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];
            try self.conn.exec(sql_z);
        }

        pub fn dropIndex(self: *Self, name: []const u8) !void {
            var buf: [256]u8 = undefined;
            var pos: usize = 0;
            pos += copySlice(&buf, pos, "DROP INDEX IF EXISTS ");
            pos += copySlice(&buf, pos, name);
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];
            try self.conn.exec(sql_z);
        }

        pub fn addForeignKey(self: *Self, table: []const u8, col: []const u8, ref_table: []const u8, ref_col: []const u8) !void {
            if (Backend.dialect == .sqlite) {
                return error.UnsupportedOperation;
            }
            var buf: [512]u8 = undefined;
            var pos: usize = 0;
            pos += copySlice(&buf, pos, "ALTER TABLE ");
            pos += copySlice(&buf, pos, table);
            pos += copySlice(&buf, pos, " ADD CONSTRAINT fk_");
            pos += copySlice(&buf, pos, table);
            pos += copySlice(&buf, pos, "_");
            pos += copySlice(&buf, pos, col);
            pos += copySlice(&buf, pos, " FOREIGN KEY (");
            pos += copySlice(&buf, pos, col);
            pos += copySlice(&buf, pos, ") REFERENCES ");
            pos += copySlice(&buf, pos, ref_table);
            pos += copySlice(&buf, pos, "(");
            pos += copySlice(&buf, pos, ref_col);
            pos += copySlice(&buf, pos, ")");
            buf[pos] = 0;
            const sql_z: [:0]const u8 = buf[0..pos :0];
            try self.conn.exec(sql_z);
        }

        pub fn execRaw(self: *Self, sql: [:0]const u8) !void {
            try self.conn.exec(sql);
        }
    };
}

pub fn Runner(comptime Backend: type) type {
    comptime backend_mod.validate(Backend);
    const PoolType = pool_mod.Pool(Backend);
    const Conn = connection_mod.Connection(Backend);
    const Txn = transaction_mod.Transaction(Backend);
    const MigCtx = MigrationContext(Backend);

    return struct {
        const Self = @This();
        pool: *PoolType,

        pub fn init(pool: *PoolType) Self {
            return .{ .pool = pool };
        }

        pub fn ensureMigrationsTable(self: Self) !void {
            var pc = try self.pool.checkout();
            defer pc.release();

            if (Backend.dialect == .postgres) {
                try pc.conn.exec("CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, inserted_at TIMESTAMP DEFAULT NOW())");
            } else {
                try pc.conn.exec("CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, inserted_at BIGINT DEFAULT 0)");
            }
        }

        pub fn appliedVersions(self: Self, allocator: std.mem.Allocator) ![]i64 {
            var pc = try self.pool.checkout();
            defer pc.release();

            var rs = try Backend.ResultSet.query(&pc.conn.db, "SELECT version FROM schema_migrations ORDER BY version", &.{});
            defer rs.deinit();

            var versions: std.ArrayList(i64) = .empty;
            errdefer versions.deinit(allocator);

            while (try rs.next()) {
                try versions.append(allocator, rs.columnInt64(0));
            }

            return try versions.toOwnedSlice(allocator);
        }

        pub fn migrate(self: Self, comptime migrations: []const MigrationDef(Backend), allocator: std.mem.Allocator) !usize {
            try self.ensureMigrationsTable();

            const applied = try self.appliedVersions(allocator);
            defer allocator.free(applied);

            var count: usize = 0;
            inline for (migrations) |mig| {
                var already_applied = false;
                for (applied) |v| {
                    if (v == mig.version) {
                        already_applied = true;
                        break;
                    }
                }

                if (!already_applied) {
                    var pc = try self.pool.checkout();
                    defer pc.release();

                    try Txn.begin(pc.conn);
                    errdefer Txn.rollback(pc.conn) catch {};

                    var ctx = MigCtx.init(pc.conn);
                    try mig.up(&ctx);

                    // Record migration version
                    try self.insertVersion(pc.conn, mig.version);

                    try Txn.commit(pc.conn);
                    count += 1;
                }
            }

            return count;
        }

        pub fn rollback(self: Self, comptime migrations: []const MigrationDef(Backend), n: usize, allocator: std.mem.Allocator) !usize {
            try self.ensureMigrationsTable();

            const applied = try self.appliedVersions(allocator);
            defer allocator.free(applied);

            var count: usize = 0;

            // Roll back in reverse order
            var idx = applied.len;
            while (idx > 0 and count < n) {
                idx -= 1;
                const version = applied[idx];

                // Find the migration definition
                inline for (migrations) |mig| {
                    if (mig.version == version) {
                        var pc = try self.pool.checkout();
                        defer pc.release();

                        try Txn.begin(pc.conn);
                        errdefer Txn.rollback(pc.conn) catch {};

                        var ctx = MigCtx.init(pc.conn);
                        try mig.down(&ctx);

                        try self.deleteVersion(pc.conn, version);

                        try Txn.commit(pc.conn);
                        count += 1;
                    }
                }
            }

            return count;
        }

        pub fn status(self: Self, comptime migrations: []const MigrationDef(Backend), allocator: std.mem.Allocator) ![]MigrationStatus {
            try self.ensureMigrationsTable();

            const applied = try self.appliedVersions(allocator);
            defer allocator.free(applied);

            var statuses: std.ArrayList(MigrationStatus) = .empty;
            errdefer statuses.deinit(allocator);

            inline for (migrations) |mig| {
                var is_applied = false;
                for (applied) |v| {
                    if (v == mig.version) {
                        is_applied = true;
                        break;
                    }
                }
                try statuses.append(allocator, .{
                    .version = mig.version,
                    .name = mig.name,
                    .applied = is_applied,
                });
            }

            return try statuses.toOwnedSlice(allocator);
        }

        fn insertVersion(self: Self, conn: *Conn, version: i64) !void {
            _ = self;
            var buf: [128]u8 = undefined;
            const sql = std.fmt.bufPrint(&buf, "INSERT INTO schema_migrations (version) VALUES ({d})", .{version}) catch return error.InternalError;
            buf[sql.len] = 0;
            const sql_z: [:0]const u8 = buf[0..sql.len :0];
            try conn.exec(sql_z);
        }

        fn deleteVersion(self: Self, conn: *Conn, version: i64) !void {
            _ = self;
            var buf: [128]u8 = undefined;
            const sql = std.fmt.bufPrint(&buf, "DELETE FROM schema_migrations WHERE version = {d}", .{version}) catch return error.InternalError;
            buf[sql.len] = 0;
            const sql_z: [:0]const u8 = buf[0..sql.len :0];
            try conn.exec(sql_z);
        }
    };
}

fn copySlice(dest: []u8, pos: usize, src: []const u8) usize {
    const end = pos + src.len;
    if (end > dest.len) return 0;
    @memcpy(dest[pos..end], src);
    return src.len;
}

// ── Tests ──────────────────────────────────────────────────────────────

const SqlitePool = pool_mod.Pool(sqlite);
const SqliteRunner = Runner(sqlite);
const SqliteMigCtx = MigrationContext(sqlite);
const SqliteMigrationDef = MigrationDef(sqlite);

const test_migrations = [_]SqliteMigrationDef{
    .{
        .version = 20240101000001,
        .name = "create_users",
        .up = &struct {
            fn f(ctx: *SqliteMigCtx) !void {
                try ctx.createTable("users", &.{
                    .{ .name = "id", .col_type = .bigint, .primary_key = true, .auto_increment = true },
                    .{ .name = "name", .col_type = .text, .not_null = true },
                    .{ .name = "email", .col_type = .text, .not_null = true, .unique = true },
                });
            }
        }.f,
        .down = &struct {
            fn f(ctx: *SqliteMigCtx) !void {
                try ctx.dropTable("users");
            }
        }.f,
    },
    .{
        .version = 20240101000002,
        .name = "create_posts",
        .up = &struct {
            fn f(ctx: *SqliteMigCtx) !void {
                try ctx.createTable("posts", &.{
                    .{ .name = "id", .col_type = .bigint, .primary_key = true, .auto_increment = true },
                    .{ .name = "title", .col_type = .text, .not_null = true },
                    .{ .name = "user_id", .col_type = .bigint, .not_null = true },
                });
                try ctx.createIndex("idx_posts_user_id", "posts", &.{"user_id"}, false);
            }
        }.f,
        .down = &struct {
            fn f(ctx: *SqliteMigCtx) !void {
                try ctx.dropIndex("idx_posts_user_id");
                try ctx.dropTable("posts");
            }
        }.f,
    },
};

test "migrate runs pending migrations and creates tables" {
    var pool = try SqlitePool.init(.{ .size = 1 });
    defer pool.deinit();

    const runner = SqliteRunner.init(&pool);
    const count = try runner.migrate(&test_migrations, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), count);

    // Verify tables exist by inserting
    var pc = try pool.checkout();
    defer pc.release();
    try pc.conn.exec("INSERT INTO users (name, email) VALUES ('Alice', 'a@e.com')");
    try pc.conn.exec("INSERT INTO posts (title, user_id) VALUES ('Hello', 1)");
}

test "migrate is idempotent" {
    var pool = try SqlitePool.init(.{ .size = 1 });
    defer pool.deinit();

    const runner = SqliteRunner.init(&pool);

    const count1 = try runner.migrate(&test_migrations, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), count1);

    const count2 = try runner.migrate(&test_migrations, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), count2);
}

test "rollback undoes last N" {
    var pool = try SqlitePool.init(.{ .size = 1 });
    defer pool.deinit();

    const runner = SqliteRunner.init(&pool);
    _ = try runner.migrate(&test_migrations, std.testing.allocator);

    // Rollback 1 migration
    const rolled = try runner.rollback(&test_migrations, 1, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), rolled);

    // Check applied versions — should only have first migration
    const applied = try runner.appliedVersions(std.testing.allocator);
    defer std.testing.allocator.free(applied);
    try std.testing.expectEqual(@as(usize, 1), applied.len);
    try std.testing.expectEqual(@as(i64, 20240101000001), applied[0]);
}

test "status shows applied and pending" {
    var pool = try SqlitePool.init(.{ .size = 1 });
    defer pool.deinit();

    const runner = SqliteRunner.init(&pool);
    _ = try runner.migrate(&test_migrations, std.testing.allocator);

    // Rollback last one
    _ = try runner.rollback(&test_migrations, 1, std.testing.allocator);

    const statuses = try runner.status(&test_migrations, std.testing.allocator);
    defer std.testing.allocator.free(statuses);

    try std.testing.expectEqual(@as(usize, 2), statuses.len);
    try std.testing.expect(statuses[0].applied); // create_users is applied
    try std.testing.expect(!statuses[1].applied); // create_posts is pending
}

test "createIndex and dropIndex" {
    var pool = try SqlitePool.init(.{ .size = 1 });
    defer pool.deinit();

    var pc = try pool.checkout();
    defer pc.release();

    try pc.conn.exec("CREATE TABLE test_idx (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");

    var ctx = SqliteMigCtx.init(pc.conn);
    try ctx.createIndex("idx_test_email", "test_idx", &.{"email"}, true);

    // Verify index works — inserting duplicate should fail
    try pc.conn.exec("INSERT INTO test_idx (name, email) VALUES ('Alice', 'a@e.com')");
    const result = pc.conn.exec("INSERT INTO test_idx (name, email) VALUES ('Bob', 'a@e.com')");
    try std.testing.expectError(error.Constraint, result);

    // Drop index
    try ctx.dropIndex("idx_test_email");

    // Now duplicate should succeed
    try pc.conn.exec("INSERT INTO test_idx (name, email) VALUES ('Bob', 'a@e.com')");
}
