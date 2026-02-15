const std = @import("std");
const backend = @import("backend.zig");
const connection_mod = @import("connection.zig");
const ConnectionState = connection_mod.ConnectionState;
const sqlite = @import("sqlite.zig");

fn spinLock(m: *std.atomic.Mutex) void {
    while (!m.tryLock()) {}
}

pub fn PoolConfig(comptime Backend: type) type {
    return struct {
        size: u16 = 5,
        connection: Backend.Config = .{},
        checkout_timeout_iterations: u32 = 100_000,
    };
}

pub fn PooledConnection(comptime Backend: type) type {
    const PoolType = Pool(Backend);
    const Conn = connection_mod.Connection(Backend);

    return struct {
        conn: *Conn,
        index: usize,
        pool: *PoolType,

        pub fn release(self: *@This()) void {
            self.pool.checkin(self.index);
        }
    };
}

pub fn Pool(comptime Backend: type) type {
    comptime backend.validate(Backend);
    const Conn = connection_mod.Connection(Backend);

    return struct {
        const Self = @This();
        pub const max_pool_size = 32;

        connections: [max_pool_size]Conn = undefined,
        in_use: [max_pool_size]bool = [_]bool{false} ** max_pool_size,
        size: u16,
        mutex: std.atomic.Mutex = .unlocked,

        pub fn init(config: PoolConfig(Backend)) !Self {
            var pool = Self{
                .size = @min(config.size, max_pool_size),
            };

            var opened: u16 = 0;
            errdefer {
                for (0..opened) |i| {
                    pool.connections[i].close();
                }
            }

            for (0..pool.size) |i| {
                pool.connections[i] = try Conn.open(config.connection);
                opened += 1;
            }

            return pool;
        }

        pub fn deinit(self: *Self) void {
            for (0..self.size) |i| {
                self.connections[i].close();
            }
        }

        pub fn checkout(self: *Self) !PooledConnection(Backend) {
            var iterations: u32 = 0;
            const max_iter: u32 = 100_000;

            while (iterations < max_iter) : (iterations += 1) {
                spinLock(&self.mutex);
                defer self.mutex.unlock();

                for (0..self.size) |i| {
                    if (!self.in_use[i]) {
                        // Health check on checkout
                        if (!self.connections[i].isAlive()) {
                            self.connections[i].reconnect() catch continue;
                        }
                        self.in_use[i] = true;
                        return .{
                            .conn = &self.connections[i],
                            .index = i,
                            .pool = self,
                        };
                    }
                }
            }

            return error.PoolExhausted;
        }

        pub fn checkin(self: *Self, index: usize) void {
            spinLock(&self.mutex);
            defer self.mutex.unlock();

            // Auto-rollback if connection was left in a transaction
            if (self.connections[index].state == .in_transaction) {
                self.connections[index].exec("ROLLBACK") catch {};
                self.connections[index].state = .connected;
                self.connections[index].savepoint_depth = 0;
            }

            self.in_use[index] = false;
        }

        pub fn available(self: *Self) u16 {
            spinLock(&self.mutex);
            defer self.mutex.unlock();

            var count: u16 = 0;
            for (0..self.size) |i| {
                if (!self.in_use[i]) count += 1;
            }
            return count;
        }

        pub fn healthCheck(self: *Self) void {
            spinLock(&self.mutex);
            defer self.mutex.unlock();

            for (0..self.size) |i| {
                if (!self.in_use[i] and !self.connections[i].isAlive()) {
                    self.connections[i].reconnect() catch {};
                }
            }
        }
    };
}

// ── Tests ──────────────────────────────────────────────────────────────

test "init and deinit pool" {
    var pool = try Pool(sqlite).init(.{ .size = 3 });
    defer pool.deinit();
    try std.testing.expectEqual(@as(u16, 3), pool.size);
}

test "checkout and checkin" {
    var pool = try Pool(sqlite).init(.{ .size = 2 });
    defer pool.deinit();

    var pc = try pool.checkout();
    try std.testing.expectEqual(@as(u16, 1), pool.available());
    pc.release();
    try std.testing.expectEqual(@as(u16, 2), pool.available());
}

test "pool exhaustion" {
    var pool = try Pool(sqlite).init(.{ .size = 1, .connection = .{}, .checkout_timeout_iterations = 100 });
    defer pool.deinit();

    var pc1 = try pool.checkout();
    defer pc1.release();

    // Second checkout should fail with exhaustion
    const result = pool.checkout();
    try std.testing.expectError(error.PoolExhausted, result);
}

test "checkin makes connection available again" {
    var pool = try Pool(sqlite).init(.{ .size = 1 });
    defer pool.deinit();

    var pc = try pool.checkout();
    try std.testing.expectEqual(@as(u16, 0), pool.available());
    pc.release();
    try std.testing.expectEqual(@as(u16, 1), pool.available());

    // Can checkout again
    var pc2 = try pool.checkout();
    pc2.release();
}

test "available count" {
    var pool = try Pool(sqlite).init(.{ .size = 3 });
    defer pool.deinit();

    try std.testing.expectEqual(@as(u16, 3), pool.available());

    var pc1 = try pool.checkout();
    try std.testing.expectEqual(@as(u16, 2), pool.available());

    var pc2 = try pool.checkout();
    try std.testing.expectEqual(@as(u16, 1), pool.available());

    pc1.release();
    try std.testing.expectEqual(@as(u16, 2), pool.available());

    pc2.release();
    try std.testing.expectEqual(@as(u16, 3), pool.available());
}
