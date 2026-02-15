const std = @import("std");
const Connection = @import("connection.zig").Connection;
const ConnectionConfig = @import("connection.zig").ConnectionConfig;
const ConnectionState = @import("connection.zig").ConnectionState;

fn spinLock(m: *std.atomic.Mutex) void {
    while (!m.tryLock()) {}
}

pub const PoolConfig = struct {
    size: u16 = 5,
    connection: ConnectionConfig = .{},
    checkout_timeout_iterations: u32 = 100_000,
};

pub const Pool = struct {
    pub const max_pool_size = 32;

    connections: [max_pool_size]Connection = undefined,
    in_use: [max_pool_size]bool = [_]bool{false} ** max_pool_size,
    size: u16,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(config: PoolConfig) !Pool {
        var pool = Pool{
            .size = @min(config.size, max_pool_size),
        };

        var opened: u16 = 0;
        errdefer {
            for (0..opened) |i| {
                pool.connections[i].close();
            }
        }

        for (0..pool.size) |i| {
            pool.connections[i] = try Connection.open(config.connection);
            opened += 1;
        }

        return pool;
    }

    pub fn deinit(self: *Pool) void {
        for (0..self.size) |i| {
            self.connections[i].close();
        }
    }

    pub fn checkout(self: *Pool) !PooledConnection {
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
                    return PooledConnection{
                        .conn = &self.connections[i],
                        .index = i,
                        .pool = self,
                    };
                }
            }
        }

        return error.PoolExhausted;
    }

    pub fn checkin(self: *Pool, index: usize) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();

        // Auto-rollback if connection was left in a transaction
        if (self.connections[index].state == .in_transaction) {
            self.connections[index].exec("ROLLBACK") catch {};
            self.connections[index].state = .connected;
        }

        self.in_use[index] = false;
    }

    pub fn available(self: *Pool) u16 {
        spinLock(&self.mutex);
        defer self.mutex.unlock();

        var count: u16 = 0;
        for (0..self.size) |i| {
            if (!self.in_use[i]) count += 1;
        }
        return count;
    }

    pub fn healthCheck(self: *Pool) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();

        for (0..self.size) |i| {
            if (!self.in_use[i] and !self.connections[i].isAlive()) {
                self.connections[i].reconnect() catch {};
            }
        }
    }
};

pub const PooledConnection = struct {
    conn: *Connection,
    index: usize,
    pool: *Pool,

    pub fn release(self: *PooledConnection) void {
        self.pool.checkin(self.index);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "init and deinit pool" {
    var pool = try Pool.init(.{ .size = 3 });
    defer pool.deinit();
    try std.testing.expectEqual(@as(u16, 3), pool.size);
}

test "checkout and checkin" {
    var pool = try Pool.init(.{ .size = 2 });
    defer pool.deinit();

    var pc = try pool.checkout();
    try std.testing.expectEqual(@as(u16, 1), pool.available());
    pc.release();
    try std.testing.expectEqual(@as(u16, 2), pool.available());
}

test "pool exhaustion" {
    var pool = try Pool.init(.{ .size = 1, .connection = .{}, .checkout_timeout_iterations = 100 });
    defer pool.deinit();

    var pc1 = try pool.checkout();
    defer pc1.release();

    // Second checkout should fail with exhaustion
    const result = pool.checkout();
    try std.testing.expectError(error.PoolExhausted, result);
}

test "checkin makes connection available again" {
    var pool = try Pool.init(.{ .size = 1 });
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
    var pool = try Pool.init(.{ .size = 3 });
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
