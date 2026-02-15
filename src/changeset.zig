const std = @import("std");

pub const FieldError = struct {
    field: []const u8,
    message: []const u8,
};

/// A simple string-keyed map for use in tests and param parsing.
pub const StringMap = struct {
    entries: []const Entry,

    pub const Entry = struct { key: []const u8, value: []const u8 };

    pub fn get(self: StringMap, key: []const u8) ?[]const u8 {
        for (self.entries) |entry| {
            if (std.mem.eql(u8, entry.key, key)) return entry.value;
        }
        return null;
    }
};

pub fn Changeset(comptime T: type) type {
    const struct_fields = @typeInfo(T).@"struct".fields;
    const N = struct_fields.len;

    return struct {
        const Self = @This();
        const max_errors = 32;

        data: T,
        changes: [N]bool = [_]bool{false} ** N,
        errors: [max_errors]FieldError = undefined,
        error_count: usize = 0,

        pub fn init(data: T) Self {
            return .{ .data = data };
        }

        pub fn valid(self: Self) bool {
            return self.error_count == 0;
        }

        pub fn getErrors(self: *const Self) []const FieldError {
            return self.errors[0..self.error_count];
        }

        pub fn errorsOn(self: *const Self, field: []const u8) bool {
            for (self.errors[0..self.error_count]) |err| {
                if (std.mem.eql(u8, err.field, field)) return true;
            }
            return false;
        }

        fn addError(self: *Self, field: []const u8, message: []const u8) void {
            if (self.error_count < max_errors) {
                self.errors[self.error_count] = .{ .field = field, .message = message };
                self.error_count += 1;
            }
        }

        /// Cast params into the changeset. Only whitelisted fields are accepted.
        /// `params` must have a `get(key: []const u8) -> ?[]const u8` method.
        pub fn cast(self: *Self, comptime allowed: []const []const u8, params: anytype) *Self {
            inline for (struct_fields, 0..) |f, idx| {
                comptime var is_allowed = false;
                inline for (allowed) |a| {
                    if (comptime std.mem.eql(u8, f.name, a)) {
                        is_allowed = true;
                    }
                }
                if (is_allowed) {
                    if (params.get(f.name)) |str_val| {
                        if (comptime isStringField(f.type)) {
                            @field(self.data, f.name) = str_val;
                            self.changes[idx] = true;
                        } else if (comptime isIntField(f.type)) {
                            if (parseInt(f.type, str_val)) |v| {
                                @field(self.data, f.name) = v;
                                self.changes[idx] = true;
                            }
                        } else if (comptime isFloatField(f.type)) {
                            if (parseFloat(f.type, str_val)) |v| {
                                @field(self.data, f.name) = v;
                                self.changes[idx] = true;
                            }
                        } else if (f.type == bool) {
                            @field(self.data, f.name) = std.mem.eql(u8, str_val, "true") or std.mem.eql(u8, str_val, "1");
                            self.changes[idx] = true;
                        }
                    }
                }
            }
            return self;
        }

        /// Validate that listed fields are present (changed) and non-empty strings.
        pub fn validateRequired(self: *Self, comptime fields: []const []const u8) *Self {
            inline for (struct_fields, 0..) |f, idx| {
                inline for (fields) |req| {
                    if (comptime std.mem.eql(u8, f.name, req)) {
                        if (!self.changes[idx]) {
                            self.addError(f.name, "is required");
                        } else if (comptime isStringField(f.type)) {
                            const val = @field(self.data, f.name);
                            if (val.len == 0) {
                                self.addError(f.name, "can't be blank");
                            }
                        }
                    }
                }
            }
            return self;
        }

        /// Validate string length bounds.
        pub fn validateLength(self: *Self, comptime field: []const u8, opts: struct { min: ?usize = null, max: ?usize = null }) *Self {
            const val = @field(self.data, field);
            if (comptime isStringField(@TypeOf(val))) {
                if (opts.min) |min| {
                    if (val.len < min) {
                        self.addError(field, "is too short");
                    }
                }
                if (opts.max) |max| {
                    if (val.len > max) {
                        self.addError(field, "is too long");
                    }
                }
            }
            return self;
        }

        /// Validate string contains a substring.
        pub fn validateFormat(self: *Self, comptime field: []const u8, contains: []const u8, message: []const u8) *Self {
            const val = @field(self.data, field);
            if (comptime isStringField(@TypeOf(val))) {
                if (std.mem.indexOf(u8, val, contains) == null) {
                    self.addError(field, message);
                }
            }
            return self;
        }

        /// Validate numeric range.
        pub fn validateNumber(self: *Self, comptime field: []const u8, opts: struct { greater_than: ?f64 = null, less_than: ?f64 = null }) *Self {
            const val = @field(self.data, field);
            const num: f64 = if (comptime isIntField(@TypeOf(val)))
                @floatFromInt(val)
            else if (comptime isFloatField(@TypeOf(val)))
                @floatCast(val)
            else
                return self;

            if (opts.greater_than) |gt| {
                if (num <= gt) {
                    self.addError(field, "must be greater than limit");
                }
            }
            if (opts.less_than) |lt| {
                if (num >= lt) {
                    self.addError(field, "must be less than limit");
                }
            }
            return self;
        }

        /// Validate value is in an allowed list.
        pub fn validateInclusion(self: *Self, comptime field: []const u8, comptime values: []const []const u8) *Self {
            const val = @field(self.data, field);
            if (comptime isStringField(@TypeOf(val))) {
                var found = false;
                inline for (values) |v| {
                    if (std.mem.eql(u8, val, v)) found = true;
                }
                if (!found) {
                    self.addError(field, "is not included in the list");
                }
            }
            return self;
        }

        /// Validate value is NOT in a disallowed list.
        pub fn validateExclusion(self: *Self, comptime field: []const u8, comptime values: []const []const u8) *Self {
            const val = @field(self.data, field);
            if (comptime isStringField(@TypeOf(val))) {
                inline for (values) |v| {
                    if (std.mem.eql(u8, val, v)) {
                        self.addError(field, "is reserved");
                        return self;
                    }
                }
            }
            return self;
        }

        /// Run a custom validator function.
        pub fn validate(self: *Self, func: *const fn (*Self) void) *Self {
            func(self);
            return self;
        }

        /// Mark a field for unique constraint check (deferred to DB layer).
        pub fn uniqueConstraint(self: *Self, comptime field: []const u8) *Self {
            _ = field;
            // Marker only — actual enforcement happens at DB insert time.
            // If a constraint violation occurs, the caller maps the DB error to a changeset error.
            return self;
        }

        /// Mark a field for foreign key constraint check (deferred to DB layer).
        pub fn foreignKeyConstraint(self: *Self, comptime field: []const u8) *Self {
            _ = field;
            return self;
        }

        /// Manually set a field value.
        pub fn putChange(self: *Self, comptime field: []const u8, value: @TypeOf(@field(self.data, field))) *Self {
            @field(self.data, field) = value;
            inline for (struct_fields, 0..) |f, idx| {
                if (comptime std.mem.eql(u8, f.name, field)) {
                    self.changes[idx] = true;
                }
            }
            return self;
        }
    };
}

// ── Helpers ───────────────────────────────────────────────────────────

fn isStringField(comptime T: type) bool {
    return T == []const u8;
}

fn isIntField(comptime T: type) bool {
    return T == i64 or T == i32 or T == u32 or T == u64 or T == i16 or T == u16;
}

fn isFloatField(comptime T: type) bool {
    return T == f64 or T == f32;
}

fn parseInt(comptime T: type, s: []const u8) ?T {
    return std.fmt.parseInt(T, s, 10) catch null;
}

fn parseFloat(comptime T: type, s: []const u8) ?T {
    return std.fmt.parseFloat(T, s) catch null;
}

// ── Tests ──────────────────────────────────────────────────────────────

const TestUser = struct {
    id: i64 = 0,
    name: []const u8 = "",
    email: []const u8 = "",
    age: i32 = 0,
    score: f64 = 0.0,
};

test "cast parses string params into typed fields" {
    const params = StringMap{ .entries = &.{
        .{ .key = "name", .value = "Alice" },
        .{ .key = "email", .value = "alice@example.com" },
        .{ .key = "age", .value = "30" },
        .{ .key = "score", .value = "95.5" },
    } };

    var cs = Changeset(TestUser).init(.{});
    _ = cs.cast(&.{ "name", "email", "age", "score" }, params);

    try std.testing.expectEqualStrings("Alice", cs.data.name);
    try std.testing.expectEqualStrings("alice@example.com", cs.data.email);
    try std.testing.expectEqual(@as(i32, 30), cs.data.age);
    try std.testing.expectApproxEqAbs(@as(f64, 95.5), cs.data.score, 0.001);
    try std.testing.expect(cs.valid());
}

test "validateRequired catches blank and missing fields" {
    var cs = Changeset(TestUser).init(.{});
    _ = cs.cast(&.{"name"}, StringMap{ .entries = &.{
        .{ .key = "name", .value = "" },
    } });
    _ = cs.validateRequired(&.{ "name", "email" });

    try std.testing.expect(!cs.valid());
    try std.testing.expect(cs.errorsOn("name"));
    try std.testing.expect(cs.errorsOn("email"));
}

test "validateLength min and max" {
    var cs = Changeset(TestUser).init(.{});
    _ = cs.cast(&.{"name"}, StringMap{ .entries = &.{
        .{ .key = "name", .value = "AB" },
    } });
    _ = cs.validateLength("name", .{ .min = 3 });
    try std.testing.expect(!cs.valid());
    try std.testing.expect(cs.errorsOn("name"));

    var cs2 = Changeset(TestUser).init(.{});
    _ = cs2.cast(&.{"name"}, StringMap{ .entries = &.{
        .{ .key = "name", .value = "A very long name that exceeds the maximum" },
    } });
    _ = cs2.validateLength("name", .{ .max = 10 });
    try std.testing.expect(!cs2.valid());
}

test "validateFormat substring check" {
    var cs = Changeset(TestUser).init(.{});
    _ = cs.cast(&.{"email"}, StringMap{ .entries = &.{
        .{ .key = "email", .value = "invalid-email" },
    } });
    _ = cs.validateFormat("email", "@", "must contain @");

    try std.testing.expect(!cs.valid());
    try std.testing.expect(cs.errorsOn("email"));

    var cs2 = Changeset(TestUser).init(.{});
    _ = cs2.cast(&.{"email"}, StringMap{ .entries = &.{
        .{ .key = "email", .value = "user@example.com" },
    } });
    _ = cs2.validateFormat("email", "@", "must contain @");
    try std.testing.expect(cs2.valid());
}

test "validateNumber range" {
    var cs = Changeset(TestUser).init(.{});
    _ = cs.cast(&.{"age"}, StringMap{ .entries = &.{
        .{ .key = "age", .value = "0" },
    } });
    _ = cs.validateNumber("age", .{ .greater_than = 0 });
    try std.testing.expect(!cs.valid());

    var cs2 = Changeset(TestUser).init(.{});
    _ = cs2.cast(&.{"age"}, StringMap{ .entries = &.{
        .{ .key = "age", .value = "200" },
    } });
    _ = cs2.validateNumber("age", .{ .less_than = 150 });
    try std.testing.expect(!cs2.valid());
}

test "validateInclusion and validateExclusion" {
    var cs = Changeset(TestUser).init(.{});
    _ = cs.cast(&.{"name"}, StringMap{ .entries = &.{
        .{ .key = "name", .value = "unknown" },
    } });
    _ = cs.validateInclusion("name", &.{ "Alice", "Bob", "Carol" });
    try std.testing.expect(!cs.valid());

    var cs2 = Changeset(TestUser).init(.{});
    _ = cs2.cast(&.{"name"}, StringMap{ .entries = &.{
        .{ .key = "name", .value = "admin" },
    } });
    _ = cs2.validateExclusion("name", &.{ "admin", "root" });
    try std.testing.expect(!cs2.valid());
}

test "custom validator" {
    var cs = Changeset(TestUser).init(.{});
    _ = cs.cast(&.{"name"}, StringMap{ .entries = &.{
        .{ .key = "name", .value = "x" },
    } });
    _ = cs.validate(&struct {
        fn v(c: *Changeset(TestUser)) void {
            if (std.mem.eql(u8, c.data.name, "x")) {
                c.addError("name", "must not be x");
            }
        }
    }.v);
    try std.testing.expect(!cs.valid());
    try std.testing.expect(cs.errorsOn("name"));
}

test "chaining validations, valid() and getErrors()" {
    const params = StringMap{ .entries = &.{
        .{ .key = "name", .value = "Alice" },
        .{ .key = "email", .value = "alice@example.com" },
        .{ .key = "age", .value = "25" },
    } };

    var cs = Changeset(TestUser).init(.{});
    _ = cs.cast(&.{ "name", "email", "age" }, params);
    _ = cs.validateRequired(&.{ "name", "email" });
    _ = cs.validateLength("name", .{ .min = 2, .max = 50 });
    _ = cs.validateFormat("email", "@", "must contain @");
    _ = cs.validateNumber("age", .{ .greater_than = 0, .less_than = 150 });

    try std.testing.expect(cs.valid());
    try std.testing.expectEqual(@as(usize, 0), cs.getErrors().len);

    // putChange
    _ = cs.putChange("name", "Bob");
    try std.testing.expectEqualStrings("Bob", cs.data.name);
}
