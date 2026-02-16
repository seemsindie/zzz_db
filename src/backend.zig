const std = @import("std");

pub const Dialect = enum {
    sqlite,
    postgres,
};

/// Comptime validation that a backend module has all required declarations.
pub fn validate(comptime Backend: type) void {
    // Must have a dialect constant
    if (!@hasDecl(Backend, "dialect")) {
        @compileError("Backend missing 'dialect' declaration");
    }
    const d = Backend.dialect;
    if (@TypeOf(d) != Dialect) {
        @compileError("Backend 'dialect' must be of type backend.Dialect");
    }

    // Must have Db type with required methods
    if (!@hasDecl(Backend, "Db")) {
        @compileError("Backend missing 'Db' type");
    }

    // Must have Config type
    if (!@hasDecl(Backend, "Config")) {
        @compileError("Backend missing 'Config' type");
    }

    // Must have ResultSet type
    if (!@hasDecl(Backend, "ResultSet")) {
        @compileError("Backend missing 'ResultSet' type");
    }

    // Must have ExecResult type
    if (!@hasDecl(Backend, "ExecResult")) {
        @compileError("Backend missing 'ExecResult' type");
    }
}

test "validate accepts sqlite backend" {
    const sqlite = @import("sqlite.zig");
    validate(sqlite);
}
