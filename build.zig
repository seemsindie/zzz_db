const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    _ = b.standardOptimizeOption(.{});

    const sqlite_enabled = b.option(bool, "sqlite", "Enable SQLite support") orelse true;
    const postgres_enabled = b.option(bool, "postgres", "Enable PostgreSQL support") orelse false;

    // Create a module for the DB build options so source can query at comptime
    const db_options = b.addOptions();
    db_options.addOption(bool, "sqlite_enabled", sqlite_enabled);
    db_options.addOption(bool, "postgres_enabled", postgres_enabled);

    const mod = b.addModule("zzz_db", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    mod.addImport("db_options", db_options.createModule());

    if (sqlite_enabled) {
        mod.addCSourceFiles(.{
            .files = &.{"vendor/sqlite3/sqlite3.c"},
            .flags = &.{"-DSQLITE_THREADSAFE=1"},
        });
        mod.addIncludePath(b.path("vendor/sqlite3"));
        mod.link_libc = true;
    }

    const libpq_dep = if (postgres_enabled)
        b.dependency("libpq", .{ .target = target, .ssl = .None, .@"disable-zlib" = true, .@"disable-zstd" = true })
    else
        null;

    if (postgres_enabled) {
        mod.linkLibrary(libpq_dep.?.artifact("pq"));
        mod.link_libc = true;
    }

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    // No need to re-add C sources or libraries here — mod_tests shares mod's
    // root_module, so all C sources, include paths, and linked libraries are
    // already attached.

    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
}
