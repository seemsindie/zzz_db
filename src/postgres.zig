const std = @import("std");
const backend = @import("backend.zig");

pub const c = @cImport({
    @cInclude("libpq-fe.h");
});

pub const dialect: backend.Dialect = .postgres;

pub const PgError = error{
    ConnectionFailed,
    ExecError,
    QueryError,
    ParseError,
    InternalError,
};

pub const Config = struct {
    database: [:0]const u8 = "host=localhost dbname=postgres",
};

pub const Db = struct {
    conn: *c.PGconn,

    pub fn open(conninfo: [:0]const u8) PgError!Db {
        const pg_conn = c.PQconnectdb(conninfo.ptr) orelse return error.ConnectionFailed;
        if (c.PQstatus(pg_conn) != c.CONNECTION_OK) {
            c.PQfinish(pg_conn);
            return error.ConnectionFailed;
        }
        return .{ .conn = pg_conn };
    }

    pub fn close(self: *Db) void {
        c.PQfinish(self.conn);
    }

    pub fn exec(self: *Db, sql: [:0]const u8) PgError!void {
        const res = c.PQexec(self.conn, sql.ptr) orelse return error.ExecError;
        defer c.PQclear(res);
        const status = c.PQresultStatus(res);
        if (status != c.PGRES_COMMAND_OK and status != c.PGRES_TUPLES_OK) {
            return error.ExecError;
        }
    }

    pub fn isAlive(self: *Db) bool {
        return c.PQstatus(self.conn) == c.CONNECTION_OK;
    }
};

pub const ResultSet = struct {
    result: *c.PGresult,
    current_row: c_int,
    num_rows: c_int,

    pub fn query(db: *Db, sql: [:0]const u8, bind_values: []const ?[]const u8) !ResultSet {
        const n_params: c_int = @intCast(bind_values.len);

        // Build param arrays (stack-allocated, max 64 params)
        var param_values: [64]?[*]const u8 = undefined;
        var param_lengths: [64]c_int = undefined;

        // libpq text format requires null-terminated C strings and ignores
        // paramLengths, so we must null-terminate each value.
        var nul_buf: [4096]u8 = undefined;
        var nul_pos: usize = 0;

        for (bind_values, 0..) |val, i| {
            if (val) |v| {
                if (nul_pos + v.len + 1 <= nul_buf.len) {
                    @memcpy(nul_buf[nul_pos .. nul_pos + v.len], v);
                    nul_buf[nul_pos + v.len] = 0;
                    param_values[i] = @ptrCast(&nul_buf[nul_pos]);
                    nul_pos += v.len + 1;
                } else {
                    // Fallback: value too large for stack buffer
                    param_values[i] = v.ptr;
                }
                param_lengths[i] = @intCast(v.len);
            } else {
                param_values[i] = null;
                param_lengths[i] = 0;
            }
        }

        const res = c.PQexecParams(
            db.conn,
            sql.ptr,
            n_params,
            null, // let PG infer types
            &param_values,
            &param_lengths,
            null, // all text format
            0, // text result format
        ) orelse return error.QueryError;

        const status = c.PQresultStatus(res);
        if (status != c.PGRES_TUPLES_OK) {
            c.PQclear(res);
            return error.QueryError;
        }

        return .{
            .result = res,
            .current_row = -1,
            .num_rows = c.PQntuples(res),
        };
    }

    pub fn next(self: *ResultSet) !bool {
        self.current_row += 1;
        return self.current_row < self.num_rows;
    }

    pub fn columnText(self: *const ResultSet, col: c_int) ?[]const u8 {
        if (c.PQgetisnull(self.result, self.current_row, col) != 0) return null;
        const ptr = c.PQgetvalue(self.result, self.current_row, col);
        if (ptr == null) return null;
        const len = c.PQgetlength(self.result, self.current_row, col);
        if (len <= 0) return "";
        return ptr[0..@intCast(len)];
    }

    pub fn columnInt64(self: *const ResultSet, col: c_int) i64 {
        const text = self.columnText(col) orelse return 0;
        return std.fmt.parseInt(i64, text, 10) catch 0;
    }

    pub fn columnDouble(self: *const ResultSet, col: c_int) f64 {
        const text = self.columnText(col) orelse return 0;
        return std.fmt.parseFloat(f64, text) catch 0;
    }

    pub fn columnIsNull(self: *const ResultSet, col: c_int) bool {
        return c.PQgetisnull(self.result, self.current_row, col) != 0;
    }

    pub fn deinit(self: *ResultSet) void {
        c.PQclear(self.result);
    }
};

pub const ExecResult = struct {
    result: *c.PGresult,
    returned_id: ?i64,
    rows: i32,

    pub fn exec(db: *Db, sql: [:0]const u8, bind_values: []const ?[]const u8) !ExecResult {
        const n_params: c_int = @intCast(bind_values.len);

        var param_values: [64]?[*]const u8 = undefined;
        var param_lengths: [64]c_int = undefined;

        // libpq text format requires null-terminated C strings and ignores
        // paramLengths, so we must null-terminate each value.
        var nul_buf: [4096]u8 = undefined;
        var nul_pos: usize = 0;

        for (bind_values, 0..) |val, i| {
            if (val) |v| {
                if (nul_pos + v.len + 1 <= nul_buf.len) {
                    @memcpy(nul_buf[nul_pos .. nul_pos + v.len], v);
                    nul_buf[nul_pos + v.len] = 0;
                    param_values[i] = @ptrCast(&nul_buf[nul_pos]);
                    nul_pos += v.len + 1;
                } else {
                    // Fallback: value too large for stack buffer
                    param_values[i] = v.ptr;
                }
                param_lengths[i] = @intCast(v.len);
            } else {
                param_values[i] = null;
                param_lengths[i] = 0;
            }
        }

        const res = c.PQexecParams(
            db.conn,
            sql.ptr,
            n_params,
            null,
            &param_values,
            &param_lengths,
            null,
            0,
        ) orelse return error.ExecError;

        const status = c.PQresultStatus(res);
        if (status != c.PGRES_COMMAND_OK and status != c.PGRES_TUPLES_OK) {
            c.PQclear(res);
            return error.ExecError;
        }

        // Extract returned ID from RETURNING clause if present
        var returned_id: ?i64 = null;
        if (status == c.PGRES_TUPLES_OK and c.PQntuples(res) > 0 and c.PQnfields(res) > 0) {
            const id_ptr = c.PQgetvalue(res, 0, 0);
            if (id_ptr != null) {
                const id_len = c.PQgetlength(res, 0, 0);
                if (id_len > 0) {
                    const id_text = id_ptr[0..@intCast(id_len)];
                    returned_id = std.fmt.parseInt(i64, id_text, 10) catch null;
                }
            }
        }

        // Parse rows affected from PQcmdTuples
        var rows_affected: i32 = 0;
        const cmd_tuples = c.PQcmdTuples(res);
        if (cmd_tuples != null) {
            const cmd_str = std.mem.span(cmd_tuples);
            if (cmd_str.len > 0) {
                rows_affected = std.fmt.parseInt(i32, cmd_str, 10) catch 0;
            }
        }

        return .{
            .result = res,
            .returned_id = returned_id,
            .rows = rows_affected,
        };
    }

    pub fn lastInsertId(self: *const ExecResult) i64 {
        return self.returned_id orelse 0;
    }

    pub fn rowsAffected(self: *const ExecResult) i32 {
        return self.rows;
    }

    pub fn deinit(self: *ExecResult) void {
        c.PQclear(self.result);
    }
};
