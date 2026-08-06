//! Conversions between Python address tuples and `struct sockaddr`.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const uv = @import("uv.zig");

const posix = std.posix;

pub const Storage = extern struct {
    bytes: [128]u8 align(8) = @splat(0),

    pub inline fn ptr(self: *Storage) *posix.sockaddr {
        return @ptrCast(self);
    }

    pub inline fn constPtr(self: *const Storage) *const posix.sockaddr {
        return @ptrCast(self);
    }
};

pub const AF_INET: c_int = posix.AF.INET;
pub const AF_INET6: c_int = posix.AF.INET6;
pub const AF_UNIX: c_int = posix.AF.UNIX;

fn tupleItem(t: *py.Object, i: c.Py_ssize_t) py.Error!*py.Object {
    return c.PyTuple_GetItem(t, i) orelse py.Error.Python;
}

/// Fills `out` from `(host, port[, flowinfo, scopeid])` or a filesystem path.
pub fn fromPython(family: c_int, address: *py.Object, out: *Storage) py.Error!void {
    out.* = .{};
    if (family == AF_UNIX) {
        var path: [*c]const u8 = undefined;
        var len: c.Py_ssize_t = 0;
        if (c.PyUnicode_Check(address) != 0) {
            path = c.PyUnicode_AsUTF8AndSize(address, &len) orelse return py.Error.Python;
        } else if (c.PyBytes_Check(address) != 0) {
            path = c.PyBytes_AsString(address);
            len = c.PyBytes_Size(address);
        } else {
            return py.errType("AF_UNIX addresses must be str or bytes");
        }
        const un: *posix.sockaddr.un = @ptrCast(out);
        if (len >= un.path.len) return py.errValue("AF_UNIX path too long");
        un.family = @intCast(AF_UNIX);
        @memcpy(un.path[0..@intCast(len)], path[0..@intCast(len)]);
        return;
    }

    if (c.PyTuple_Check(address) == 0) return py.errType("address must be a tuple");
    const size = c.PyTuple_Size(address);
    if (size < 2) return py.errType("address tuple must be (host, port)");

    const host_obj = try tupleItem(address, 0);
    // Read wide and saturating, then narrow: converting to `c_int` first would
    // refuse a port above `INT_MAX` with an error about the width of the type,
    // before the range check below ever saw it.
    const port_value = try py.asIsizeClamped(try tupleItem(address, 1));
    // `uv_ip4_addr` stores `htons(port)`, which truncates rather than complains:
    // a port of 70000 would quietly address 4464. The standard library rejects it.
    if (port_value < 0 or port_value > 65535) return py.errOverflow("port must be 0-65535");
    const port: c_int = @intCast(port_value);

    var host_buf: [256]u8 = undefined;
    const host: [*:0]const u8 = blk: {
        if (py.isNone(host_obj)) break :blk if (family == AF_INET6) "::" else "0.0.0.0";
        var len: c.Py_ssize_t = 0;
        const s = c.PyUnicode_AsUTF8AndSize(host_obj, &len) orelse return py.Error.Python;
        if (len >= host_buf.len) return py.errValue("host name too long");
        @memcpy(host_buf[0..@intCast(len)], s[0..@intCast(len)]);
        host_buf[@intCast(len)] = 0;
        break :blk @ptrCast(&host_buf);
    };

    if (family == AF_INET6 or (family == 0 and std.mem.indexOfScalar(u8, std.mem.span(host), ':') != null)) {
        const sin6: *posix.sockaddr.in6 = @ptrCast(out);
        try py.errUvIfNeg(uv.uv_ip6_addr(host, port, sin6));
        if (size >= 3) sin6.flowinfo = @intCast(try py.asCInt(try tupleItem(address, 2)));
        if (size >= 4) sin6.scope_id = @intCast(try py.asCInt(try tupleItem(address, 3)));
        return;
    }
    const sin: *posix.sockaddr.in = @ptrCast(out);
    try py.errUvIfNeg(uv.uv_ip4_addr(host, port, sin));
}

/// Builds the tuple `socket.getsockname()` would return for `sa`.
pub fn toPython(sa: *const posix.sockaddr) py.Error!*py.Object {
    const family: c_int = sa.family;
    if (family == AF_UNIX) {
        const un: *const posix.sockaddr.un = @ptrCast(@alignCast(sa));
        const len = std.mem.indexOfScalar(u8, &un.path, 0) orelse un.path.len;
        return py.str(un.path[0..len]) orelse py.Error.Python;
    }
    if (family != AF_INET and family != AF_INET6) return py.noneRef();

    var buf: [64]u8 = undefined;
    try py.errUvIfNeg(uv.uv_ip_name(sa, &buf, buf.len));
    const host = std.mem.sliceTo(&buf, 0);

    if (family == AF_INET) {
        const sin: *const posix.sockaddr.in = @ptrCast(@alignCast(sa));
        return c.Py_BuildValue("s#i", host.ptr, @as(c.Py_ssize_t, @intCast(host.len)), @as(c_int, std.mem.bigToNative(u16, sin.port))) orelse py.Error.Python;
    }
    const sin6: *const posix.sockaddr.in6 = @ptrCast(@alignCast(sa));
    return c.Py_BuildValue(
        "s#iII",
        host.ptr,
        @as(c.Py_ssize_t, @intCast(host.len)),
        @as(c_int, std.mem.bigToNative(u16, sin6.port)),
        @as(c_uint, sin6.flowinfo),
        @as(c_uint, sin6.scope_id),
    ) orelse py.Error.Python;
}
