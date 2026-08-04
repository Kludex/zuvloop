//! Name resolution on libuv's threadpool, resolving straight into a Future.
//!
//! asyncio runs `socket.getaddrinfo` on the default executor; going through
//! libuv keeps resolution off the Python thread pool entirely.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const uv = @import("uv.zig");
const addr = @import("addr.zig");
const loopmod = @import("loop.zig");
const LoopObject = loopmod.LoopObject;

const alloc = std.heap.c_allocator;

var str_create_future: ?*py.Object = null;
var str_set_result: ?*py.Object = null;
var str_set_exception: ?*py.Object = null;
var str_done: ?*py.Object = null;
var address_family: ?*py.Object = null;
var socket_kind: ?*py.Object = null;
var gaierror: ?*py.Object = null;

const max_host = 1024;

const Request = struct {
    loop: *LoopObject,
    future: *py.Object,
    hints: std.c.addrinfo = std.mem.zeroes(std.c.addrinfo),
    host: [max_host]u8 = undefined,
    service: [32]u8 = undefined,

    inline fn addrReq(self: *Request) *uv.GetAddrInfo {
        return @ptrCast(@as([*]u8, @ptrCast(self)) + req_offset);
    }

    inline fn nameReq(self: *Request) *uv.GetNameInfo {
        return @ptrCast(@as([*]u8, @ptrCast(self)) + req_offset);
    }
};

const req_offset = std.mem.alignForward(usize, @sizeOf(Request), 8);

fn allocRequest(loop: *LoopObject, kind: uv.ReqType) py.Error!*Request {
    const size = req_offset + uv.uv_req_size(kind);
    const raw = alloc.alignedAlloc(u8, .@"8", size) catch return py.errNoMemory();
    const self: *Request = @ptrCast(raw.ptr);
    self.* = .{ .loop = loop, .future = undefined };

    const future = c.PyObject_CallMethodNoArgs(@ptrCast(loop), str_create_future) orelse {
        alloc.free(raw);
        return py.Error.Python;
    };
    self.future = future;
    uv.setData(self.addrReq(), self);
    return self;
}

fn freeRequest(self: *Request, kind: uv.ReqType) void {
    py.decref(self.future);
    alloc.free(@as([*]u8, @ptrCast(self))[0 .. req_offset + uv.uv_req_size(kind)]);
}

fn copyZ(dst: []u8, value: *py.Object, what: [:0]const u8) py.Error!?[*:0]const u8 {
    if (py.isNone(value)) return null;
    var len: c.Py_ssize_t = 0;
    var src: [*c]const u8 = undefined;
    if (c.PyUnicode_Check(value) != 0) {
        src = c.PyUnicode_AsUTF8AndSize(value, &len) orelse return py.Error.Python;
    } else if (c.PyBytes_Check(value) != 0) {
        src = c.PyBytes_AsString(value);
        len = c.PyBytes_Size(value);
    } else if (c.PyLong_Check(value) != 0) {
        const n = try py.asCInt(value);
        const rendered = std.fmt.bufPrint(dst[0 .. dst.len - 1], "{d}", .{n}) catch return py.errValue("value out of range");
        dst[rendered.len] = 0;
        return @ptrCast(dst.ptr);
    } else {
        _ = c.PyErr_Format(@ptrCast(c.PyExc_TypeError), "%s must be str, bytes, int or None", what.ptr);
        return py.Error.Python;
    }
    if (@as(usize, @intCast(len)) >= dst.len) return py.errValue("value too long");
    @memcpy(dst[0..@intCast(len)], src[0..@intCast(len)]);
    dst[@intCast(len)] = 0;
    return @ptrCast(dst.ptr);
}

fn settle(future: *py.Object, method: ?*py.Object, value: *py.Object) void {
    const done = c.PyObject_CallMethodNoArgs(future, str_done) orelse {
        py.writeUnraisable(future);
        return;
    };
    defer py.decref(done);
    if (c.PyObject_IsTrue(done) != 0) return;
    const res = c.PyObject_CallMethodOneArg(future, method, value) orelse {
        py.writeUnraisable(future);
        return;
    };
    py.decref(res);
}

fn failFuture(future: *py.Object, status: c_int) void {
    var buf: [128]u8 = undefined;
    const msg = uv.strerror(status, &buf);
    const exc = c.PyObject_CallFunction(gaierror, "is", @as(c_int, status), msg.ptr) orelse {
        py.writeUnraisable(future);
        return;
    };
    defer py.decref(exc);
    settle(future, str_set_exception, exc);
}

fn buildResults(res: ?*std.c.addrinfo) py.Error!*py.Object {
    const list = c.PyList_New(0) orelse return py.Error.Python;
    errdefer py.decref(list);
    var node = res;
    while (node) |ai| : (node = ai.next) {
        const sa = ai.addr orelse continue;
        const item = c.PyTuple_New(5) orelse return py.Error.Python;

        // PyTuple_SetItem steals each reference.
        const fields = [5]?*py.Object{
            c.PyObject_CallFunction(address_family, "i", ai.family),
            c.PyObject_CallFunction(socket_kind, "i", ai.socktype),
            c.PyLong_FromLong(ai.protocol),
            if (ai.canonname) |cn| py.strZ(cn) else py.str(""),
            addr.toPython(sa) catch null,
        };
        for (fields, 0..) |field, i| {
            if (field == null) {
                for (fields[i..]) |rest| py.xdecref(rest);
                py.decref(item);
                return py.Error.Python;
            }
            _ = c.PyTuple_SetItem(item, @intCast(i), field);
        }
        const appended = c.PyList_Append(list, item);
        py.decref(item);
        if (appended < 0) return py.Error.Python;
    }
    return list;
}

fn onAddrInfo(req: ?*uv.GetAddrInfo, status: c_int, res: ?*std.c.addrinfo) callconv(.c) void {
    const self: *Request = @ptrCast(@alignCast(uv.getData(req.?)));
    const st = self.loop.state();
    st.gilEnter();
    defer st.gilExit();

    if (status < 0) {
        failFuture(self.future, status);
    } else if (buildResults(res)) |list| {
        defer py.decref(list);
        settle(self.future, str_set_result, list);
    } else |_| {
        const exc = c.PyErr_GetRaisedException();
        if (exc) |e| {
            defer py.decref(e);
            settle(self.future, str_set_exception, e);
        }
    }
    uv.uv_freeaddrinfo(res);
    freeRequest(self, .getaddrinfo);
}

fn onNameInfo(req: ?*uv.GetNameInfo, status: c_int, hostname: ?[*:0]const u8, service: ?[*:0]const u8) callconv(.c) void {
    const self: *Request = @ptrCast(@alignCast(uv.getData(req.?)));
    const st = self.loop.state();
    st.gilEnter();
    defer st.gilExit();

    if (status < 0) {
        failFuture(self.future, status);
    } else if (c.Py_BuildValue("ss", hostname orelse "", service orelse "")) |pair| {
        defer py.decref(pair);
        settle(self.future, str_set_result, pair);
    } else {
        py.writeUnraisable(self.future);
    }
    freeRequest(self, .getnameinfo);
}

/// `_getaddrinfo(host, port, family, type, proto, flags)`
pub fn getaddrinfo(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    try py.expectArgs(args, 6, "_getaddrinfo");
    const loop = loopmod.asLoop(self_obj);
    try loopmod.checkClosed(loop.state());

    const req = try allocRequest(loop, .getaddrinfo);
    errdefer freeRequest(req, .getaddrinfo);

    const host = try copyZ(&req.host, args[0].?, "host");
    const service = try copyZ(&req.service, args[1].?, "port");
    req.hints.family = try py.asCInt(args[2].?);
    req.hints.socktype = try py.asCInt(args[3].?);
    req.hints.protocol = try py.asCInt(args[4].?);
    req.hints.flags = @bitCast(@as(u32, @bitCast(try py.asCInt(args[5].?))));

    try py.errUvIfNeg(uv.uv_getaddrinfo(loop.state().uvloop, req.addrReq(), onAddrInfo, host, service, &req.hints));
    return py.newref(req.future).?;
}

/// `_getnameinfo(sockaddr, flags)`
pub fn getnameinfo(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    try py.expectArgs(args, 2, "_getnameinfo");
    const loop = loopmod.asLoop(self_obj);
    try loopmod.checkClosed(loop.state());

    var storage: addr.Storage = .{};
    try addr.fromPython(0, args[0].?, &storage);
    const flags = try py.asCInt(args[1].?);

    const req = try allocRequest(loop, .getnameinfo);
    errdefer freeRequest(req, .getnameinfo);
    try py.errUvIfNeg(uv.uv_getnameinfo(loop.state().uvloop, req.nameReq(), onNameInfo, storage.constPtr(), flags));
    return py.newref(req.future).?;
}

pub fn register() py.Error!void {
    str_create_future = py.intern("create_future") orelse return py.Error.Python;
    str_set_result = py.intern("set_result") orelse return py.Error.Python;
    str_set_exception = py.intern("set_exception") orelse return py.Error.Python;
    str_done = py.intern("done") orelse return py.Error.Python;
    address_family = py.importFrom("socket", "AddressFamily") orelse return py.Error.Python;
    socket_kind = py.importFrom("socket", "SocketKind") orelse return py.Error.Python;
    gaierror = py.importFrom("socket", "gaierror") orelse return py.Error.Python;
}
