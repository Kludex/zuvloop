//! Name resolution on libuv's threadpool, resolving straight into a Future.
//!
//! asyncio runs `socket.getaddrinfo` on the default executor; going through
//! libuv keeps resolution off the Python thread pool entirely.

const std = @import("std");
const builtin = @import("builtin");
const py = @import("py.zig");
const c = py.c;
const uv = @import("uv.zig");
const addr = @import("addr.zig");
const loopmod = @import("loop.zig");
const LoopObject = loopmod.LoopObject;

const posix = std.posix;

/// `std.c`'s resolver surface has holes on Windows targets, so the same shapes
/// come from `win32.zig` there.
const netdb = if (builtin.os.tag == .windows) @import("win32.zig") else std.c;

/// libc's own parser rather than Zig's: what it accepts has to match what
/// `getaddrinfo` would have accepted, since the two answer the same calls.
extern fn inet_pton(af: c_int, src: [*:0]const u8, dst: *anyopaque) c_int;

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
    state: *loopmod.State,
    future: ?*py.Object,
    kind: uv.ReqType,
    prev: ?*Request = null,
    next: ?*Request = null,
    hints: netdb.addrinfo = std.mem.zeroes(netdb.addrinfo),
    // SAFETY: request construction fills this array before libuv receives it.
    host: [max_host]u8 = undefined,
    // SAFETY: request construction fills this array before libuv receives it.
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
    const st = loop.state();
    self.* = .{ .state = st, .future = null, .kind = kind };

    const future = c.PyObject_CallMethodNoArgs(@ptrCast(loop), str_create_future) orelse {
        alloc.free(raw);
        return py.Error.Python;
    };
    self.future = future;
    self.next = @ptrCast(@alignCast(st.dns_requests));
    if (self.next) |next| next.prev = self;
    st.dns_requests = self;
    uv.setData(self.addrReq(), self);
    return self;
}

fn freeRequest(self: *Request, kind: uv.ReqType) void {
    const st = self.state;
    if (self.prev) |prev| {
        prev.next = self.next;
    } else {
        st.dns_requests = self.next;
    }
    if (self.next) |next| next.prev = self.prev;
    py.xdecref(self.future);
    alloc.free(@as([*]u8, @ptrCast(self))[0 .. req_offset + uv.uv_req_size(kind)]);
}

/// Cancels every resolver request owned by `st`. Completion callbacks still
/// run and are responsible for unlinking and freeing their request.
pub fn cancelAll(st: *loopmod.State) void {
    var node: ?*Request = @ptrCast(@alignCast(st.dns_requests));
    while (node) |req| : (node = req.next) {
        const raw = switch (req.kind) {
            .getaddrinfo => uv.asReq(req.addrReq()),
            .getnameinfo => uv.asReq(req.nameReq()),
            else => unreachable,
        };
        _ = uv.uv_cancel(raw);
    }
}

/// Drops every Python future before the remaining native requests move to the
/// background reaper. Their callbacks become purely native from this point.
pub fn releaseFutures(st: *loopmod.State) void {
    var node: ?*Request = @ptrCast(@alignCast(st.dns_requests));
    while (node) |req| : (node = req.next) py.clear(&req.future);
}

/// Exposes the futures owned by native libuv requests to cyclic GC. A Future
/// points back to its loop, so hiding this edge would make an abandoned request
/// look like an external root and prevent the loop finalizer from cancelling it.
pub fn traverse(st: *loopmod.State, visitproc: c.visitproc, arg: ?*anyopaque) c_int {
    var node: ?*Request = @ptrCast(@alignCast(st.dns_requests));
    while (node) |req| : (node = req.next) {
        const r = py.visit(req.future, visitproc, arg);
        if (r != 0) return r;
    }
    return 0;
}

fn copyZ(dst: []u8, value: *py.Object, what: [:0]const u8) py.Error!?[*:0]const u8 {
    if (py.isNone(value)) return null;
    var len: c.Py_ssize_t = 0;
    // SAFETY: each accepted Python type assigns src, and every other type returns.
    var src: [*c]const u8 = undefined;
    if (py.isUnicode(value)) {
        src = c.PyUnicode_AsUTF8AndSize(value, &len) orelse return py.Error.Python;
    } else if (py.isBytes(value)) {
        src = c.PyBytes_AsString(value);
        len = c.PyBytes_Size(value);
    } else if (py.isLong(value)) {
        const n = try py.asCInt(value);
        const rendered = std.fmt.bufPrint(dst[0 .. dst.len - 1], "{d}", .{n}) catch return py.errValue("value out of range");
        dst[rendered.len] = 0;
        return @ptrCast(dst.ptr);
    } else {
        _ = c.PyErr_Format(py.exc_type_error, "%s must be str, bytes, int or None", what.ptr);
        return py.Error.Python;
    }
    const value_len: usize = @intCast(len);
    if (std.mem.indexOfScalar(u8, src[0..value_len], 0) != null) return py.errValue("embedded null byte");
    if (value_len >= dst.len) return py.errValue("value too long");
    @memcpy(dst[0..value_len], src[0..value_len]);
    dst[value_len] = 0;
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

/// The member if this platform has it, and a stand-in if it does not: glibc
/// carries no `EAI_BADHINTS` or `EAI_PROTOCOL`, and the codes are not the same
/// numbers across platforms either.
inline fn eai(comptime name: [:0]const u8, comptime fallback: netdb.EAI) netdb.EAI {
    return if (@hasField(netdb.EAI, name)) @field(netdb.EAI, name) else fallback;
}

/// libuv reports resolver failures with codes of its own; `socket.gaierror`
/// carries the platform's `EAI_*`, which is what callers written against the
/// standard library compare against. Anything that is not a resolver failure -
/// a cancellation, an out-of-memory - is left to `OSError`.
fn resolverError(status: c_int) ?netdb.EAI {
    return switch (status) {
        uv.EAI_ADDRFAMILY => eai("ADDRFAMILY", .FAIL),
        uv.EAI_AGAIN => .AGAIN,
        uv.EAI_BADFLAGS => .BADFLAGS,
        uv.EAI_BADHINTS => eai("BADHINTS", .FAIL),
        uv.EAI_FAIL => .FAIL,
        uv.EAI_FAMILY => .FAMILY,
        uv.EAI_MEMORY => .MEMORY,
        uv.EAI_NODATA => eai("NODATA", .NONAME),
        uv.EAI_NONAME => .NONAME,
        uv.EAI_OVERFLOW => eai("OVERFLOW", .FAIL),
        uv.EAI_PROTOCOL => eai("PROTOCOL", .FAIL),
        uv.EAI_SERVICE => .SERVICE,
        uv.EAI_SOCKTYPE => .SOCKTYPE,
        else => null,
    };
}

/// Builds the exception the standard library would have raised for `status`.
fn resolverException(status: c_int) ?*py.Object {
    if (resolverError(status)) |code| {
        return c.PyObject_CallFunction(gaierror, "is", @intFromEnum(code), netdb.gai_strerror(code));
    }
    var buf: [128]u8 = undefined;
    const msg = uv.strerror(status, &buf);
    return c.PyObject_CallFunction(py.exc_os_error, "is", uv.toErrno(status), msg.ptr);
}

/// Raises `socket.gaierror` for a code the platform's resolver produced itself.
fn raisePlatformError(code: netdb.EAI) py.Error {
    // `EAI_SYSTEM` says only that the real error is in `errno`, so the standard
    // library reports that instead - `set_gaierror` in CPython's socketmodule
    // hands it straight to `PyErr_SetFromErrno`. Windows has no such code.
    if (@hasField(netdb.EAI, "SYSTEM") and code == eai("SYSTEM", .FAIL)) {
        _ = c.PyErr_SetFromErrno(py.exc_os_error);
        return py.Error.Python;
    }
    const exc = c.PyObject_CallFunction(gaierror, "is", @intFromEnum(code), netdb.gai_strerror(code)) orelse
        return py.Error.Python;
    c.PyErr_SetRaisedException(exc);
    return py.Error.Python;
}

/// Raises it, for the paths that report synchronously rather than through a future.
/// `PyErr_SetRaisedException` takes the reference, and needs no separate type -
/// `PyObject_Type` would hand back one more to own.
fn raiseResolverError(status: c_int) py.Error {
    const exc = resolverException(status) orelse return py.Error.Python;
    c.PyErr_SetRaisedException(exc);
    return py.Error.Python;
}

fn failFuture(future: *py.Object, status: c_int) void {
    const exc = resolverException(status) orelse {
        py.writeUnraisable(future);
        return;
    };
    defer py.decref(exc);
    settle(future, str_set_exception, exc);
}

/// `socket.AddressFamily(2)` costs over a hundred nanoseconds - the value goes
/// through `EnumMeta.__call__` - and every result tuple needs one of those plus
/// a `SocketKind`. The members are singletons, so only the first lookup of each
/// value ever has to pay for it.
var family_cache: [64]?*py.Object = @splat(null);
var kind_cache: [64]?*py.Object = @splat(null);

fn cachedEnum(cache: []?*py.Object, ctor: ?*py.Object, value: c_int) ?*py.Object {
    if (value < 0 or value >= cache.len) return c.PyObject_CallFunction(ctor, "i", value);
    const slot = &cache[@intCast(value)];
    if (slot.*) |member| return py.newref(member);
    const member = c.PyObject_CallFunction(ctor, "i", value) orelse return null;
    slot.* = py.newref(member);
    return member;
}

fn buildResults(res: ?*netdb.addrinfo) py.Error!*py.Object {
    const list = c.PyList_New(0) orelse return py.Error.Python;
    errdefer py.decref(list);
    var node = res;
    while (node) |ai| : (node = ai.next) {
        const sa = ai.addr orelse continue;
        const item = c.PyTuple_New(5) orelse return py.Error.Python;

        // PyTuple_SetItem steals each reference.
        const fields = [5]?*py.Object{
            cachedEnum(&family_cache, address_family, ai.family),
            cachedEnum(&kind_cache, socket_kind, ai.socktype),
            c.PyLong_FromLong(ai.protocol),
            if (ai.canonname) |cn| py.strZ(cn) else py.str(""),
            addr.toPython(sa, @intCast(ai.addrlen)) catch null,
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

fn onAddrInfo(req: ?*uv.GetAddrInfo, status: c_int, res: ?*netdb.addrinfo) callconv(.c) void {
    const self: *Request = @ptrCast(@alignCast(uv.getData(req.?)));
    const st = self.state;
    if (loopmod.isReaping(st)) {
        uv.uv_freeaddrinfo(res);
        freeRequest(self, .getaddrinfo);
        return;
    }
    st.pythonEnter();
    defer st.pythonExit();

    if (!st.closed) {
        if (status < 0) {
            failFuture(self.future.?, status);
        } else if (buildResults(res)) |list| {
            defer py.decref(list);
            settle(self.future.?, str_set_result, list);
        } else |_| {
            const exc = c.PyErr_GetRaisedException();
            if (exc) |e| {
                defer py.decref(e);
                settle(self.future.?, str_set_exception, e);
            }
        }
    }
    uv.uv_freeaddrinfo(res);
    freeRequest(self, .getaddrinfo);
}

fn onNameInfo(req: ?*uv.GetNameInfo, status: c_int, hostname: ?[*:0]const u8, service: ?[*:0]const u8) callconv(.c) void {
    const self: *Request = @ptrCast(@alignCast(uv.getData(req.?)));
    const st = self.state;
    if (loopmod.isReaping(st)) {
        freeRequest(self, .getnameinfo);
        return;
    }
    st.pythonEnter();
    defer st.pythonExit();

    if (!st.closed) {
        if (status < 0) {
            failFuture(self.future.?, status);
        } else if (c.Py_BuildValue("ss", hostname orelse "", service orelse "")) |pair| {
            defer py.decref(pair);
            settle(self.future.?, str_set_result, pair);
        } else {
            const exc = c.PyErr_GetRaisedException();
            if (exc) |err| {
                defer py.decref(err);
                settle(self.future.?, str_set_exception, err);
            }
        }
    }
    freeRequest(self, .getnameinfo);
}

/// Resolves a literal address without leaving the calling thread.
///
/// Numeric hosts are the common case for `create_connection` and friends, and
/// `AI_NUMERICHOST | AI_NUMERICSERV` guarantees libc answers from the string
/// alone - no resolver, no threadpool hop.
/// Answers a plain address literal without entering libc at all.
///
/// `getaddrinfo` costs around half a microsecond even when `AI_NUMERICHOST`
/// leaves it nothing to look up: it still builds an `addrinfo` chain, takes the
/// resolver's locks and has to be freed again. `inet_pton` is an order of
/// magnitude cheaper, and address literals are what `create_connection` is
/// handed most of the time.
///
/// Every case this cannot answer identically is refused, and the caller falls
/// back to libc: a scoped address, whose zone only libc can resolve; a legacy
/// form like `127.1` that `inet_pton` rejects but `getaddrinfo` accepts; an
/// unspecified socket type, which libc answers with one entry per type; and any
/// flag beyond the ones that cannot change the answer for a literal. The result
/// is built through `buildResults`, so it is rendered by the same code as the
/// libc path rather than by a second implementation of the same formatting.
fn resolveLiteral(hints: *const netdb.addrinfo, host: ?[*:0]const u8, service: ?[*:0]const u8) ?*py.Object {
    // musl populates `ai_canonname` for numeric addresses even when callers do
    // not request `AI_CANONNAME`. Synthesizing the result here would lose that
    // platform-visible field and disagree with `socket.getaddrinfo`, so let the
    // numeric libc path below answer instead.
    if (builtin.abi.isMusl()) return null;

    const flags: u32 = @bitCast(hints.flags);
    const ignorable: u32 = @bitCast(netdb.AI{ .NUMERICHOST = true, .NUMERICSERV = true, .PASSIVE = true });
    if (flags & ~ignorable != 0) return null;

    const protocol: c_int = switch (hints.socktype) {
        std.c.SOCK.STREAM => std.c.IPPROTO.TCP,
        std.c.SOCK.DGRAM => std.c.IPPROTO.UDP,
        else => return null,
    };
    if (hints.protocol != 0 and hints.protocol != protocol) return null;
    // Winsock preserves an unspecified protocol as zero in the result, unlike
    // the POSIX resolvers, which fill the protocol implied by the socket type.
    const result_protocol = if (uv.is_windows) hints.protocol else protocol;

    const name = std.mem.sliceTo(host orelse return null, 0);
    if (name.len == 0) return null;
    // A zone index is libc's to interpret; uvloop's equivalent shortcut drops it
    // and answers with scope 0, which is the wrong interface.
    if (std.mem.indexOfScalar(u8, name, '%') != null) return null;

    var port: u16 = 0;
    if (service) |svc| {
        const text = std.mem.sliceTo(svc, 0);
        port = std.fmt.parseInt(u16, text, 10) catch return null;
    }

    // SAFETY: inet_pton initializes the address and the successful branch fills
    // the remaining sockaddr fields before storage is read.
    var storage: addr.Storage = undefined;
    const family = hints.family;
    if (family == std.c.AF.INET or family == std.c.AF.UNSPEC) {
        const sin: *posix.sockaddr.in = @ptrCast(@alignCast(&storage));
        if (inet_pton(std.c.AF.INET, name.ptr, &sin.addr) == 1) {
            sin.* = .{ .port = std.mem.nativeToBig(u16, port), .addr = sin.addr };
            return finishLiteral(std.c.AF.INET, hints.socktype, result_protocol, @ptrCast(sin));
        }
    }
    if (family == std.c.AF.INET6 or family == std.c.AF.UNSPEC) {
        const sin6: *posix.sockaddr.in6 = @ptrCast(@alignCast(&storage));
        if (inet_pton(std.c.AF.INET6, name.ptr, &sin6.addr) == 1) {
            // Darwin carries a KAME-style scope id in bytes 2-3 of a link-local
            // literal: libc turns `fe80:1::` into `("fe80::", ..., scope=1)`.
            // Bypassing libc would expose a different address and scope. Plain
            // link-local literals have zero there and remain safe to answer.
            if (builtin.os.tag.isDarwin() and
                sin6.addr[0] == 0xfe and (sin6.addr[1] & 0xc0) == 0x80 and
                (sin6.addr[2] != 0 or sin6.addr[3] != 0)) return null;
            sin6.* = .{ .port = std.mem.nativeToBig(u16, port), .flowinfo = 0, .addr = sin6.addr, .scope_id = 0 };
            return finishLiteral(std.c.AF.INET6, hints.socktype, result_protocol, @ptrCast(sin6));
        }
    }
    return null;
}

fn finishLiteral(family: c_int, socktype: c_int, protocol: c_int, sa: *posix.sockaddr) ?*py.Object {
    var node = std.mem.zeroes(netdb.addrinfo);
    node.family = family;
    node.socktype = socktype;
    node.protocol = protocol;
    node.addr = sa;
    return buildResults(&node) catch {
        c.PyErr_Clear();
        return null;
    };
}

fn resolveNumeric(hints: *const netdb.addrinfo, host: ?[*:0]const u8, service: ?[*:0]const u8) ?*py.Object {
    if (hints.flags.CANONNAME) return null;
    var numeric = hints.*;
    numeric.flags.NUMERICHOST = true;
    numeric.flags.NUMERICSERV = true;

    var res: ?*netdb.addrinfo = null;
    if (netdb.getaddrinfo(host, service, &numeric, &res) != @as(netdb.EAI, @enumFromInt(0))) return null;
    defer if (res) |list| netdb.freeaddrinfo(list);
    return buildResults(res) catch {
        c.PyErr_Clear();
        return null;
    };
}

/// `_getaddrinfo(host, port, family, type, proto, flags)`
pub fn getaddrinfo(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    try py.expectArgs(args, 6, "_getaddrinfo");
    const loop = loopmod.asLoop(self_obj);
    try loopmod.checkClosed(loop.state());

    // Answered without a resolver request: nothing is being requested, so
    // nothing is allocated or linked into the loop's outstanding list.
    var host_buf: [max_host]u8 = undefined;
    var service_buf: [32]u8 = undefined;
    var hints = std.mem.zeroes(netdb.addrinfo);
    const host = try copyZ(&host_buf, args[0].?, "host");
    const service = try copyZ(&service_buf, args[1].?, "port");
    hints.family = try py.asCInt(args[2].?);
    hints.socktype = try py.asCInt(args[3].?);
    hints.protocol = try py.asCInt(args[4].?);
    hints.flags = @bitCast(@as(u32, @bitCast(try py.asCInt(args[5].?))));
    try loopmod.checkClosed(loop.state());

    // libuv refuses this pair outright, before any resolver sees it, and reports
    // it as EINVAL. The resolver the standard library reaches would have answered
    // "neither node nor service", which is what callers are written to expect.
    if (host == null and service == null) return raiseResolverError(uv.EAI_NONAME);

    // libuv runs every hostname through IDNA, which rejects an empty one, so `""`
    // can never reach a resolver that way. The platforms disagree about what it
    // means - BSD reads it as the null host, glibc as a name it cannot find - and
    // matching `socket.getaddrinfo` means letting the platform answer rather than
    // picking one.
    //
    // Nothing is resolved here, but this does enter libc on the loop thread, and
    // the first call in a process pays for the resolver's own initialization:
    // measured on macOS at 1.5ms for a numeric service and 2.3ms for a name,
    // then under a microsecond for every call after it. `resolveNumeric` pays the
    // same initialization on the main path, so moving this one to a threadpool
    // would not buy a loop that never waits for the resolver to wake up.
    if (host) |name| if (name[0] == 0) {
        var res: ?*netdb.addrinfo = null;
        const rc = netdb.getaddrinfo(name, service, &hints, &res);
        if (rc != @as(netdb.EAI, @enumFromInt(0))) return raisePlatformError(rc);
        defer if (res) |first| netdb.freeaddrinfo(first);
        const list = try buildResults(res);
        defer py.decref(list);
        const future = c.PyObject_CallMethodNoArgs(@ptrCast(loop), str_create_future) orelse return py.Error.Python;
        settle(future, str_set_result, list);
        return future;
    };

    if (resolveLiteral(&hints, host, service) orelse resolveNumeric(&hints, host, service)) |list| {
        defer py.decref(list);
        const future = c.PyObject_CallMethodNoArgs(@ptrCast(loop), str_create_future) orelse return py.Error.Python;
        settle(future, str_set_result, list);
        return future;
    }

    const req = try allocRequest(loop, .getaddrinfo);
    errdefer freeRequest(req, .getaddrinfo);
    try loopmod.checkClosed(loop.state());
    req.hints = hints;
    @memcpy(req.host[0..host_buf.len], &host_buf);
    @memcpy(req.service[0..service_buf.len], &service_buf);
    const req_host: ?[*:0]const u8 = if (host == null) null else @ptrCast(&req.host);
    const req_service: ?[*:0]const u8 = if (service == null) null else @ptrCast(&req.service);

    const status = uv.uv_getaddrinfo(loop.state().uvloop, req.addrReq(), onAddrInfo, req_host, req_service, &req.hints);
    if (status < 0) return raiseResolverError(status);
    return py.newref(req.future.?).?;
}

/// `_getnameinfo(sockaddr, flags)`
pub fn getnameinfo(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    try py.expectArgs(args, 2, "_getnameinfo");
    const loop = loopmod.asLoop(self_obj);
    try loopmod.checkClosed(loop.state());

    var storage: addr.Storage = .{};
    _ = addr.fromPython(0, args[0].?, &storage) catch |e| {
        // Only the host is the resolver's to report; a bad tuple or port keeps
        // its own exception, and those are not `OSError`.
        if (c.PyErr_ExceptionMatches(py.exc_os_error) == 0) return e;
        c.PyErr_Clear();
        return raisePlatformError(eai("NONAME", .FAIL));
    };
    const flags = try py.asCInt(args[1].?);

    const req = try allocRequest(loop, .getnameinfo);
    errdefer freeRequest(req, .getnameinfo);
    const status = uv.uv_getnameinfo(loop.state().uvloop, req.nameReq(), onNameInfo, storage.constPtr(), flags);
    if (status < 0) return raiseResolverError(status);
    return py.newref(req.future.?).?;
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
