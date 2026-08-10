//! Native datagram transport over `uv_udp_t`.
//!
//! The socket is created, bound and connected in Python; libuv owns only the
//! data path. Sends go through `uv_udp_try_send` first, so a datagram the
//! kernel accepts outright never allocates a request.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const uv = @import("uv.zig");
const addr = @import("addr.zig");
const handlemod = @import("handle.zig");
const loopmod = @import("loop.zig");

const alloc = std.heap.c_allocator;

const READING: u32 = 1 << 0;
const CLOSING: u32 = 1 << 1;
const CONN_LOST: u32 = 1 << 2;
const PROTOCOL_PAUSED: u32 = 1 << 3;
const OPEN: u32 = 1 << 4;
const CONNECTED: u32 = 1 << 5;

const default_high_water: usize = 64 * 1024;
const recv_size: usize = 64 * 1024;

var str_connection_lost: ?*py.Object = null;
var str_datagram_received: ?*py.Object = null;
var str_error_received: ?*py.Object = null;
var str_pause_writing: ?*py.Object = null;
var str_resume_writing: ?*py.Object = null;
var str_sock_detach: ?*py.Object = null;
var str_addr: ?*py.Object = null;
var str_high: ?*py.Object = null;
var str_low: ?*py.Object = null;

pub var datagram_type: ?*c.PyTypeObject = null;

pub const Datagram = extern struct {
    ob_base: c.PyObject,
    /// `asyncio.BaseTransport` declares `__slots__ = ('_extra',)`; that word is
    /// its layout, and leaving it NULL keeps `_extra` looking unset.
    base_extra: ?*py.Object,
    loop: ?*py.Object,
    state: ?*loopmod.State,
    protocol: ?*py.Object,
    extra: ?*py.Object,
    conn_lost_exc: ?*py.Object,
    socket_view: ?*py.Object,
    context: ?*py.Object,
    cb_connection_lost: ?*py.Object,
    cb_datagram_received: ?*py.Object,
    cb_error_received: ?*py.Object,
    cb_pause_writing: ?*py.Object,
    cb_resume_writing: ?*py.Object,
    write_buffer_size: usize,
    high_water: usize,
    low_water: usize,
    flags: u32,
    family: c_int,

    inline fn udp(self: *Datagram) *uv.Udp {
        return @ptrCast(@as([*]u8, @ptrCast(self)) + handle_offset);
    }

    inline fn loopState(self: *Datagram) *loopmod.State {
        return self.state.?;
    }
};

var handle_offset: usize = 0;

/// A queued datagram: the libuv request, the payload copied inline, and the
/// destination. Unlike a stream write, a datagram is a single small message and
/// its destination has to outlive the call, so this copies rather than holding
/// a view of the caller's buffer.
const SendReq = struct {
    transport: *Datagram,
    size: usize,
    total: usize,
    dest: addr.Storage,
    has_dest: bool,

    inline fn req(self: *SendReq) *uv.UdpSend {
        return @ptrCast(@as([*]u8, @ptrCast(self)) + send_req_offset);
    }

    inline fn payload(self: *SendReq) [*]u8 {
        return @as([*]u8, @ptrCast(self)) + send_req_offset + uv.uv_req_size(.udp_send);
    }
};

var send_req_offset: usize = 0;

// ---------------------------------------------------------------------------
// protocol dispatch

fn cacheCallback(protocol: *py.Object, name: ?*py.Object, slot: *?*py.Object) void {
    py.clear(slot);
    slot.* = c.PyObject_GetAttr(protocol, name);
    if (slot.* == null) c.PyErr_Clear();
}

fn bindProtocol(self: *Datagram, protocol: *py.Object) void {
    py.clear(&self.protocol);
    py.incref(protocol);
    self.protocol = protocol;
    cacheCallback(protocol, str_connection_lost, &self.cb_connection_lost);
    cacheCallback(protocol, str_datagram_received, &self.cb_datagram_received);
    cacheCallback(protocol, str_error_received, &self.cb_error_received);
    cacheCallback(protocol, str_pause_writing, &self.cb_pause_writing);
    cacheCallback(protocol, str_resume_writing, &self.cb_resume_writing);
}

fn reportError(self: *Datagram, comptime message: [:0]const u8) void {
    const exc = c.PyErr_GetRaisedException() orelse return;
    defer py.decref(exc);
    loopmod.callExceptionHandler(loopmod.asLoop(self.loop.?), message, exc, @ptrCast(self));
}

/// Turns a libuv status into the exception asyncio would have raised, so it can
/// be handed to `error_received` instead.
fn takeUvError(status: c_int) ?*py.Object {
    switch (py.errUv(status)) {
        error.Python => {},
    }
    return c.PyErr_GetRaisedException();
}

fn callProtocol(self: *Datagram, callback: ?*py.Object, arg: ?*py.Object) void {
    const cb = callback orelse return;
    const result = if (arg) |a| c.PyObject_CallOneArg(cb, a) else c.PyObject_CallNoArgs(cb);
    if (result) |r| py.decref(r) else reportError(self, "Error in protocol callback");
}

/// Reads reach the protocol inside the context captured when the endpoint was
/// created, matching how asyncio delivers them from a handle.
fn callInContext(self: *Datagram, callback: ?*py.Object, args: []const ?*py.Object) void {
    const cb = callback orelse return;
    const ctx = self.context orelse return;
    if (c.PyContext_Enter(ctx) != 0) {
        reportError(self, "Error entering transport context");
        return;
    }
    const result = c.PyObject_Vectorcall(cb, args.ptr, args.len, null);
    if (result) |r| py.decref(r) else reportError(self, "Error in protocol callback");
    if (c.PyContext_Exit(ctx) != 0) reportError(self, "Error leaving transport context");
}

fn scheduleCall(self: *Datagram, callback: ?*py.Object, arg: ?*py.Object) void {
    const cb = callback orelse return;
    const st = self.loopState();
    if (st.closed) return;
    var argv: [1]?*py.Object = .{arg};
    const n: usize = if (arg == null) 0 else 1;
    const h = handlemod.create(handlemod.handle_type.?, self.loop.?, cb, argv[0..n], self.context) catch {
        c.PyErr_Clear();
        return;
    };
    st.ready.push(@ptrCast(h)) catch py.decref(h);
    loopmod.startIdle(st);
}

// ---------------------------------------------------------------------------
// receive path

fn onAlloc(handle: ?*uv.Handle, suggested: usize, buf: *uv.Buf) callconv(.c) void {
    _ = suggested;
    const self: *Datagram = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    const scratch = loopmod.scratchBuffer(st) orelse {
        buf.* = .{ .base = @ptrFromInt(@alignOf(u8)), .len = 0 };
        return;
    };
    buf.* = .{ .base = scratch, .len = recv_size };
}

/// libuv signals "nothing more to read for now" with `nread == 0` and a null
/// address, which is not an empty datagram - an empty datagram arrives with an
/// address attached and has to reach the protocol.
fn onRecv(
    handle: ?*uv.Udp,
    nread: isize,
    buf: *const uv.Buf,
    from: ?*const std.posix.sockaddr,
    flags: c_uint,
) callconv(.c) void {
    const self: *Datagram = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();

    if (nread == 0 and from == null) return;
    if (flags & uv.UDP_PARTIAL != 0) {
        // The datagram did not fit and the tail is gone; reporting the prefix
        // would be worse than reporting the loss.
        const exc = c.PyObject_CallFunction(@ptrCast(c.PyExc_OSError), "s", "datagram truncated") orelse {
            c.PyErr_Clear();
            return;
        };
        defer py.decref(exc);
        callInContext(self, self.cb_error_received, &.{exc});
        return;
    }
    if (nread < 0) {
        const exc = takeUvError(@intCast(nread)) orelse return;
        defer py.decref(exc);
        callInContext(self, self.cb_error_received, &.{exc});
        return;
    }

    const data = c.PyBytes_FromStringAndSize(buf.base, @intCast(nread)) orelse {
        c.PyErr_Clear();
        return;
    };
    defer py.decref(data);
    const sender = addr.toPython(from.?) catch {
        // The datagram cannot be delivered without naming its sender, and a
        // receive failure is what `error_received` exists to report.
        const exc = c.PyErr_GetRaisedException() orelse return;
        defer py.decref(exc);
        callInContext(self, self.cb_error_received, &.{exc});
        return;
    };
    defer py.decref(sender);
    callInContext(self, self.cb_datagram_received, &.{ data, sender });
}

fn startReceiving(self: *Datagram) py.Error!void {
    if (self.flags & (READING | CLOSING | CONN_LOST) != 0) return;
    try py.errUvIfNeg(uv.uv_udp_recv_start(self.udp(), onAlloc, onRecv));
    self.flags |= READING;
}

// ---------------------------------------------------------------------------
// send path

fn onSent(req: ?*uv.UdpSend, status: c_int) callconv(.c) void {
    const wr: *SendReq = @ptrCast(@alignCast(uv.getData(req.?)));
    const self = wr.transport;
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();

    self.write_buffer_size -= @min(wr.size, self.write_buffer_size);
    const total = wr.total;
    alloc.free(@as([*]align(16) u8, @ptrCast(@alignCast(wr)))[0..total]);
    py.decref(self);

    // A failed datagram is reported to the protocol, never raised: asyncio
    // treats the endpoint as still usable.
    if (status < 0 and self.flags & CONN_LOST == 0) {
        if (takeUvError(status)) |exc| {
            defer py.decref(exc);
            callProtocol(self, self.cb_error_received, exc);
        }
    }
    maybeResumeProtocol(self);
    if (self.write_buffer_size == 0 and self.flags & CLOSING != 0) shutdownAndClose(self);
}

fn queueSend(self: *Datagram, buf: uv.Buf, dest: ?*const std.posix.sockaddr) py.Error!void {
    const total = send_req_offset + uv.uv_req_size(.udp_send) + buf.len;
    const raw = alloc.alignedAlloc(u8, .@"16", total) catch return py.errNoMemory();
    const wr: *SendReq = @ptrCast(raw.ptr);
    wr.* = .{ .transport = self, .size = buf.len, .total = total, .dest = .{}, .has_dest = dest != null };
    if (dest) |d| @memcpy(std.mem.asBytes(&wr.dest)[0..@sizeOf(addr.Storage)], @as([*]const u8, @ptrCast(d))[0..@sizeOf(addr.Storage)]);
    @memcpy(wr.payload()[0..buf.len], buf.base[0..buf.len]);
    uv.setData(wr.req(), wr);

    const send_buf = uv.Buf{ .base = wr.payload(), .len = buf.len };
    py.incref(self);
    const status = uv.uv_udp_send(
        wr.req(),
        self.udp(),
        @ptrCast(&send_buf),
        1,
        if (wr.has_dest) wr.dest.ptr() else null,
        onSent,
    );
    if (status < 0) {
        py.decref(self);
        alloc.free(raw);
        return py.errUv(status);
    }
    self.write_buffer_size += buf.len;
    maybePauseProtocol(self);
}

fn maybePauseProtocol(self: *Datagram) void {
    if (self.write_buffer_size <= self.high_water) return;
    if (self.flags & PROTOCOL_PAUSED != 0) return;
    self.flags |= PROTOCOL_PAUSED;
    callProtocol(self, self.cb_pause_writing, null);
}

fn maybeResumeProtocol(self: *Datagram) void {
    if (self.flags & PROTOCOL_PAUSED == 0) return;
    if (self.write_buffer_size > self.low_water) return;
    self.flags &= ~PROTOCOL_PAUSED;
    callProtocol(self, self.cb_resume_writing, null);
}

// ---------------------------------------------------------------------------
// teardown

fn releaseSocketView(self: *Datagram) void {
    const view = self.socket_view orelse return;
    self.socket_view = null;
    const result = c.PyObject_CallMethodNoArgs(view, str_sock_detach);
    if (result) |r| py.decref(r) else c.PyErr_Clear();
    py.decref(view);
}

fn onClosed(handle: ?*uv.Handle) callconv(.c) void {
    const self: *Datagram = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();

    releaseSocketView(self);
    self.flags |= CONN_LOST;
    scheduleCall(self, self.cb_connection_lost, self.conn_lost_exc orelse py.none());
    py.decref(self);
}

fn shutdownAndClose(self: *Datagram) void {
    if (self.flags & OPEN == 0) return;
    self.flags &= ~OPEN;
    if (self.flags & READING != 0) {
        _ = uv.uv_udp_recv_stop(self.udp());
        self.flags &= ~READING;
    }
    uv.uv_close(uv.asHandle(self.udp()), onClosed);
}

/// Closes a datagram endpoint discovered while the owning loop shuts down.
pub fn closeFromLoop(handle: *uv.Handle) void {
    const self: *Datagram = @ptrCast(@alignCast(uv.getData(handle)));
    self.flags |= CLOSING;
    self.flags &= ~OPEN;
    if (self.flags & READING != 0) {
        _ = uv.uv_udp_recv_stop(self.udp());
        self.flags &= ~READING;
    }
    uv.uv_close(handle, onClosed);
}

// ---------------------------------------------------------------------------
// python methods

fn asDatagram(obj: *py.Object) *Datagram {
    return @ptrCast(@alignCast(obj));
}

fn sendto(self_obj: *py.Object, args: []const ?*py.Object, nargs: usize, kwnames: ?*py.Object) py.Error!*py.Object {
    if (nargs < 1 or nargs > 2) return py.errType("sendto() takes 1 or 2 arguments");
    var address: ?*py.Object = if (nargs > 1) args[1] else null;
    if (kwnames) |names| {
        const n: usize = @intCast(c.PyTuple_Size(names));
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const key = c.PyTuple_GetItem(names, @intCast(i)) orelse return py.Error.Python;
            if (c.PyObject_RichCompareBool(key, str_addr, c.Py_EQ) != 1) {
                return py.errType("sendto() got an unexpected keyword argument");
            }
            if (address != null) return py.errType("sendto() got multiple values for argument 'addr'");
            address = args[nargs + i];
        }
    }
    const self = asDatagram(self_obj);

    const target = if (address) |a| (if (py.isNone(a)) null else address) else null;
    var dest: addr.Storage = .{};
    var dest_ptr: ?*const std.posix.sockaddr = null;
    if (target) |t| {
        const dest_len = try addr.fromPython(self.family, t, &dest);
        if (self.flags & CONNECTED != 0) {
            // Naming the peer is allowed, as it is for asyncio; naming anything
            // else is not.
            var peer: addr.Storage = .{};
            var len: c_int = @sizeOf(addr.Storage);
            if (uv.uv_udp_getpeername(self.udp(), peer.ptr(), &len) < 0 or
                !addr.same(dest.constPtr(), dest_len, peer.constPtr(), len))
            {
                return py.errValue("Transport is connected; sendto() takes no address but the peer's");
            }
        } else {
            dest_ptr = dest.ptr();
        }
    } else if (self.flags & CONNECTED == 0) {
        return py.errValue("sendto() requires an address on an unconnected transport");
    }

    var view: c.Py_buffer = undefined;
    if (c.PyObject_GetBuffer(args[0].?, &view, c.PyBUF_SIMPLE) < 0) return py.Error.Python;
    defer c.PyBuffer_Release(&view);

    // A closing endpoint drops the datagram, as asyncio does - but the argument
    // errors above still raise there, which is why they all come first.
    if (self.flags & (CONN_LOST | CLOSING) != 0) return py.noneRef();

    const buf = uv.Buf{ .base = @ptrCast(view.buf), .len = @intCast(view.len) };

    if (self.write_buffer_size == 0) {
        const sent = uv.uv_udp_try_send(self.udp(), @ptrCast(&buf), 1, dest_ptr);
        if (sent >= 0) return py.noneRef();
        if (sent != uv.EAGAIN) {
            // asyncio reports a failed datagram to the protocol rather than
            // raising, and the endpoint stays usable.
            if (takeUvError(sent)) |exc| {
                defer py.decref(exc);
                callProtocol(self, self.cb_error_received, exc);
            }
            return py.noneRef();
        }
    }
    try queueSend(self, buf, dest_ptr);
    return py.noneRef();
}

fn abort(self_obj: *py.Object) py.Error!*py.Object {
    const self = asDatagram(self_obj);
    self.flags |= CLOSING;
    self.write_buffer_size = 0;
    shutdownAndClose(self);
    return py.noneRef();
}

fn close(self_obj: *py.Object) py.Error!*py.Object {
    const self = asDatagram(self_obj);
    if (self.flags & CLOSING != 0) return py.noneRef();
    self.flags |= CLOSING;
    if (self.flags & READING != 0) {
        _ = uv.uv_udp_recv_stop(self.udp());
        self.flags &= ~READING;
    }
    if (self.write_buffer_size == 0) shutdownAndClose(self);
    return py.noneRef();
}

fn isClosing(self_obj: *py.Object) py.Error!*py.Object {
    return py.boolRef(asDatagram(self_obj).flags & CLOSING != 0);
}

fn getExtraInfo(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    if (args.len < 1 or args.len > 2) return py.errType("get_extra_info() takes 1 or 2 arguments");
    const self = asDatagram(self_obj);
    const fallback: *py.Object = if (args.len == 2) args[1].? else py.none();
    const extra = self.extra orelse return py.newref(fallback).?;
    if (py.isNone(extra)) return py.newref(fallback).?;
    const found = c.PyDict_GetItemWithError(extra, args[0].?) orelse {
        if (c.PyErr_Occurred() != null) return py.Error.Python;
        return py.newref(fallback).?;
    };
    return py.newref(found).?;
}

fn getProtocol(self_obj: *py.Object) py.Error!*py.Object {
    const self = asDatagram(self_obj);
    return py.newref(self.protocol orelse py.none()).?;
}

fn setProtocol(self_obj: *py.Object, protocol: *py.Object) py.Error!*py.Object {
    bindProtocol(asDatagram(self_obj), protocol);
    return py.noneRef();
}

fn getWriteBufferSize(self_obj: *py.Object) py.Error!*py.Object {
    return py.int(asDatagram(self_obj).write_buffer_size) orelse py.Error.Python;
}

fn getWriteBufferLimits(self_obj: *py.Object) py.Error!*py.Object {
    const self = asDatagram(self_obj);
    return c.Py_BuildValue("nn", @as(c.Py_ssize_t, @intCast(self.low_water)), @as(c.Py_ssize_t, @intCast(self.high_water))) orelse py.Error.Python;
}

fn setWriteBufferLimits(
    self_obj: *py.Object,
    args: []const ?*py.Object,
    nargs: usize,
    kwnames: ?*py.Object,
) py.Error!*py.Object {
    if (nargs > 2) return py.errType("set_write_buffer_limits() takes at most 2 arguments");
    var high: ?*py.Object = if (nargs > 0) args[0] else null;
    var low: ?*py.Object = if (nargs > 1) args[1] else null;
    if (kwnames) |names| {
        const n: usize = @intCast(c.PyTuple_Size(names));
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const key = c.PyTuple_GetItem(names, @intCast(i)) orelse return py.Error.Python;
            if (c.PyObject_RichCompareBool(key, str_high, c.Py_EQ) == 1) {
                high = args[nargs + i];
            } else if (c.PyObject_RichCompareBool(key, str_low, c.Py_EQ) == 1) {
                low = args[nargs + i];
            } else {
                return py.errType("set_write_buffer_limits() got an unexpected keyword argument");
            }
        }
    }
    const self = asDatagram(self_obj);
    var high_value: usize = default_high_water;
    if (high) |h| {
        if (!py.isNone(h)) high_value = @intCast(try py.asIsize(h));
    }
    var low_value: usize = high_value / 4;
    if (low) |l| {
        if (!py.isNone(l)) low_value = @intCast(try py.asIsize(l));
    }
    if (high == null or py.isNone(high.?)) {
        if (low) |l| {
            if (!py.isNone(l)) high_value = low_value * 4;
        }
    }
    if (low_value > high_value) return py.errValue("high water mark must be >= low water mark");
    self.high_water = high_value;
    self.low_water = low_value;
    maybePauseProtocol(self);
    return py.noneRef();
}

fn adoptSocketView(self_obj: *py.Object, view: *py.Object) py.Error!*py.Object {
    const self = asDatagram(self_obj);
    py.clear(&self.socket_view);
    py.incref(view);
    self.socket_view = view;
    return py.noneRef();
}

/// `pause_reading()`: stop delivering datagrams until reading resumes.
///
/// A datagram endpoint that is not reading leaves what arrives in the socket's
/// receive buffer, and the kernel drops from there once it fills. That is what
/// asyncio's does too; uvloop's has no such method at all.
fn pauseReading(self_obj: *py.Object) py.Error!*py.Object {
    const self = asDatagram(self_obj);
    if (self.flags & CLOSING != 0) return py.errRuntime("Cannot pause_reading() after close()");
    if (self.flags & READING != 0) {
        _ = uv.uv_udp_recv_stop(self.udp());
        self.flags &= ~READING;
    }
    return py.noneRef();
}

/// `resume_reading()`: deliver datagrams again.
fn resumeReading(self_obj: *py.Object) py.Error!*py.Object {
    const self = asDatagram(self_obj);
    if (self.flags & CLOSING != 0) return py.errRuntime("Cannot resume_reading() after close()");
    try startReceiving(self);
    return py.noneRef();
}

fn startReceivingMethod(self_obj: *py.Object) py.Error!*py.Object {
    const self = asDatagram(self_obj);
    startReceiving(self) catch {
        const exc = c.PyErr_GetRaisedException();
        py.clear(&self.conn_lost_exc);
        self.conn_lost_exc = exc;
        self.flags |= CLOSING;
        shutdownAndClose(self);
        return py.noneRef();
    };
    return py.noneRef();
}

// ---------------------------------------------------------------------------
// construction

/// `loop._make_datagram_transport(fd, family, connected, protocol, extra)`
pub fn makeDatagram(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    try py.expectArgs(args, 5, "_make_datagram_transport");
    const loop = loopmod.asLoop(self_obj);
    const st = loop.state();
    try loopmod.checkClosed(st);

    const fd = try py.asCInt(args[0].?);
    const family = try py.asCInt(args[1].?);
    const connected = c.PyObject_IsTrue(args[2].?) == 1;

    const obj = c.PyType_GenericAlloc(datagram_type, 0) orelse return py.Error.Python;
    const self = asDatagram(obj);
    errdefer py.decref(obj);

    self.state = st;
    self.high_water = default_high_water;
    self.low_water = default_high_water / 4;
    self.family = family;
    if (connected) self.flags |= CONNECTED;
    self.context = c.PyContext_CopyCurrent();
    if (self.context == null) return py.Error.Python;

    bindProtocol(self, args[3].?);
    st.ready.ensureUnusedCapacity(1) catch return py.errNoMemory();

    try py.errUvIfNeg(uv.uv_udp_init_ex(st.uvloop, self.udp(), 0));
    self.flags |= OPEN;
    uv.setData(self.udp(), self);

    if (uv.uv_udp_open(self.udp(), fd) < 0) {
        // libuv never took the descriptor, so the caller still owns it.
        self.flags &= ~OPEN;
        py.incref(obj);
        uv.uv_close(uv.asHandle(self.udp()), onOpenFailed);
        return py.errUv(uv.uv_udp_open(self.udp(), fd));
    }

    py.incref(obj);
    py.incref(self_obj);
    self.loop = self_obj;
    py.incref(args[4].?);
    self.extra = args[4].?;
    return obj;
}

fn onOpenFailed(handle: ?*uv.Handle) callconv(.c) void {
    const self: *Datagram = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();
    py.decref(self);
}

// ---------------------------------------------------------------------------
// type

fn dealloc(obj: ?*py.Object) callconv(.c) void {
    const self = asDatagram(obj.?);
    const tp = py.typeOf(obj.?);
    c.PyObject_GC_UnTrack(obj);
    c.PyObject_ClearWeakRefs(obj);
    c.PyObject_ClearManagedDict(obj);
    py.clear(&self.base_extra);
    py.clear(&self.loop);
    py.clear(&self.protocol);
    py.clear(&self.extra);
    py.clear(&self.conn_lost_exc);
    py.clear(&self.socket_view);
    py.clear(&self.context);
    py.clear(&self.cb_connection_lost);
    py.clear(&self.cb_datagram_received);
    py.clear(&self.cb_error_received);
    py.clear(&self.cb_pause_writing);
    py.clear(&self.cb_resume_writing);
    tp.tp_free.?(obj);
    py.decref(tp);
}

fn traverse(obj: ?*py.Object, visitproc: c.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
    const self = asDatagram(obj.?);
    const refs = [_]?*py.Object{
        self.base_extra,
        self.loop,
        self.protocol,
        self.extra,
        self.conn_lost_exc,
        self.socket_view,
        self.context,
        self.cb_connection_lost,
        self.cb_datagram_received,
        self.cb_error_received,
        self.cb_pause_writing,
        self.cb_resume_writing,
    };
    for (refs) |slot| {
        const r = py.visit(slot, visitproc, arg);
        if (r != 0) return r;
    }
    const managed = c.PyObject_VisitManagedDict(obj, visitproc, arg);
    if (managed != 0) return managed;
    return py.visit(@ptrCast(py.typeOf(obj.?)), visitproc, arg);
}

fn clear_(obj: ?*py.Object) callconv(.c) c_int {
    const self = asDatagram(obj.?);
    c.PyObject_ClearManagedDict(obj);
    py.clear(&self.base_extra);
    py.clear(&self.protocol);
    py.clear(&self.extra);
    py.clear(&self.conn_lost_exc);
    py.clear(&self.cb_connection_lost);
    py.clear(&self.cb_datagram_received);
    py.clear(&self.cb_error_received);
    py.clear(&self.cb_pause_writing);
    py.clear(&self.cb_resume_writing);
    return 0;
}

var methods = [_]c.PyMethodDef{
    py.method("get_extra_info", getExtraInfo, "Return transport metadata."),
    py.methodNoArgs("is_closing", isClosing, "Return True once the transport is closing."),
    py.methodNoArgs("close", close, "Close the endpoint once pending sends drain."),
    py.methodNoArgs("abort", abort, "Close the endpoint immediately."),
    py.methodNoArgs("get_protocol", getProtocol, "Return the current protocol."),
    py.methodO("set_protocol", setProtocol, "Replace the current protocol."),
    py.methodO("_adopt_socket_view", adoptSocketView, "Track the socket object mirroring libuv's descriptor."),
    py.methodNoArgs("pause_reading", pauseReading, "Stop delivering datagrams."),
    py.methodNoArgs("resume_reading", resumeReading, "Deliver datagrams again."),
    py.methodNoArgs("_start_receiving", startReceivingMethod, "Begin delivering datagrams."),
    py.methodKw("sendto", sendto, "Send a datagram to an address."),
    py.methodNoArgs("get_write_buffer_size", getWriteBufferSize, "Return the number of bytes queued."),
    py.methodNoArgs("get_write_buffer_limits", getWriteBufferLimits, "Return the (low, high) flow-control marks."),
    py.methodKw("set_write_buffer_limits", setWriteBufferLimits, "Set the flow-control marks."),
    py.sentinel,
};

var slots = [_]c.PyType_Slot{
    .{ .slot = c.Py_tp_dealloc, .pfunc = @constCast(@ptrCast(&dealloc)) },
    .{ .slot = c.Py_tp_traverse, .pfunc = @constCast(@ptrCast(&traverse)) },
    .{ .slot = c.Py_tp_clear, .pfunc = @constCast(@ptrCast(&clear_)) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_doc, .pfunc = @constCast(@ptrCast("A libuv-backed datagram transport.")) },
    .{ .slot = 0, .pfunc = null },
};

var spec = c.PyType_Spec{
    .name = "zuvloop._zuvloop.DatagramTransport",
    .basicsize = 0,
    .itemsize = 0,
    .flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_MANAGED_WEAKREF |
        c.Py_TPFLAGS_MANAGED_DICT | c.Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .slots = &slots,
};

pub fn register(module: *py.Object) py.Error!void {
    str_connection_lost = py.intern("connection_lost") orelse return py.Error.Python;
    str_datagram_received = py.intern("datagram_received") orelse return py.Error.Python;
    str_error_received = py.intern("error_received") orelse return py.Error.Python;
    str_pause_writing = py.intern("pause_writing") orelse return py.Error.Python;
    str_resume_writing = py.intern("resume_writing") orelse return py.Error.Python;
    str_sock_detach = py.intern("detach") orelse return py.Error.Python;
    str_addr = py.intern("addr") orelse return py.Error.Python;
    str_high = py.intern("high") orelse return py.Error.Python;
    str_low = py.intern("low") orelse return py.Error.Python;

    handle_offset = std.mem.alignForward(usize, @sizeOf(Datagram), 16);
    send_req_offset = std.mem.alignForward(usize, @sizeOf(SendReq), 16);
    spec.basicsize = @intCast(handle_offset + uv.uv_handle_size(.udp));

    // asyncio's own datagram transport derives from both legs, so `isinstance(t,
    // asyncio.Transport)` holds for it as well; test_events asserts exactly that.
    const stream_base = py.importFrom("asyncio.transports", "Transport") orelse return py.Error.Python;
    defer py.decref(stream_base);
    const dgram_base = py.importFrom("asyncio.transports", "DatagramTransport") orelse return py.Error.Python;
    defer py.decref(dgram_base);
    const layout: c.Py_ssize_t = @offsetOf(Datagram, "loop");
    if (@as(*c.PyTypeObject, @ptrCast(stream_base)).tp_basicsize != layout or
        @as(*c.PyTypeObject, @ptrCast(dgram_base)).tp_basicsize != layout)
    {
        return py.errRuntime("asyncio transport instance layout is not the one zuvloop was built against");
    }
    const bases = c.PyTuple_Pack(2, stream_base, dgram_base) orelse return py.Error.Python;
    defer py.decref(bases);

    datagram_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &spec, bases) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "DatagramTransport", @ptrCast(datagram_type)) < 0) return py.Error.Python;
}
