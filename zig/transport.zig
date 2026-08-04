//! Native stream transport over `uv_tcp_t` / `uv_pipe_t`.
//!
//! Sockets are created, bound and accepted in Python; libuv only owns the data
//! path. Writes go through `uv_try_write` first, so the common case of a socket
//! with room in its send buffer never allocates a request. Protocol callbacks
//! are cached as bound methods when the protocol is set, keeping the per-read
//! cost to one vectorcall.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const uv = @import("uv.zig");
const handlemod = @import("handle.zig");
const loopmod = @import("loop.zig");
const LoopObject = loopmod.LoopObject;

const alloc = std.heap.c_allocator;

const READING: u32 = 1 << 0;
const CLOSING: u32 = 1 << 1;
const CONN_LOST: u32 = 1 << 2;
const EOF_WRITTEN: u32 = 1 << 3;
const BUFFERED: u32 = 1 << 4;
const PROTOCOL_PAUSED: u32 = 1 << 5;
const OPEN: u32 = 1 << 6;

pub const KIND_TCP: c_int = 0;
pub const KIND_PIPE: c_int = 1;

const default_high_water: usize = 64 * 1024;
pub const read_buffer_size: usize = 256 * 1024;
const inline_bufs = 16;

var str_connection_made: ?*py.Object = null;
var str_connection_lost: ?*py.Object = null;
var str_data_received: ?*py.Object = null;
var str_eof_received: ?*py.Object = null;
var str_get_buffer: ?*py.Object = null;
var str_buffer_updated: ?*py.Object = null;
var str_pause_writing: ?*py.Object = null;
var str_resume_writing: ?*py.Object = null;
var str_detach: ?*py.Object = null;
var str_high: ?*py.Object = null;
var str_low: ?*py.Object = null;
var str_start_reading: ?*py.Object = null;
var str_set_result_unless_cancelled: ?*py.Object = null;
var set_result_unless_cancelled: ?*py.Object = null;

pub var transport_type: ?*c.PyTypeObject = null;

pub const Transport = extern struct {
    ob_base: c.PyObject,
    loop: ?*py.Object,
    protocol: ?*py.Object,
    server: ?*py.Object,
    extra: ?*py.Object,
    conn_lost_exc: ?*py.Object,
    cb_connection_lost: ?*py.Object,
    cb_data_received: ?*py.Object,
    cb_eof_received: ?*py.Object,
    cb_get_buffer: ?*py.Object,
    cb_buffer_updated: ?*py.Object,
    cb_pause_writing: ?*py.Object,
    cb_resume_writing: ?*py.Object,
    write_buffer_size: usize,
    high_water: usize,
    low_water: usize,
    flags: u32,
    kind: c_int,
    view: c.Py_buffer,

    inline fn stream(self: *Transport) *uv.Stream {
        return @ptrCast(@as([*]u8, @ptrCast(self)) + handle_offset);
    }

    inline fn loopState(self: *Transport) *loopmod.State {
        return loopmod.asLoop(self.loop.?).state();
    }
};

var handle_offset: usize = 0;

/// A queued write: the libuv request and its payload in one allocation.
const WriteReq = struct {
    transport: *Transport,
    size: usize,
    total: usize,

    inline fn req(self: *WriteReq) *uv.Write {
        return @ptrCast(@as([*]u8, @ptrCast(self)) + write_req_offset);
    }

    inline fn payload(self: *WriteReq) [*]u8 {
        return @as([*]u8, @ptrCast(self)) + write_req_offset + uv.uv_req_size(.write);
    }
};

var write_req_offset: usize = 0;

// ---------------------------------------------------------------------------
// protocol dispatch

fn cacheCallback(protocol: *py.Object, name: ?*py.Object, slot: *?*py.Object) void {
    py.clear(slot);
    slot.* = c.PyObject_GetAttr(protocol, name);
    if (slot.* == null) c.PyErr_Clear();
}

fn bindProtocol(self: *Transport, protocol: *py.Object) void {
    py.clear(&self.protocol);
    py.incref(protocol);
    self.protocol = protocol;
    cacheCallback(protocol, str_connection_lost, &self.cb_connection_lost);
    cacheCallback(protocol, str_eof_received, &self.cb_eof_received);
    cacheCallback(protocol, str_pause_writing, &self.cb_pause_writing);
    cacheCallback(protocol, str_resume_writing, &self.cb_resume_writing);
    if (self.flags & BUFFERED != 0) {
        cacheCallback(protocol, str_get_buffer, &self.cb_get_buffer);
        cacheCallback(protocol, str_buffer_updated, &self.cb_buffer_updated);
    } else {
        cacheCallback(protocol, str_data_received, &self.cb_data_received);
    }
}

fn callProtocol(self: *Transport, callback: ?*py.Object, arg: ?*py.Object) void {
    const cb = callback orelse return;
    const result = if (arg) |a| c.PyObject_CallOneArg(cb, a) else c.PyObject_CallNoArgs(cb);
    if (result) |r| py.decref(r) else reportError(self, "Error in protocol callback");
}

fn reportError(self: *Transport, comptime message: [:0]const u8) void {
    const exc = c.PyErr_GetRaisedException() orelse return;
    defer py.decref(exc);
    loopmod.callExceptionHandler(loopmod.asLoop(self.loop.?), message, exc, @ptrCast(self));
}

/// Queues `callback(arg)` on the loop rather than reentering protocol code
/// from inside a libuv callback.
fn scheduleCall(self: *Transport, callback: ?*py.Object, arg: ?*py.Object) void {
    const cb = callback orelse return;
    const st = self.loopState();
    if (st.closed) return;
    var argv: [1]?*py.Object = .{arg};
    const n: usize = if (arg == null) 0 else 1;
    const h = handlemod.create(handlemod.handle_type.?, self.loop.?, cb, argv[0..n], null) catch {
        c.PyErr_Clear();
        return;
    };
    st.ready.push(@ptrCast(h)) catch py.decref(h);
    loopmod.startIdle(st);
}

// ---------------------------------------------------------------------------
// reading

fn onAlloc(handle: ?*uv.Handle, suggested: usize, buf: *uv.Buf) callconv(.c) void {
    _ = suggested;
    const self: *Transport = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();

    if (self.flags & BUFFERED != 0) {
        const size = py.int(@as(c.Py_ssize_t, 65536)) orelse {
            c.PyErr_Clear();
            buf.* = .{ .base = @ptrFromInt(@alignOf(u8)), .len = 0 };
            return;
        };
        defer py.decref(size);
        const target = c.PyObject_CallOneArg(self.cb_get_buffer orelse {
            buf.* = .{ .base = @ptrFromInt(@alignOf(u8)), .len = 0 };
            return;
        }, size);
        if (target) |t| {
            defer py.decref(t);
            if (c.PyObject_GetBuffer(t, &self.view, c.PyBUF_WRITABLE) == 0) {
                buf.* = .{ .base = @ptrCast(self.view.buf), .len = @intCast(self.view.len) };
                return;
            }
        }
        reportError(self, "Error allocating a receive buffer");
        buf.* = .{ .base = @ptrFromInt(@alignOf(u8)), .len = 0 };
        return;
    }

    const shared = loopmod.readBuffer(st) orelse {
        buf.* = .{ .base = @ptrFromInt(@alignOf(u8)), .len = 0 };
        return;
    };
    buf.* = .{ .base = shared, .len = read_buffer_size };
}

fn onRead(stream: ?*uv.Stream, nread: isize, buf: *const uv.Buf) callconv(.c) void {
    const self: *Transport = @ptrCast(@alignCast(uv.getData(stream.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();

    const buffered = self.flags & BUFFERED != 0;
    if (buffered and self.view.obj != null) c.PyBuffer_Release(&self.view);

    if (nread > 0) {
        if (buffered) {
            const n = py.int(@as(c.Py_ssize_t, @intCast(nread))) orelse {
                c.PyErr_Clear();
                return;
            };
            defer py.decref(n);
            callProtocol(self, self.cb_buffer_updated, n);
        } else {
            const data = py.bytes(buf.base[0..@intCast(nread)]) orelse {
                c.PyErr_Clear();
                return;
            };
            defer py.decref(data);
            callProtocol(self, self.cb_data_received, data);
        }
        return;
    }
    if (nread == 0) return;

    if (nread == uv.EOF) {
        handleEof(self);
        return;
    }
    forceClose(self, py.errUv(@intCast(nread)) catch null);
}

fn handleEof(self: *Transport) void {
    _ = uv.uv_read_stop(self.stream());
    self.flags &= ~READING;
    const cb = self.cb_eof_received orelse {
        closeTransport(self);
        return;
    };
    const keep = c.PyObject_CallNoArgs(cb) orelse {
        reportError(self, "Error in eof_received");
        closeTransport(self);
        return;
    };
    defer py.decref(keep);
    if (c.PyObject_IsTrue(keep) != 1) closeTransport(self);
}

fn startReading(self: *Transport) py.Error!void {
    if (self.flags & (READING | CLOSING | CONN_LOST) != 0) return;
    try py.errUvIfNeg(uv.uv_read_start(self.stream(), onAlloc, onRead));
    self.flags |= READING;
}

// ---------------------------------------------------------------------------
// writing

fn onWritten(req: ?*uv.Write, status: c_int) callconv(.c) void {
    const wr: *WriteReq = @ptrCast(@alignCast(uv.getData(req.?)));
    const self = wr.transport;
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();

    // abort() zeroes the queue while requests are still in flight.
    self.write_buffer_size -= @min(wr.size, self.write_buffer_size);
    alloc.free(@as([*]u8, @ptrCast(wr))[0..wr.total]);
    py.decref(self);

    if (status < 0) {
        forceClose(self, py.errUv(status) catch null);
        return;
    }
    maybeResumeProtocol(self);
    if (self.write_buffer_size == 0 and self.flags & CLOSING != 0) shutdownAndClose(self);
}

fn queueWrite(self: *Transport, bufs: []const uv.Buf) py.Error!void {
    var size: usize = 0;
    for (bufs) |b| size += b.len;
    if (size == 0) return;

    const req_size = uv.uv_req_size(.write);
    const total = write_req_offset + req_size + size;
    const raw = alloc.alignedAlloc(u8, .@"8", total) catch return py.errNoMemory();
    const wr: *WriteReq = @ptrCast(raw.ptr);
    wr.* = .{ .transport = self, .size = size, .total = total };

    var cursor = wr.payload();
    for (bufs) |b| {
        @memcpy(cursor[0..b.len], b.base[0..b.len]);
        cursor += b.len;
    }
    uv.setData(wr.req(), wr);

    const one = [_]uv.Buf{.{ .base = wr.payload(), .len = size }};
    py.incref(self);
    const status = uv.uv_write(wr.req(), self.stream(), &one, 1, onWritten);
    if (status < 0) {
        py.decref(self);
        alloc.free(raw);
        return py.errUv(status);
    }
    self.write_buffer_size += size;
    maybePauseProtocol(self);
}

/// Writes what the socket accepts immediately and queues the rest.
fn writeBufs(self: *Transport, bufs: []uv.Buf) py.Error!void {
    if (self.flags & CONN_LOST != 0) return;
    if (self.flags & (CLOSING | EOF_WRITTEN) != 0) {
        return py.errRuntime("Cannot call write() after write_eof() or close()");
    }

    var pending = bufs;
    if (self.write_buffer_size == 0) {
        const written = uv.uv_try_write(self.stream(), pending.ptr, @intCast(pending.len));
        if (written > 0) {
            var remaining: usize = @intCast(written);
            while (pending.len != 0 and remaining >= pending[0].len) {
                remaining -= pending[0].len;
                pending = pending[1..];
            }
            if (pending.len == 0) return;
            pending[0].base += remaining;
            pending[0].len -= remaining;
        } else if (written < 0 and written != uv.EAGAIN) {
            forceClose(self, py.errUv(written) catch null);
            return;
        }
    }
    try queueWrite(self, pending);
}

fn maybePauseProtocol(self: *Transport) void {
    if (self.write_buffer_size <= self.high_water) return;
    if (self.flags & PROTOCOL_PAUSED != 0) return;
    self.flags |= PROTOCOL_PAUSED;
    callProtocol(self, self.cb_pause_writing, null);
}

fn maybeResumeProtocol(self: *Transport) void {
    if (self.flags & PROTOCOL_PAUSED == 0) return;
    if (self.write_buffer_size > self.low_water) return;
    self.flags &= ~PROTOCOL_PAUSED;
    callProtocol(self, self.cb_resume_writing, null);
}

// ---------------------------------------------------------------------------
// teardown

fn onClosed(handle: ?*uv.Handle) callconv(.c) void {
    const self: *Transport = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();

    self.flags |= CONN_LOST;
    scheduleCall(self, self.cb_connection_lost, self.conn_lost_exc orelse py.none());
    if (self.server) |server| {
        const res = c.PyObject_CallMethodNoArgs(server, str_detach);
        if (res) |r| py.decref(r) else py.writeUnraisable(@ptrCast(self));
    }
    py.decref(self);
}

fn shutdownAndClose(self: *Transport) void {
    if (self.flags & OPEN == 0) return;
    self.flags &= ~OPEN;
    if (self.flags & READING != 0) {
        _ = uv.uv_read_stop(self.stream());
        self.flags &= ~READING;
    }
    uv.uv_close(uv.asHandle(self.stream()), onClosed);
}

fn closeTransport(self: *Transport) void {
    if (self.flags & CLOSING != 0) return;
    self.flags |= CLOSING;
    if (self.flags & READING != 0) {
        _ = uv.uv_read_stop(self.stream());
        self.flags &= ~READING;
    }
    if (self.write_buffer_size == 0) shutdownAndClose(self);
}

fn forceClose(self: *Transport, exc: ?*py.Object) void {
    if (exc) |e| {
        py.clear(&self.conn_lost_exc);
        self.conn_lost_exc = e;
    } else {
        c.PyErr_Clear();
    }
    self.flags |= CLOSING;
    self.write_buffer_size = 0;
    shutdownAndClose(self);
}

// ---------------------------------------------------------------------------
// python methods

fn asTransport(obj: *py.Object) *Transport {
    return @ptrCast(@alignCast(obj));
}

fn getExtraInfo(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    if (args.len < 1 or args.len > 2) return py.errType("get_extra_info() takes 1 or 2 arguments");
    const self = asTransport(self_obj);
    const default: *py.Object = if (args.len == 2) args[1].? else py.none();
    const extra = self.extra orelse return py.newref(default).?;
    const found = c.PyDict_GetItemWithError(extra, args[0].?);
    if (found) |v| return py.newref(v).?;
    if (c.PyErr_Occurred() != null) return py.Error.Python;
    return py.newref(default).?;
}

fn isClosing(self_obj: *py.Object) py.Error!*py.Object {
    return py.boolRef(asTransport(self_obj).flags & (CLOSING | CONN_LOST) != 0);
}

fn close(self_obj: *py.Object) py.Error!*py.Object {
    closeTransport(asTransport(self_obj));
    return py.noneRef();
}

fn abort(self_obj: *py.Object) py.Error!*py.Object {
    forceClose(asTransport(self_obj), null);
    return py.noneRef();
}

fn getProtocol(self_obj: *py.Object) py.Error!*py.Object {
    return py.newref(asTransport(self_obj).protocol orelse py.none()).?;
}

fn setProtocol(self_obj: *py.Object, protocol: *py.Object) py.Error!*py.Object {
    bindProtocol(asTransport(self_obj), protocol);
    return py.noneRef();
}

fn isReading(self_obj: *py.Object) py.Error!*py.Object {
    return py.boolRef(asTransport(self_obj).flags & READING != 0);
}

fn pauseReading(self_obj: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    if (self.flags & READING != 0) {
        try py.errUvIfNeg(uv.uv_read_stop(self.stream()));
        self.flags &= ~READING;
    }
    return py.noneRef();
}

fn resumeReading(self_obj: *py.Object) py.Error!*py.Object {
    try startReading(asTransport(self_obj));
    return py.noneRef();
}

pub fn startReadingMethod(self_obj: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    startReading(self) catch {
        const exc = c.PyErr_GetRaisedException();
        forceClose(self, exc);
        return py.noneRef();
    };
    return py.noneRef();
}

fn write(self_obj: *py.Object, data: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    var view: c.Py_buffer = undefined;
    if (c.PyObject_GetBuffer(data, &view, c.PyBUF_SIMPLE) < 0) return py.Error.Python;
    defer c.PyBuffer_Release(&view);
    if (view.len == 0) return py.noneRef();
    var bufs = [_]uv.Buf{.{ .base = @ptrCast(view.buf), .len = @intCast(view.len) }};
    try writeBufs(self, &bufs);
    return py.noneRef();
}

fn writelines(self_obj: *py.Object, data: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    const seq = c.PySequence_Fast(data, "writelines() requires an iterable of buffers") orelse return py.Error.Python;
    defer py.decref(seq);
    const n: usize = @intCast(c.PySequence_Size(seq));
    if (n == 0) return py.noneRef();

    var inline_storage: [inline_bufs]uv.Buf = undefined;
    var inline_views: [inline_bufs]c.Py_buffer = undefined;
    const bufs = if (n <= inline_bufs) inline_storage[0..n] else alloc.alloc(uv.Buf, n) catch return py.errNoMemory();
    const views = if (n <= inline_bufs) inline_views[0..n] else alloc.alloc(c.Py_buffer, n) catch return py.errNoMemory();
    defer if (n > inline_bufs) {
        alloc.free(bufs);
        alloc.free(views);
    };

    var filled: usize = 0;
    defer while (filled > 0) {
        filled -= 1;
        c.PyBuffer_Release(&views[filled]);
    };
    while (filled < n) : (filled += 1) {
        const item = c.PySequence_GetItem(seq, @intCast(filled)) orelse return py.Error.Python;
        // The buffer view keeps the exporter alive, so the item reference can go.
        const acquired = c.PyObject_GetBuffer(item, &views[filled], c.PyBUF_SIMPLE);
        py.decref(item);
        if (acquired < 0) return py.Error.Python;
        bufs[filled] = .{ .base = @ptrCast(views[filled].buf), .len = @intCast(views[filled].len) };
    }
    try writeBufs(self, bufs);
    return py.noneRef();
}

fn writeEof(self_obj: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    if (self.flags & (CONN_LOST | EOF_WRITTEN | CLOSING) != 0) return py.noneRef();
    self.flags |= EOF_WRITTEN;
    // libuv's shutdown request would need to outlive this call; closing the
    // write side directly matches what asyncio's transports do.
    var fd: uv.OsFd = -1;
    if (uv.uv_fileno(uv.asHandle(self.stream()), &fd) == 0) {
        _ = std.c.shutdown(fd, std.c.SHUT.WR);
    }
    return py.noneRef();
}

fn canWriteEof(self_obj: *py.Object) py.Error!*py.Object {
    _ = self_obj;
    return py.boolRef(true);
}

fn getWriteBufferSize(self_obj: *py.Object) py.Error!*py.Object {
    return py.int(asTransport(self_obj).write_buffer_size) orelse py.Error.Python;
}

fn getWriteBufferLimits(self_obj: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    return c.Py_BuildValue("nn", @as(c.Py_ssize_t, @intCast(self.low_water)), @as(c.Py_ssize_t, @intCast(self.high_water))) orelse py.Error.Python;
}

/// `set_write_buffer_limits(high=None, low=None)`, matching asyncio: an
/// omitted low mark defaults to a quarter of the high mark.
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

    const self = asTransport(self_obj);
    var high_water: usize = default_high_water;
    if (high) |value| {
        if (!py.isNone(value)) high_water = @intCast(try py.asIsize(value));
    }
    var low_water: usize = high_water / 4;
    if (low) |value| {
        if (!py.isNone(value)) low_water = @intCast(try py.asIsize(value));
    }
    if (low_water > high_water) return py.errValue("high water mark must be >= low water mark");
    self.high_water = high_water;
    self.low_water = low_water;
    maybePauseProtocol(self);
    return py.noneRef();
}

fn forceCloseMethod(self_obj: *py.Object, exc: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    forceClose(self, if (py.isNone(exc)) null else py.newref(exc));
    return py.noneRef();
}

// ---------------------------------------------------------------------------
// construction

/// `loop._make_transport(fd, kind, protocol, waiter, extra, server, buffered)`
pub fn makeTransport(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    try py.expectArgs(args, 7, "_make_transport");
    const loop = loopmod.asLoop(self_obj);
    const st = loop.state();
    try loopmod.checkClosed(st);

    const fd = try py.asCInt(args[0].?);
    const kind = try py.asCInt(args[1].?);
    const buffered = try py.isTrue(args[6].?);

    const obj = c.PyType_GenericAlloc(transport_type, 0) orelse return py.Error.Python;
    const self = asTransport(obj);
    errdefer py.decref(obj);

    self.high_water = default_high_water;
    self.low_water = default_high_water / 4;
    self.kind = kind;
    if (buffered) self.flags |= BUFFERED;

    const init_status = if (kind == KIND_TCP)
        uv.uv_tcp_init_ex(st.uvloop, @ptrCast(self.stream()), 0)
    else
        uv.uv_pipe_init(st.uvloop, @ptrCast(self.stream()), 0);
    try py.errUvIfNeg(init_status);
    self.flags |= OPEN;
    uv.setData(self.stream(), self);

    const open_status = if (kind == KIND_TCP)
        uv.uv_tcp_open(@ptrCast(self.stream()), fd)
    else
        uv.uv_pipe_open(@ptrCast(self.stream()), fd);
    if (open_status < 0) {
        uv.uv_close(uv.asHandle(self.stream()), null);
        self.flags &= ~OPEN;
        return py.errUv(open_status);
    }
    if (kind == KIND_TCP) _ = uv.uv_tcp_nodelay(@ptrCast(self.stream()), 1);

    py.incref(self_obj);
    self.loop = self_obj;
    if (!py.isNone(args[4].?)) {
        py.incref(args[4].?);
        self.extra = args[4];
    }
    if (!py.isNone(args[5].?)) {
        py.incref(args[5].?);
        self.server = args[5];
    }
    bindProtocol(self, args[2].?);
    // Kept alive until the close callback fires.
    py.incref(obj);

    const connection_made = c.PyObject_GetAttr(args[2].?, str_connection_made) orelse return py.Error.Python;
    defer py.decref(connection_made);
    scheduleCall(self, connection_made, obj);

    const start = c.PyObject_GetAttr(obj, str_start_reading) orelse return py.Error.Python;
    defer py.decref(start);
    scheduleCall(self, start, null);

    if (!py.isNone(args[3].?)) {
        const h = try handlemod.create(
            handlemod.handle_type.?,
            self_obj,
            set_result_unless_cancelled.?,
            &.{ args[3], py.none() },
            null,
        );
        st.ready.push(@ptrCast(h)) catch py.decref(h);
        loopmod.startIdle(st);
    }
    return obj;
}

fn dealloc(obj: ?*py.Object) callconv(.c) void {
    const self = asTransport(obj.?);
    const tp = py.typeOf(obj.?);
    c.PyObject_GC_UnTrack(obj);
    if (self.view.obj != null) c.PyBuffer_Release(&self.view);
    py.clear(&self.loop);
    py.clear(&self.protocol);
    py.clear(&self.server);
    py.clear(&self.extra);
    py.clear(&self.conn_lost_exc);
    py.clear(&self.cb_connection_lost);
    py.clear(&self.cb_data_received);
    py.clear(&self.cb_eof_received);
    py.clear(&self.cb_get_buffer);
    py.clear(&self.cb_buffer_updated);
    py.clear(&self.cb_pause_writing);
    py.clear(&self.cb_resume_writing);
    tp.tp_free.?(obj);
    py.decref(tp);
}

fn traverse(obj: ?*py.Object, visitproc: c.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
    const self = asTransport(obj.?);
    const refs = [_]?*py.Object{
        self.loop,
        self.protocol,
        self.server,
        self.extra,
        self.conn_lost_exc,
        self.cb_connection_lost,
        self.cb_data_received,
        self.cb_eof_received,
        self.cb_get_buffer,
        self.cb_buffer_updated,
        self.cb_pause_writing,
        self.cb_resume_writing,
    };
    for (refs) |slot| {
        const r = py.visit(slot, visitproc, arg);
        if (r != 0) return r;
    }
    return py.visit(@ptrCast(py.typeOf(obj.?)), visitproc, arg);
}

fn clear_(obj: ?*py.Object) callconv(.c) c_int {
    const self = asTransport(obj.?);
    py.clear(&self.protocol);
    py.clear(&self.server);
    py.clear(&self.extra);
    py.clear(&self.conn_lost_exc);
    py.clear(&self.cb_connection_lost);
    py.clear(&self.cb_data_received);
    py.clear(&self.cb_eof_received);
    py.clear(&self.cb_get_buffer);
    py.clear(&self.cb_buffer_updated);
    py.clear(&self.cb_pause_writing);
    py.clear(&self.cb_resume_writing);
    return 0;
}

var methods = [_]c.PyMethodDef{
    py.method("get_extra_info", getExtraInfo, "Return transport metadata."),
    py.methodNoArgs("is_closing", isClosing, "Return True once the transport is closing."),
    py.methodNoArgs("close", close, "Close the transport once pending writes drain."),
    py.methodNoArgs("abort", abort, "Close the transport immediately."),
    py.methodNoArgs("get_protocol", getProtocol, "Return the current protocol."),
    py.methodO("set_protocol", setProtocol, "Replace the current protocol."),
    py.methodNoArgs("is_reading", isReading, "Return True while reads are delivered."),
    py.methodNoArgs("pause_reading", pauseReading, "Stop delivering reads."),
    py.methodNoArgs("resume_reading", resumeReading, "Resume delivering reads."),
    py.methodNoArgs("_start_reading", startReadingMethod, "Begin reading from the stream."),
    py.methodO("write", write, "Write a buffer to the transport."),
    py.methodO("writelines", writelines, "Write an iterable of buffers to the transport."),
    py.methodNoArgs("write_eof", writeEof, "Shut down the writing end."),
    py.methodNoArgs("can_write_eof", canWriteEof, "Return True; stream transports support write_eof()."),
    py.methodNoArgs("get_write_buffer_size", getWriteBufferSize, "Return the number of bytes queued."),
    py.methodNoArgs("get_write_buffer_limits", getWriteBufferLimits, "Return the (low, high) flow-control marks."),
    py.methodKw("set_write_buffer_limits", setWriteBufferLimits, "Set the flow-control marks."),
    py.methodO("_force_close", forceCloseMethod, "Close immediately, reporting an exception."),
    py.sentinel,
};

var slots = [_]c.PyType_Slot{
    .{ .slot = c.Py_tp_dealloc, .pfunc = @constCast(@ptrCast(&dealloc)) },
    .{ .slot = c.Py_tp_traverse, .pfunc = @constCast(@ptrCast(&traverse)) },
    .{ .slot = c.Py_tp_clear, .pfunc = @constCast(@ptrCast(&clear_)) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_doc, .pfunc = @constCast(@ptrCast("A libuv-backed stream transport.")) },
    .{ .slot = 0, .pfunc = null },
};

var spec = c.PyType_Spec{
    .name = "zuv._zuv.Transport",
    .basicsize = 0,
    .itemsize = 0,
    .flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_IMMUTABLETYPE | c.Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .slots = &slots,
};

pub fn register(module: *py.Object) py.Error!void {
    str_connection_made = py.intern("connection_made") orelse return py.Error.Python;
    str_connection_lost = py.intern("connection_lost") orelse return py.Error.Python;
    str_data_received = py.intern("data_received") orelse return py.Error.Python;
    str_eof_received = py.intern("eof_received") orelse return py.Error.Python;
    str_get_buffer = py.intern("get_buffer") orelse return py.Error.Python;
    str_buffer_updated = py.intern("buffer_updated") orelse return py.Error.Python;
    str_pause_writing = py.intern("pause_writing") orelse return py.Error.Python;
    str_resume_writing = py.intern("resume_writing") orelse return py.Error.Python;
    str_detach = py.intern("_detach") orelse return py.Error.Python;
    str_high = py.intern("high") orelse return py.Error.Python;
    str_low = py.intern("low") orelse return py.Error.Python;
    str_start_reading = py.intern("_start_reading") orelse return py.Error.Python;
    str_set_result_unless_cancelled = py.intern("_set_result_unless_cancelled") orelse return py.Error.Python;
    set_result_unless_cancelled = py.importFrom("asyncio.futures", "_set_result_unless_cancelled") orelse return py.Error.Python;

    handle_offset = std.mem.alignForward(usize, @sizeOf(Transport), 16);
    write_req_offset = std.mem.alignForward(usize, @sizeOf(WriteReq), 8);
    const handle_size = @max(uv.uv_handle_size(.tcp), uv.uv_handle_size(.named_pipe));
    spec.basicsize = @intCast(handle_offset + handle_size);

    transport_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &spec, null) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "Transport", @ptrCast(transport_type)) < 0) return py.Error.Python;
    if (c.PyModule_AddIntConstant(module, "KIND_TCP", KIND_TCP) < 0) return py.Error.Python;
    if (c.PyModule_AddIntConstant(module, "KIND_PIPE", KIND_PIPE) < 0) return py.Error.Python;
}
