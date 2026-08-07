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
pub const FLUSH_QUEUED: u32 = 1 << 7;
const HANDLE_REF: u32 = 1 << 8;
/// The tracked object is a pipe this transport owns, so closing it is the
/// transport's job. A socket view is only detached - libuv owns its descriptor.
const PIPE_OWNED: u32 = 1 << 9;
/// The application asked for this transport to close. Loop teardown also sets
/// CLOSING, and `repr` has to tell the two apart: asyncio still calls a transport
/// "open" after `loop.close()`, because that never delivers `connection_lost`.
const CLOSE_REQUESTED: u32 = 1 << 10;

pub const KIND_TCP: c_int = 0;
pub const KIND_PIPE: c_int = 1;
/// A pipe whose descriptor is write-only, so no read is ever started on it.
pub const KIND_PIPE_WRITE: c_int = 2;

const default_high_water: usize = 64 * 1024;
/// Reads land in a `bytes` object sized to what the peer has been sending.
///
/// A fixed size cannot serve both shapes of traffic: a large buffer wastes an
/// allocation on every small request, while a small one caps bulk transfer by
/// forcing a syscall per chunk. The size follows the traffic instead, doubling
/// whenever a read fills the buffer and easing back when it does not.
const read_size_min: usize = 16 * 1024;
const read_size_max: usize = 256 * 1024;

/// Below this, a read is copied out of a shared buffer into an exactly sized
/// `bytes`; above it, libuv fills the final object directly. Small requests -
/// an HTTP header block is a couple of hundred bytes - are far cheaper to copy
/// than to allocate a large object for and shrink.
pub const copy_threshold: usize = 64 * 1024;
const inline_bufs = 16;

/// Writes issued within one loop iteration are held here and sent together.
///
/// A protocol sends a response in pieces - a header block and a body, which is
/// what ASGI and aiohttp both do - and writing each piece as it arrives costs a
/// syscall per piece. Holding them until the iteration ends turns the whole
/// response into one vectored write. Four slots covers the shapes that occur in
/// practice; a protocol that writes more simply flushes when the batch fills,
/// which is no worse than writing each one immediately.
const pending_max = 4;

var str_connection_made: ?*py.Object = null;
var str_connection_lost: ?*py.Object = null;
var str_data_received: ?*py.Object = null;
var str_eof_received: ?*py.Object = null;
var str_get_buffer: ?*py.Object = null;
var str_buffer_updated: ?*py.Object = null;
var str_pause_writing: ?*py.Object = null;
var str_resume_writing: ?*py.Object = null;
var str_detach: ?*py.Object = null;
var str_sock_detach: ?*py.Object = null;
var str_pipe_close: ?*py.Object = null;
var str_high: ?*py.Object = null;
var str_low: ?*py.Object = null;
var str_start_reading: ?*py.Object = null;
var str_set_result_unless_cancelled: ?*py.Object = null;
var set_result_unless_cancelled: ?*py.Object = null;
var buffered_protocol_type: ?*py.Object = null;

pub var transport_type: ?*c.PyTypeObject = null;

pub const Transport = extern struct {
    ob_base: c.PyObject,
    /// `asyncio.BaseTransport` declares `__slots__ = ('_extra',)`, so its instance
    /// layout owns this word. Leaving it NULL keeps `_extra` looking unset, which
    /// is what it is - `get_extra_info` reads our own `extra` instead.
    base_extra: ?*py.Object,
    loop: ?*py.Object,
    state: ?*loopmod.State,
    protocol: ?*py.Object,
    server: ?*py.Object,
    extra: ?*py.Object,
    conn_lost_exc: ?*py.Object,
    read_bytes: ?*py.Object,
    socket_view: ?*py.Object,
    context: ?*py.Object,
    read_size: usize,
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

    /// Writes accepted this iteration but not yet handed to libuv. `flush_next`
    /// threads the transport onto the loop's flush list; see `pending_max`.
    pending_bufs: [pending_max]uv.Buf,
    pending_views: [pending_max]c.Py_buffer,
    pending_count: usize,
    pending_size: usize,
    flush_next: ?*Transport,
    owner_prev: ?*Transport,
    owner_next: ?*Transport,

    inline fn stream(self: *Transport) *uv.Stream {
        return @ptrCast(@as([*]u8, @ptrCast(self)) + handle_offset);
    }

    inline fn loopState(self: *Transport) *loopmod.State {
        return self.state.?;
    }
};

var handle_offset: usize = 0;

/// A queued write: the libuv request, the buffer views keeping the caller's
/// memory alive, and the vector libuv reads from - all in one allocation.
///
/// Retaining views rather than copying is what makes a large `write()` free:
/// the exporter stays alive (and, for a bytearray, locked against resizing)
/// until libuv reports the write complete.
///
/// The request does not take another Python reference to `transport`. The
/// loop-owned handle reference remains alive until `onClosed`, and libuv runs
/// every outstanding request callback before that close callback.
const WriteReq = struct {
    transport: *Transport,
    size: usize,
    total: usize,
    nviews: usize,

    inline fn req(self: *WriteReq) *uv.Write {
        return @ptrCast(@as([*]u8, @ptrCast(self)) + write_req_offset);
    }

    inline fn views(self: *WriteReq) [*]c.Py_buffer {
        return @ptrCast(@alignCast(@as([*]u8, @ptrCast(self)) + write_req_offset + uv.uv_req_size(.write)));
    }

    inline fn bufs(self: *WriteReq) [*]uv.Buf {
        const after = @as([*]u8, @ptrCast(self)) + write_req_offset + uv.uv_req_size(.write);
        return @ptrCast(@alignCast(after + self.nviews * @sizeOf(c.Py_buffer)));
    }

    fn release(self: *WriteReq) void {
        const owned = self.views();
        var i: usize = 0;
        while (i < self.nviews) : (i += 1) c.PyBuffer_Release(&owned[i]);
    }
};

var write_req_offset: usize = 0;

fn releaseViews(views: []c.Py_buffer) void {
    for (views) |*view| c.PyBuffer_Release(view);
}

// ---------------------------------------------------------------------------
// protocol dispatch

fn cacheCallback(protocol: *py.Object, name: ?*py.Object, slot: *?*py.Object) void {
    py.clear(slot);
    slot.* = c.PyObject_GetAttr(protocol, name);
    if (slot.* == null) c.PyErr_Clear();
}

/// Re-reads which reading protocol the object implements. `set_protocol` can
/// swap a plain protocol for a buffered one - `start_tls` does exactly that -
/// so bufferedness is decided here rather than by the caller.
fn bindProtocol(self: *Transport, protocol: *py.Object) void {
    py.clear(&self.protocol);
    py.incref(protocol);
    self.protocol = protocol;
    cacheCallback(protocol, str_connection_lost, &self.cb_connection_lost);
    cacheCallback(protocol, str_eof_received, &self.cb_eof_received);
    cacheCallback(protocol, str_pause_writing, &self.cb_pause_writing);
    cacheCallback(protocol, str_resume_writing, &self.cb_resume_writing);

    py.clear(&self.cb_get_buffer);
    py.clear(&self.cb_buffer_updated);
    py.clear(&self.cb_data_received);

    const buffered = c.PyObject_IsInstance(protocol, buffered_protocol_type);
    if (buffered < 0) c.PyErr_Clear();
    if (buffered == 1) {
        self.flags |= BUFFERED;
        cacheCallback(protocol, str_get_buffer, &self.cb_get_buffer);
        cacheCallback(protocol, str_buffer_updated, &self.cb_buffer_updated);
    } else {
        self.flags &= ~BUFFERED;
        cacheCallback(protocol, str_data_received, &self.cb_data_received);
    }
}

/// Used for callbacks asyncio invokes synchronously from `write()`, which
/// simply inherit whichever context is already active.
fn callProtocol(self: *Transport, callback: ?*py.Object, arg: ?*py.Object) void {
    const cb = callback orelse return;
    const result = if (arg) |a| c.PyObject_CallOneArg(cb, a) else c.PyObject_CallNoArgs(cb);
    if (result) |r| py.decref(r) else reportError(self, "Error in protocol callback");
}

/// Used for the read path. asyncio delivers reads from a handle whose context
/// was copied when the transport was created, so contextvars set before the
/// connection existed stay visible inside `data_received`. libuv calls straight
/// into us with no context entered, so it is entered here instead.
fn callInContext(self: *Transport, callback: ?*py.Object, arg: ?*py.Object, comptime failed: [:0]const u8) void {
    const cb = callback orelse return;
    const context = self.context;
    if (context) |ctx| {
        if (c.PyContext_Enter(ctx) < 0) {
            reportError(self, "Error entering the transport context");
            return;
        }
    }
    const result = if (arg) |a| c.PyObject_CallOneArg(cb, a) else c.PyObject_CallNoArgs(cb);
    // Taken before leaving the context, which can raise one of its own.
    const failure = if (result) |r| blk: {
        py.decref(r);
        break :blk null;
    } else c.PyErr_GetRaisedException();
    if (context) |ctx| {
        if (c.PyContext_Exit(ctx) < 0) py.writeUnraisable(@ptrCast(self));
    }
    // Fatal, as it is for asyncio: carrying on hands the protocol another chunk
    // after it has said it cannot cope.
    if (failure) |exc| {
        // Asking to leave is the loop's business, not the connection's.
        const loop = loopmod.asLoop(self.loop.?);
        if (isExit(exc)) {
            c.PyErr_SetRaisedException(exc);
            loopmod.captureFatal(loop);
            forceClose(self, null);
            return;
        }
        loopmod.callExceptionHandler(loop, failed, exc, @ptrCast(self));
        forceClose(self, exc);
    }
}

fn isExit(exc: *py.Object) bool {
    const kind: *py.Object = @ptrCast(py.typeOf(exc));
    return c.PyType_IsSubtype(@ptrCast(kind), @ptrCast(py.exc_system_exit)) != 0 or
        c.PyType_IsSubtype(@ptrCast(kind), @ptrCast(py.exc_keyboard_interrupt)) != 0;
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
    const h = handlemod.create(handlemod.handle_type.?, self.loop.?, cb, argv[0..n], self.context) catch {
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

    // Handing back the loop's shared buffer touches no Python object, and libuv
    // asks for one on every readable event, so the GIL round trip the rest of
    // this needs would be paid per read for nothing.
    if (self.flags & BUFFERED == 0 and self.read_bytes == null and self.read_size <= copy_threshold) {
        if (loopmod.scratchBuffer(st)) |scratch| {
            buf.* = .{ .base = scratch, .len = self.read_size };
            return;
        }
    }

    st.gilEnter();
    defer st.gilExit();

    if (self.flags & BUFFERED != 0) {
        const size = py.int(@as(c.Py_ssize_t, 65536)) orelse {
            c.PyErr_Clear();
            buf.* = .{ .base = @ptrFromInt(@alignOf(u8)), .len = 0 };
            return;
        };
        defer py.decref(size);
        const getter = self.cb_get_buffer orelse {
            buf.* = .{ .base = @ptrFromInt(@alignOf(u8)), .len = 0 };
            return;
        };
        if (self.context) |ctx| {
            if (c.PyContext_Enter(ctx) < 0) c.PyErr_Clear();
        }
        const target = c.PyObject_CallOneArg(getter, size);
        if (self.context) |ctx| {
            if (c.PyContext_Exit(ctx) < 0) c.PyErr_Clear();
        }
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

    py.clear(&self.read_bytes);
    if (self.read_size <= copy_threshold) {
        // Small traffic: read into the shared buffer and allocate exactly what
        // arrived. A null read_bytes marks this path for onRead.
        if (loopmod.scratchBuffer(st)) |scratch| {
            buf.* = .{ .base = scratch, .len = self.read_size };
            return;
        }
    }
    // Bulk traffic: libuv fills the final object, so nothing is copied.
    const target = c.PyBytes_FromStringAndSize(null, @intCast(self.read_size)) orelse {
        c.PyErr_Clear();
        buf.* = .{ .base = @ptrFromInt(@alignOf(u8)), .len = 0 };
        return;
    };
    self.read_bytes = target;
    buf.* = .{ .base = @ptrCast(c.PyBytes_AsString(target)), .len = self.read_size };
}

fn onRead(stream: ?*uv.Stream, nread: isize, buf: *const uv.Buf) callconv(.c) void {
    const self: *Transport = @ptrCast(@alignCast(uv.getData(stream.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();

    const buffered = self.flags & BUFFERED != 0;
    if (buffered and self.view.obj != null) c.PyBuffer_Release(&self.view);

    // A write pipe reads only to learn that the peer went away; asyncio arms the
    // descriptor for the same reason and never delivers what arrives on it.
    if (self.kind == KIND_PIPE_WRITE) {
        py.clear(&self.read_bytes);
        if (nread == 0) return;
        if (bufferedBytes(self) != 0) {
            forceClose(self, takeUvError(uv.EPIPE));
        } else {
            closeTransport(self);
        }
        return;
    }

    if (nread > 0) {
        if (buffered) {
            const n = py.int(@as(c.Py_ssize_t, @intCast(nread))) orelse {
                c.PyErr_Clear();
                return;
            };
            defer py.decref(n);
            callInContext(self, self.cb_buffer_updated, n, "Fatal error: protocol.buffer_updated() call failed.");
            return;
        }
        adjustReadSize(self, @intCast(nread));
        const data = blk: {
            if (self.read_bytes) |owned| {
                self.read_bytes = null;
                var resized = owned;
                if (c._PyBytes_Resize(@ptrCast(&resized), @intCast(nread)) < 0) {
                    c.PyErr_Clear();
                    return;
                }
                break :blk resized;
            }
            break :blk py.bytes(buf.base[0..@intCast(nread)]) orelse {
                c.PyErr_Clear();
                return;
            };
        };
        defer py.decref(data);
        callInContext(self, self.cb_data_received, data, "Fatal error: protocol.data_received() call failed.");
        return;
    }
    py.clear(&self.read_bytes);
    if (nread == 0) return;

    if (nread == uv.EOF) {
        handleEof(self);
        return;
    }
    forceClose(self, takeUvError(@intCast(nread)));
}

fn repr(obj: ?*py.Object) callconv(.c) ?*py.Object {
    const self: *Transport = @ptrCast(@alignCast(obj.?));
    const name = py.typeOf(obj.?).tp_name;
    if (self.flags & CLOSE_REQUESTED == 0) return c.PyUnicode_FromFormat("<%s open>", name);
    if (self.flags & CONN_LOST != 0) return c.PyUnicode_FromFormat("<%s closed>", name);
    return c.PyUnicode_FromFormat("<%s closing>", name);
}

fn isSocket(fd: c_int) bool {
    // getsockopt succeeds only on a socket, and unlike fstat it reaches libc on
    // every platform Zig targets (std.c.fstat is unavailable on Linux).
    var kind: c_int = 0;
    if (uv.is_windows) {
        const win32 = @import("win32.zig");
        var len: c_int = @sizeOf(c_int);
        return win32.getsockopt(@intCast(fd), std.c.SOL.SOCKET, std.c.SO.TYPE, @ptrCast(&kind), &len) == 0;
    } else {
        var len: std.c.socklen_t = @sizeOf(c_int);
        return std.c.getsockopt(fd, std.c.SOL.SOCKET, std.c.SO.TYPE, &kind, &len) == 0;
    }
}

fn takeUvError(status: c_int) ?*py.Object {
    switch (py.errUv(status)) {
        error.Python => {},
    }
    return c.PyErr_GetRaisedException();
}

/// A full buffer means the peer had more to give, so read bigger next time.
fn adjustReadSize(self: *Transport, nread: usize) void {
    if (nread >= self.read_size) {
        self.read_size = @min(self.read_size * 2, read_size_max);
    } else if (nread * 4 <= self.read_size) {
        self.read_size = @max(self.read_size / 2, read_size_min);
    }
}

fn handleEof(self: *Transport) void {
    _ = uv.uv_read_stop(self.stream());
    self.flags &= ~READING;
    const cb = self.cb_eof_received orelse {
        closeTransport(self);
        return;
    };
    if (self.context) |ctx| {
        if (c.PyContext_Enter(ctx) < 0) c.PyErr_Clear();
    }
    const keep = c.PyObject_CallNoArgs(cb);
    if (self.context) |ctx| {
        if (c.PyContext_Exit(ctx) < 0) c.PyErr_Clear();
    }
    const kept = keep orelse {
        reportError(self, "Error in eof_received");
        closeTransport(self);
        return;
    };
    defer py.decref(kept);
    if (c.PyObject_IsTrue(kept) != 1) closeTransport(self);
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
    wr.release();
    alloc.free(@as([*]u8, @ptrCast(wr))[0..wr.total]);

    if (status < 0) {
        // Closing the handle completes everything still queued with ECANCELED,
        // and so does a sibling request failing. The reason for the close is
        // already recorded by then, and this is not it. `OPEN` is cleared
        // immediately before `uv_close` on both teardown paths, so it separates
        // a real write failure from libuv tidying up after one.
        if (self.flags & OPEN != 0) forceClose(self, takeUvError(status));
        return;
    }
    maybeResumeProtocol(self);
    if (self.write_buffer_size == 0) {
        if (self.flags & CLOSING != 0) {
            shutdownAndClose(self);
        } else if (self.flags & EOF_WRITTEN != 0) {
            shutdownWrite(self);
        }
    }
}

/// Takes ownership of `views`, releasing them once libuv reports completion.
fn queueWrite(self: *Transport, bufs: []const uv.Buf, views: []c.Py_buffer) py.Error!void {
    var size: usize = 0;
    for (bufs) |b| size += b.len;
    if (size == 0) {
        releaseViews(views);
        return;
    }

    const n = views.len;
    const total = write_req_offset + uv.uv_req_size(.write) + n * @sizeOf(c.Py_buffer) + bufs.len * @sizeOf(uv.Buf);
    const raw = alloc.alignedAlloc(u8, .@"16", total) catch {
        releaseViews(views);
        return py.errNoMemory();
    };
    const wr: *WriteReq = @ptrCast(raw.ptr);
    wr.* = .{ .transport = self, .size = size, .total = total, .nviews = n };
    @memcpy(wr.views()[0..n], views);
    @memcpy(wr.bufs()[0..bufs.len], bufs);
    uv.setData(wr.req(), wr);

    const status = uv.uv_write(wr.req(), self.stream(), wr.bufs(), @intCast(bufs.len), onWritten);
    if (status < 0) {
        wr.release();
        alloc.free(raw);
        return py.errUv(status);
    }
    self.write_buffer_size += size;
    maybePauseProtocol(self);
}

/// Writes what the socket accepts immediately and queues the rest.
/// Takes ownership of `views` on every path.
fn writeBufs(self: *Transport, bufs: []uv.Buf, views: []c.Py_buffer) py.Error!void {
    if (self.flags & CONN_LOST != 0) {
        releaseViews(views);
        return;
    }
    if (self.flags & (CLOSING | EOF_WRITTEN) != 0) {
        releaseViews(views);
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
            if (pending.len == 0) {
                releaseViews(views);
                return;
            }
            pending[0].base += remaining;
            pending[0].len -= remaining;
        } else if (written < 0 and written != uv.EAGAIN) {
            releaseViews(views);
            forceClose(self, takeUvError(written));
            return;
        }
    }
    try queueWrite(self, pending, views);
}

/// Hands everything accepted this iteration to libuv as one vectored write.
///
/// Called from the loop's check handle, and directly by anything that must not
/// leave data sitting unwritten - closing, half-closing, or reporting the queue
/// depth to the protocol.
pub fn flushPending(self: *Transport) void {
    const count = self.pending_count;
    if (count == 0) return;
    self.pending_count = 0;
    self.pending_size = 0;
    writeBufs(self, self.pending_bufs[0..count], self.pending_views[0..count]) catch {
        // Nothing above us can act on a write failure - asyncio reports it
        // through the protocol - and the views are already released.
        const exc = c.PyErr_GetRaisedException();
        forceClose(self, exc);
        return;
    };
    // The socket may have taken the whole batch, in which case there is no write
    // callback coming to lift a pause this transport is still under.
    maybeResumeProtocol(self);
}

/// Drops writes that will never reach the socket, releasing what they pinned.
pub fn discardPending(self: *Transport) void {
    const count = self.pending_count;
    self.pending_count = 0;
    self.pending_size = 0;
    releaseViews(self.pending_views[0..count]);
}

fn appendPending(self: *Transport, bufs: []const uv.Buf, views: []c.Py_buffer) void {
    for (bufs, views, 0..) |b, v, i| {
        // Flushing may call pause_writing(), which is allowed to reenter write()
        // and refill the batch. Re-establish the capacity invariant after every
        // trip through Python before indexing the fixed-size arrays.
        while (self.pending_count == pending_max) {
            flushPending(self);
            if (self.flags & (CONN_LOST | CLOSING | EOF_WRITTEN) != 0) {
                releaseViews(views[i..]);
                return;
            }
        }
        self.pending_bufs[self.pending_count] = b;
        self.pending_views[self.pending_count] = v;
        self.pending_count += 1;
        self.pending_size += b.len;
    }
    loopmod.scheduleFlush(self.loopState(), self);
}

/// Accepts a write, deferring the syscall to the end of the iteration.
/// Takes ownership of `views` on every path.
fn submitWrite(self: *Transport, bufs: []uv.Buf, views: []c.Py_buffer) py.Error!void {
    if (self.flags & CONN_LOST != 0) {
        releaseViews(views);
        return;
    }
    if (self.flags & (CLOSING | EOF_WRITTEN) != 0) {
        releaseViews(views);
        return py.errRuntime("Cannot call write() after write_eof() or close()");
    }
    appendPending(self, bufs, views);
    // Batching pays off only for consecutive writes from one callback, and a
    // caller outside the loop may block reading the peer before the flush could
    // ever run, so writes from a stopped loop go straight out.
    if (!self.loopState().running) flushPending(self);
}

/// What `write()` has accepted and not yet handed back to the socket, whether
/// it is sitting in the pending batch or in libuv's queue.
inline fn bufferedBytes(self: *Transport) usize {
    return self.write_buffer_size + self.pending_size;
}

fn maybePauseProtocol(self: *Transport) void {
    if (bufferedBytes(self) <= self.high_water) return;
    if (self.flags & PROTOCOL_PAUSED != 0) return;
    self.flags |= PROTOCOL_PAUSED;
    callProtocol(self, self.cb_pause_writing, null);
}

fn maybeResumeProtocol(self: *Transport) void {
    // A protocol that closed the transport from inside `pause_writing` - giving
    // up rather than buffering more - would otherwise be told to resume writing
    // to a connection that is already gone.
    if (self.flags & (CLOSING | CONN_LOST) != 0) return;
    if (self.flags & PROTOCOL_PAUSED == 0) return;
    if (bufferedBytes(self) > self.low_water) return;
    self.flags &= ~PROTOCOL_PAUSED;
    callProtocol(self, self.cb_resume_writing, null);
}

fn onShutdown(req: ?*uv.Shutdown, status: c_int) callconv(.c) void {
    _ = status;
    alloc.free(@as([*]u8, @ptrCast(req.?))[0..uv.uv_req_size(.shutdown)]);
}

/// Half-closes through `uv_shutdown`, the spelling that also covers Windows,
/// where the write side of a stream is not a file descriptor to `shutdown()`.
/// Only called with an empty write queue, so libuv has nothing to drain first.
fn shutdownWrite(self: *Transport) void {
    const raw = alloc.alignedAlloc(u8, .@"16", uv.uv_req_size(.shutdown)) catch return;
    const req: *uv.Shutdown = @ptrCast(raw.ptr);
    if (uv.uv_shutdown(req, self.stream(), onShutdown) < 0) alloc.free(raw);
}

// ---------------------------------------------------------------------------
// teardown

/// libuv owns the descriptor, so the socket object handed out through
/// `get_extra_info("socket")` must be detached rather than closed - otherwise
/// Python would close a descriptor libuv has already closed and reused.
fn releaseSocketView(self: *Transport) void {
    const view = self.socket_view orelse return;
    self.socket_view = null;
    const method = if (self.flags & PIPE_OWNED != 0) str_pipe_close else str_sock_detach;
    const result = c.PyObject_CallMethodNoArgs(view, method);
    if (result) |r| py.decref(r) else c.PyErr_Clear();
    py.decref(view);
}

fn onClosed(handle: ?*uv.Handle) callconv(.c) void {
    const self: *Transport = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();

    releaseSocketView(self);
    self.flags |= CONN_LOST;
    scheduleCall(self, self.cb_connection_lost, self.conn_lost_exc orelse py.none());
    if (self.server) |server| {
        const res = c.PyObject_CallMethodOneArg(server, str_detach, @ptrCast(self));
        if (res) |r| py.decref(r) else py.writeUnraisable(@ptrCast(self));
    }
    if (self.owner_prev) |prev| {
        prev.owner_next = self.owner_next;
    } else {
        st.transport_head = self.owner_next;
    }
    if (self.owner_next) |next| next.owner_prev = self.owner_prev;
    self.owner_prev = null;
    self.owner_next = null;
    self.flags &= ~HANDLE_REF;
    py.decref(self);
}

/// Releases the temporary reference that keeps an initialized libuv handle's
/// embedded Python object alive while an unsuccessful open is closed.
fn onOpenFailed(handle: ?*uv.Handle) callconv(.c) void {
    const self: *Transport = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();
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

/// Closes a transport discovered while the owning loop is shutting down.
/// The transport callback must be preserved because it releases the native
/// self-reference acquired by `makeTransport`.
pub fn closeFromLoop(handle: *uv.Handle) void {
    const self: *Transport = @ptrCast(@alignCast(uv.getData(handle)));
    self.flags |= CLOSING;
    self.flags &= ~OPEN;
    if (self.flags & READING != 0) {
        _ = uv.uv_read_stop(self.stream());
        self.flags &= ~READING;
    }
    uv.uv_close(handle, onClosed);
}

fn closeTransport(self: *Transport) void {
    self.flags |= CLOSE_REQUESTED;
    if (self.flags & CLOSING != 0) return;
    flushPending(self);
    self.flags |= CLOSING;
    if (self.flags & READING != 0) {
        _ = uv.uv_read_stop(self.stream());
        self.flags &= ~READING;
    }
    if (self.write_buffer_size == 0) shutdownAndClose(self);
}

fn forceClose(self: *Transport, exc: ?*py.Object) void {
    self.flags |= CLOSE_REQUESTED;
    discardPending(self);
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

/// `_adopt_pipe(pipe)`: the pipe this transport was handed. asyncio makes the
/// transport its owner, so it is closed when the transport is - unlike a socket
/// view, whose descriptor belongs to libuv and is only detached.
fn adoptPipe(self_obj: *py.Object, pipe: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    py.clear(&self.socket_view);
    py.incref(pipe);
    self.socket_view = pipe;
    self.flags |= PIPE_OWNED;
    return py.noneRef();
}

/// `_adopt_socket_view(sock)`: the socket object mirroring libuv's descriptor.
fn adoptSocketView(self_obj: *py.Object, sock: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    releaseSocketView(self);
    py.incref(sock);
    self.socket_view = sock;
    return py.noneRef();
}

fn setProtocol(self_obj: *py.Object, protocol: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    if (self.view.obj != null) c.PyBuffer_Release(&self.view);
    bindProtocol(self, protocol);
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
    var views = [_]c.Py_buffer{undefined};
    if (c.PyObject_GetBuffer(data, &views[0], c.PyBUF_SIMPLE) < 0) return py.Error.Python;
    if (views[0].len == 0) {
        c.PyBuffer_Release(&views[0]);
        return py.noneRef();
    }
    var bufs = [_]uv.Buf{.{ .base = @ptrCast(views[0].buf), .len = @intCast(views[0].len) }};
    try submitWrite(self, &bufs, &views);
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
    defer if (n > inline_bufs) alloc.free(bufs);
    const views = if (n <= inline_bufs) inline_views[0..n] else alloc.alloc(c.Py_buffer, n) catch return py.errNoMemory();
    defer if (n > inline_bufs) alloc.free(views);

    var filled: usize = 0;
    while (filled < n) : (filled += 1) {
        const item = c.PySequence_GetItem(seq, @intCast(filled)) orelse {
            releaseViews(views[0..filled]);
            return py.Error.Python;
        };
        // The buffer view keeps the exporter alive, so the item reference can go.
        const acquired = c.PyObject_GetBuffer(item, &views[filled], c.PyBUF_SIMPLE);
        py.decref(item);
        if (acquired < 0) {
            releaseViews(views[0..filled]);
            return py.Error.Python;
        }
        bufs[filled] = .{ .base = @ptrCast(views[filled].buf), .len = @intCast(views[filled].len) };
    }
    try submitWrite(self, bufs, views);
    return py.noneRef();
}

fn writeEof(self_obj: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    if (self.flags & (CONN_LOST | EOF_WRITTEN | CLOSING) != 0) return py.noneRef();
    flushPending(self);
    self.flags |= EOF_WRITTEN;
    // A half-close must follow all writes that were accepted before this call.
    // The final write callback performs it when libuv still owns queued data.
    if (self.write_buffer_size == 0) shutdownWrite(self);
    return py.noneRef();
}

fn canWriteEof(self_obj: *py.Object) py.Error!*py.Object {
    _ = self_obj;
    return py.boolRef(true);
}

fn getWriteBufferSize(self_obj: *py.Object) py.Error!*py.Object {
    return py.int(bufferedBytes(asTransport(self_obj))) orelse py.Error.Python;
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
        if (!py.isNone(value)) {
            const parsed = try py.asIsize(value);
            if (parsed < 0) return py.errValue("high water mark must be non-negative");
            high_water = @intCast(parsed);
        }
    }
    var low_water: usize = high_water / 4;
    if (low) |value| {
        if (!py.isNone(value)) {
            const parsed = try py.asIsize(value);
            if (parsed < 0) return py.errValue("low water mark must be non-negative");
            low_water = @intCast(parsed);
        }
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

/// `loop._make_transport(fd, kind, protocol, waiter, extra, server)`
pub fn makeTransport(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    try py.expectArgs(args, 6, "_make_transport");
    const loop = loopmod.asLoop(self_obj);
    const st = loop.state();
    try loopmod.checkClosed(st);

    const fd = try py.asCInt(args[0].?);
    const kind = try py.asCInt(args[1].?);

    const obj = c.PyType_GenericAlloc(transport_type, 0) orelse return py.Error.Python;
    const self = asTransport(obj);
    errdefer py.decref(obj);

    self.state = st;
    self.high_water = default_high_water;
    self.low_water = default_high_water / 4;
    self.kind = kind;
    self.read_size = read_size_min;
    self.context = c.PyContext_CopyCurrent();
    if (self.context == null) return py.Error.Python;

    // Everything after descriptor adoption must be infallible: otherwise the
    // Python socket would still believe it owns a descriptor now owned by
    // libuv. Prepare callbacks, handles and queue capacity first.
    bindProtocol(self, args[2].?);
    const connection_made = c.PyObject_GetAttr(args[2].?, str_connection_made) orelse return py.Error.Python;
    defer py.decref(connection_made);
    // A write pipe still watches for readability, which is how asyncio notices the
    // peer closing. Only on a socket, though: on a PTY it would steal the bytes a
    // paired read transport is waiting for, and an O_WRONLY FIFO cannot be read at
    // all - libuv rejects `uv_read_start` there with ENOTCONN.
    const reads = kind != KIND_PIPE_WRITE or isSocket(fd);
    const start = if (reads) c.PyObject_GetAttr(obj, str_start_reading) orelse return py.Error.Python else null;
    defer if (start) |s| py.decref(s);

    const connection_handle = try handlemod.create(
        handlemod.handle_type.?,
        self_obj,
        connection_made,
        &.{obj},
        null,
    );
    errdefer py.decref(connection_handle);
    const start_handle = if (start) |s|
        try handlemod.create(handlemod.handle_type.?, self_obj, s, &.{}, null)
    else
        null;
    errdefer if (start_handle) |h| py.decref(h);
    const waiter_handle = if (!py.isNone(args[3].?))
        try handlemod.create(
            handlemod.handle_type.?,
            self_obj,
            set_result_unless_cancelled.?,
            &.{ args[3], py.none() },
            null,
        )
    else
        null;
    errdefer if (waiter_handle) |h| py.decref(h);
    var queued: usize = 1;
    if (start_handle != null) queued += 1;
    if (waiter_handle != null) queued += 1;
    st.ready.ensureUnusedCapacity(queued) catch return py.errNoMemory();

    const init_status = if (kind == KIND_TCP)
        uv.uv_tcp_init_ex(st.uvloop, @ptrCast(self.stream()), 0)
    else
        uv.uv_pipe_init(st.uvloop, @ptrCast(self.stream()), 0);
    try py.errUvIfNeg(init_status);
    self.flags |= OPEN;
    uv.setData(self.stream(), self);

    const open_status = if (kind == KIND_TCP)
        uv.uv_tcp_open(@ptrCast(self.stream()), @intCast(fd))
    else
        uv.uv_pipe_open(@ptrCast(self.stream()), fd);
    if (open_status < 0) {
        // uv_close is asynchronous, and the handle's storage is embedded in
        // `obj`. Keep that storage alive until libuv has finished with it.
        py.incref(obj);
        uv.uv_close(uv.asHandle(self.stream()), onOpenFailed);
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
    // The loop owns every initialized transport until its close callback. Keep
    // the ownership edge explicit so cyclic GC can distinguish a live loop from
    // an unreachable loop/transport cycle.
    py.incref(obj);
    self.flags |= HANDLE_REF;
    self.owner_next = st.transport_head;
    if (self.owner_next) |next| next.owner_prev = self;
    st.transport_head = self;

    st.ready.pushAssumeCapacity(@ptrCast(connection_handle));
    if (start_handle) |h| st.ready.pushAssumeCapacity(@ptrCast(h));
    if (waiter_handle) |h| st.ready.pushAssumeCapacity(@ptrCast(h));
    loopmod.startIdle(st);
    return obj;
}

fn dealloc(obj: ?*py.Object) callconv(.c) void {
    const self = asTransport(obj.?);
    const tp = py.typeOf(obj.?);
    c.PyObject_GC_UnTrack(obj);
    c.PyObject_ClearWeakRefs(obj);
    c.PyObject_ClearManagedDict(obj);
    py.clear(&self.base_extra);
    releaseSocketView(self);
    py.clear(&self.context);
    if (self.view.obj != null) c.PyBuffer_Release(&self.view);
    py.clear(&self.read_bytes);
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
        self.base_extra,
        self.loop,
        self.protocol,
        self.server,
        self.extra,
        self.conn_lost_exc,
        self.read_bytes,
        self.socket_view,
        self.context,
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
    for (self.pending_views[0..self.pending_count]) |view| {
        const r = py.visit(@ptrCast(view.obj), visitproc, arg);
        if (r != 0) return r;
    }
    const managed = c.PyObject_VisitManagedDict(obj, visitproc, arg);
    if (managed != 0) return managed;
    return py.visit(@ptrCast(py.typeOf(obj.?)), visitproc, arg);
}

fn clear_(obj: ?*py.Object) callconv(.c) c_int {
    const self = asTransport(obj.?);
    discardPending(self);
    c.PyObject_ClearManagedDict(obj);
    py.clear(&self.base_extra);
    py.clear(&self.protocol);
    py.clear(&self.server);
    py.clear(&self.extra);
    py.clear(&self.conn_lost_exc);
    py.clear(&self.read_bytes);
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
    py.methodO("_adopt_socket_view", adoptSocketView, "Track the socket object mirroring libuv's descriptor."),
    py.methodO("_adopt_pipe", adoptPipe, "Take ownership of the pipe object backing this transport."),
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
    .{ .slot = c.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&dealloc)) },
    .{ .slot = c.Py_tp_traverse, .pfunc = @ptrCast(@constCast(&traverse)) },
    .{ .slot = c.Py_tp_clear, .pfunc = @ptrCast(@constCast(&clear_)) },
    .{ .slot = c.Py_tp_repr, .pfunc = @ptrCast(@constCast(&repr)) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_doc, .pfunc = @ptrCast(@constCast("A libuv-backed stream transport.")) },
    .{ .slot = 0, .pfunc = null },
};

var spec = c.PyType_Spec{
    .name = "zuvloop._zuvloop.Transport",
    .basicsize = 0,
    .itemsize = 0,
    // asyncio's transports are ordinary objects that accept attributes, and
    // callers - test suites especially - rely on being able to set them.
    // Not immutable: CPython forbids an immutable type deriving from a mutable
    // base, and `asyncio.Transport` is an ordinary class.
    .flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_MANAGED_WEAKREF |
        c.Py_TPFLAGS_MANAGED_DICT | c.Py_TPFLAGS_DISALLOW_INSTANTIATION,
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
    str_sock_detach = py.intern("detach") orelse return py.Error.Python;
    str_pipe_close = py.intern("close") orelse return py.Error.Python;
    str_high = py.intern("high") orelse return py.Error.Python;
    str_low = py.intern("low") orelse return py.Error.Python;
    str_start_reading = py.intern("_start_reading") orelse return py.Error.Python;
    str_set_result_unless_cancelled = py.intern("_set_result_unless_cancelled") orelse return py.Error.Python;
    set_result_unless_cancelled = py.importFrom("asyncio.futures", "_set_result_unless_cancelled") orelse return py.Error.Python;
    buffered_protocol_type = py.importFrom("asyncio.protocols", "BufferedProtocol") orelse return py.Error.Python;

    handle_offset = std.mem.alignForward(usize, @sizeOf(Transport), 16);
    write_req_offset = std.mem.alignForward(usize, @sizeOf(WriteReq), 16);
    const handle_size = @max(uv.uv_handle_size(.tcp), uv.uv_handle_size(.named_pipe));
    spec.basicsize = @intCast(handle_offset + handle_size);

    // Inheriting from `asyncio.Transport` is what makes `isinstance(t, asyncio.Transport)`
    // hold, which protocols in the wild - aiohttp's test suite among them - assert on.
    // We override every method it declares, so nothing of its behaviour survives; only
    // its instance layout does, and `base_extra` reserves exactly that.
    const base = py.importFrom("asyncio.transports", "Transport") orelse return py.Error.Python;
    defer py.decref(base);
    if (@as(*c.PyTypeObject, @ptrCast(base)).tp_basicsize != @offsetOf(Transport, "loop")) {
        return py.errRuntime("asyncio.Transport instance layout is not the one zuvloop was built against");
    }
    const bases = c.PyTuple_Pack(1, base) orelse return py.Error.Python;
    defer py.decref(bases);

    transport_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &spec, bases) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "Transport", @ptrCast(transport_type)) < 0) return py.Error.Python;
    if (c.PyModule_AddIntConstant(module, "KIND_TCP", KIND_TCP) < 0) return py.Error.Python;
    if (c.PyModule_AddIntConstant(module, "KIND_PIPE", KIND_PIPE) < 0) return py.Error.Python;
    if (c.PyModule_AddIntConstant(module, "KIND_PIPE_WRITE", KIND_PIPE_WRITE) < 0) return py.Error.Python;
}
