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
const protocolmod = @import("protocol.zig");
const LoopObject = loopmod.LoopObject;

const alloc = std.heap.c_allocator;

const Flags = packed struct(u32) {
    reading: bool = false,
    closing: bool = false,
    conn_lost: bool = false,
    eof_written: bool = false,
    buffered: bool = false,
    protocol_paused: bool = false,
    open: bool = false,
    flush_queued: bool = false,
    handle_ref: bool = false,
    /// The tracked object is a pipe this transport owns, so closing it is the
    /// transport's job. A socket view is only detached - libuv owns its descriptor.
    pipe_owned: bool = false,
    /// The application asked for this transport to close. Loop teardown also sets
    /// `closing`, and `repr` has to tell the two apart: asyncio still calls a transport
    /// "open" after `loop.close()`, because that never delivers `connection_lost`.
    close_requested: bool = false,
    allocating_read_buffer: bool = false,
    read_state_pending: bool = false,
    previous_read_full: bool = false,
    _: u18 = 0,
};

pub const KIND_TCP: c_int = 0;
pub const KIND_PIPE: c_int = 1;
/// A pipe whose descriptor is write-only, so no read is ever started on it.
pub const KIND_PIPE_WRITE: c_int = 2;

/// Reads land in a `bytes` object sized to what the peer has been sending.
///
/// A fixed size cannot serve both shapes of traffic: a large buffer wastes an
/// allocation on every small request, while a small one caps bulk transfer by
/// forcing a syscall per chunk. The size follows the traffic instead, doubling
/// whenever a read fills the buffer and easing back when it does not.
const read_size_min: usize = 16 * 1024;
const read_size_max: usize = 256 * 1024;

/// At or below this, a read is copied out of a shared buffer into an exactly
/// sized `bytes`; above it, libuv fills the final object directly. This keeps a
/// 64 KiB body plus its framing in one read without allocating a large object
/// and shrinking it for every request.
pub const copy_threshold: usize = 128 * 1024;
const inline_bufs = 16;

/// Writes issued within one loop iteration are held here and sent together.
///
/// A protocol sends a response in pieces - a header block and a body, which is
/// what ASGI and aiohttp both do - and writing each piece as it arrives costs a
/// syscall per piece. Holding them until the iteration ends turns the whole
/// response into one vectored write. Four slots cover consecutive `write()`
/// calls. A larger `writelines()` call is already a complete vector and goes
/// directly to the socket without using this storage.
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
    flags: Flags,
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

/// A queued write: the libuv request, immutable buffer views, and the vector
/// libuv reads from - all in one allocation.
///
/// An exact `bytes` object is retained without copying. Every other exporter is
/// snapshotted first, so its view owns immutable bytes rather than caller memory;
/// either way the backing object stays alive until libuv reports completion.
///
/// The request does not take another Python reference to `transport`. The
/// loop-owned handle reference remains alive until `onClosed`, and libuv runs
/// every outstanding request callback before that close callback.
///
/// A write that libuv cannot take as one request is queued as several. The
/// first of them owns the views for all, and outlives its own completion until
/// the last one is done.
const WriteReq = struct {
    transport: *Transport,
    size: usize,
    total: usize,
    nviews: usize,
    owner: *WriteReq,
    /// On the owner: how many requests of its write libuv still holds.
    pending: usize = 0,

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

    fn free(self: *WriteReq) void {
        alloc.free(@as([*]align(16) u8, @ptrCast(@alignCast(self)))[0..self.total]);
    }

    /// Called once libuv is done with this request, whatever the outcome.
    fn done(self: *WriteReq) void {
        const owner = self.owner;
        if (self != owner) self.free();
        owner.pending -= 1;
        if (owner.pending != 0) return;
        releaseViews(owner.views()[0..owner.nviews]);
        owner.free();
    }
};

/// libuv refuses a request whose buffers add up to more than this - its
/// `UV__IO_MAX_BYTES` - because some platforms fail such a write outright.
const io_max: usize = 0x7ffff000;

/// The most buffers libuv takes in one request.
const bufs_max: usize = 1024 * 1024;

var write_req_offset: usize = 0;

fn releaseViews(views: []c.Py_buffer) void {
    for (views) |*view| c.PyBuffer_Release(view);
}

/// Hold immutable bytes directly; snapshot every other exporter before write()
/// returns so queued I/O cannot observe later mutations by the caller.
fn acquireWriteView(data: *py.Object, view: *c.Py_buffer) py.Error!void {
    if (py.isExactBytes(data)) {
        if (c.PyObject_GetBuffer(data, view, c.PyBUF_SIMPLE) < 0) return py.Error.Python;
        return;
    }

    // SAFETY: PyObject_GetBuffer initializes source or returns an error.
    var source: c.Py_buffer = undefined;
    if (c.PyObject_GetBuffer(data, &source, c.PyBUF_SIMPLE) < 0) return py.Error.Python;
    defer c.PyBuffer_Release(&source);
    const snapshot = c.PyBytes_FromStringAndSize(@ptrCast(source.buf), source.len) orelse return py.Error.Python;
    defer py.decref(snapshot);
    if (c.PyObject_GetBuffer(snapshot, view, c.PyBUF_SIMPLE) < 0) return py.Error.Python;
}

// ---------------------------------------------------------------------------
// protocol dispatch

/// Re-reads which reading protocol the object implements. `set_protocol` can
/// swap a plain protocol for a buffered one - `start_tls` does exactly that -
/// so bufferedness is decided here rather than by the caller.
fn bindProtocol(self: *Transport, protocol: *py.Object) void {
    py.clear(&self.protocol);
    py.incref(protocol);
    self.protocol = protocol;
    protocolmod.cacheCallback(protocol, str_connection_lost, &self.cb_connection_lost);
    protocolmod.cacheCallback(protocol, str_eof_received, &self.cb_eof_received);
    protocolmod.cacheCallback(protocol, str_pause_writing, &self.cb_pause_writing);
    protocolmod.cacheCallback(protocol, str_resume_writing, &self.cb_resume_writing);

    py.clear(&self.cb_get_buffer);
    py.clear(&self.cb_buffer_updated);
    py.clear(&self.cb_data_received);

    const buffered = c.PyObject_IsInstance(protocol, buffered_protocol_type);
    if (buffered < 0) c.PyErr_Clear();
    if (buffered == 1) {
        self.flags.buffered = true;
        protocolmod.cacheCallback(protocol, str_get_buffer, &self.cb_get_buffer);
        protocolmod.cacheCallback(protocol, str_buffer_updated, &self.cb_buffer_updated);
    } else {
        self.flags.buffered = false;
        protocolmod.cacheCallback(protocol, str_data_received, &self.cb_data_received);
    }
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
            protocolmod.reportError(self.loop.?, @ptrCast(self), "Error entering the transport context");
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
        if (loopmod.isExit(exc)) {
            c.PyErr_SetRaisedException(exc);
            loopmod.captureFatal(loop);
            forceClose(self, null);
            return;
        }
        loopmod.callExceptionHandler(loop, failed, exc, @ptrCast(self));
        forceClose(self, exc);
    }
}

// ---------------------------------------------------------------------------
// reading

fn onAlloc(handle: ?*uv.Handle, suggested: usize, buf: *uv.Buf) callconv(.c) void {
    _ = suggested;
    const self: *Transport = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();

    // Handing back the loop's shared buffer touches no Python object, and libuv
    // asks for one on every readable event, so entering Python for the rest of
    // this callback would be paid per read for nothing.
    if (!self.flags.buffered and self.read_bytes == null and self.read_size <= copy_threshold) {
        if (loopmod.scratchBuffer(st)) |scratch| {
            buf.* = uv.Buf.init(scratch, self.read_size);
            return;
        }
    }

    st.pythonEnter();
    defer st.pythonExit();

    if (self.flags.buffered) {
        self.flags.allocating_read_buffer = true;
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
                if (self.flags.read_state_pending) {
                    buf.* = .{ .base = @ptrFromInt(@alignOf(u8)), .len = 0 };
                    return;
                }
                // A short read is always allowed, so a buffer wider than the
                // platform's vector element is simply not filled past it.
                const avail = @min(@as(usize, @intCast(self.view.len)), uv.Buf.max_len);
                buf.* = uv.Buf.init(@ptrCast(self.view.buf), avail);
                return;
            }
        }
        protocolmod.reportError(self.loop.?, @ptrCast(self), "Error allocating a receive buffer");
        buf.* = .{ .base = @ptrFromInt(@alignOf(u8)), .len = 0 };
        return;
    }

    py.clear(&self.read_bytes);
    if (self.read_size <= copy_threshold) {
        // Small traffic: read into the shared buffer and allocate exactly what
        // arrived. A null read_bytes marks this path for onRead.
        if (loopmod.scratchBuffer(st)) |scratch| {
            buf.* = uv.Buf.init(scratch, self.read_size);
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
    buf.* = uv.Buf.init(@ptrCast(c.PyBytes_AsString(target)), self.read_size);
}

fn onRead(stream: ?*uv.Stream, nread: isize, buf: *const uv.Buf) callconv(.c) void {
    const self: *Transport = @ptrCast(@alignCast(uv.getData(stream.?)));
    const st = self.loopState();
    st.pythonEnter();
    defer st.pythonExit();

    const allocated_with_python = self.flags.allocating_read_buffer;
    self.flags.allocating_read_buffer = false;
    const buffered = self.flags.buffered;
    if (buffered and self.view.obj != null) c.PyBuffer_Release(&self.view);
    if (allocated_with_python and self.flags.read_state_pending) {
        self.flags.read_state_pending = false;
        py.clear(&self.read_bytes);
        if (self.flags.reading) {
            _ = uv.uv_read_stop(self.stream());
            self.flags.reading = false;
        }
        if (self.flags.closing and self.write_buffer_size == 0) shutdownAndClose(self);
        return;
    }

    // A write pipe reads only to learn that the peer went away; asyncio arms the
    // descriptor for the same reason and never delivers what arrives on it.
    if (self.kind == KIND_PIPE_WRITE) {
        py.clear(&self.read_bytes);
        if (nread == 0) return;
        if (bufferedBytes(self) != 0) {
            forceClose(self, protocolmod.takeUvError(uv.EPIPE));
        } else {
            closeTransport(self);
        }
        return;
    }

    if (nread > 0) {
        if (buffered) {
            const n = py.int(@as(c.Py_ssize_t, @intCast(nread))) orelse {
                forceClose(self, c.PyErr_GetRaisedException());
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
                    forceClose(self, c.PyErr_GetRaisedException());
                    return;
                }
                break :blk resized;
            }
            break :blk py.bytes(buf.base[0..@intCast(nread)]) orelse {
                forceClose(self, c.PyErr_GetRaisedException());
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
    forceClose(self, protocolmod.takeUvError(@intCast(nread)));
}

fn repr(obj: ?*py.Object) callconv(.c) ?*py.Object {
    const self: *Transport = @ptrCast(@alignCast(obj.?));
    const name = py.typeOf(obj.?).tp_name;
    if (!self.flags.close_requested) return c.PyUnicode_FromFormat("<%s open>", name);
    if (self.flags.conn_lost) return c.PyUnicode_FromFormat("<%s closed>", name);
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

/// A full buffer means the peer had more to give, so read bigger next time.
fn adjustReadSize(self: *Transport, nread: usize) void {
    if (nread >= self.read_size) {
        self.read_size = @min(self.read_size * 2, read_size_max);
        self.flags.previous_read_full = true;
    } else if (self.flags.previous_read_full) {
        // A short remainder after a full read marks a message boundary, not a smaller traffic shape.
        self.flags.previous_read_full = false;
    } else if (nread * 4 <= self.read_size) {
        self.read_size = @max(self.read_size / 2, read_size_min);
    }
}

fn handleEof(self: *Transport) void {
    _ = uv.uv_read_stop(self.stream());
    self.flags.reading = false;
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
        protocolmod.reportError(self.loop.?, @ptrCast(self), "Error in eof_received");
        closeTransport(self);
        return;
    };
    defer py.decref(kept);
    if (c.PyObject_IsTrue(kept) != 1) closeTransport(self);
}

fn startReading(self: *Transport) py.Error!void {
    if (self.flags.reading or self.flags.closing or self.flags.conn_lost) return;
    try py.errUvIfNeg(uv.uv_read_start(self.stream(), onAlloc, onRead));
    self.flags.reading = true;
}

// ---------------------------------------------------------------------------
// writing

fn onWritten(req: ?*uv.Write, status: c_int) callconv(.c) void {
    const wr: *WriteReq = @ptrCast(@alignCast(uv.getData(req.?)));
    const self = wr.transport;
    const st = self.loopState();
    st.pythonEnter();
    defer st.pythonExit();

    // abort() zeroes the queue while requests are still in flight.
    self.write_buffer_size -= @min(wr.size, self.write_buffer_size);
    wr.done();

    if (status < 0) {
        // Closing the handle completes everything still queued with ECANCELED,
        // and so does a sibling request failing. The reason for the close is
        // already recorded by then, and this is not it. `OPEN` is cleared
        // immediately before `uv_close` on both teardown paths, so it separates
        // a real write failure from libuv tidying up after one.
        if (self.flags.open) forceClose(self, protocolmod.takeUvError(status));
        return;
    }
    maybeResumeProtocol(self);
    if (self.write_buffer_size == 0) {
        if (self.flags.closing) {
            shutdownAndClose(self);
        } else if (self.flags.eof_written) {
            shutdownWrite(self);
        }
    }
}

/// Takes ownership of `views`, releasing them once libuv reports completion.
/// Consumes `bufs`.
fn queueWrite(self: *Transport, bufs: []uv.Buf, views: []c.Py_buffer) py.Error!void {
    var remaining: usize = 0;
    for (bufs) |b| remaining += b.len;
    if (remaining == 0) {
        releaseViews(views);
        return;
    }

    var rest = bufs;
    var owner: ?*WriteReq = null;
    while (remaining != 0) {
        // A request of nothing but empty buffers would count as part of the
        // write being queued, and turn a later failure into a closed transport.
        while (rest[0].len == 0) rest = rest[1..];
        var whole: usize = 0;
        var size: usize = 0;
        while (whole < rest.len and whole < bufs_max and size + rest[whole].len <= io_max) : (whole += 1) {
            size += rest[whole].len;
        }
        // What still fits of a buffer that has to be cut to stay within the limit.
        const part = if (whole < rest.len and whole < bufs_max) io_max - size else 0;
        const count = whole + @intFromBool(part != 0);
        const nviews = if (owner == null) views.len else 0;

        const total = write_req_offset + uv.uv_req_size(.write) + nviews * @sizeOf(c.Py_buffer) + count * @sizeOf(uv.Buf);
        // Once a request is queued, a failure cannot be raised to the caller: the
        // start of the write is on its way, and a retry would send it twice.
        // Requests already queued keep the views until libuv is done with them.
        const raw = alloc.alignedAlloc(u8, .@"16", total) catch {
            if (owner == null) {
                releaseViews(views);
                return py.errNoMemory();
            }
            _ = c.PyErr_NoMemory();
            forceClose(self, c.PyErr_GetRaisedException());
            return;
        };
        const wr: *WriteReq = @ptrCast(raw.ptr);
        wr.* = .{ .transport = self, .size = size + part, .total = total, .nviews = nviews, .owner = owner orelse wr };
        @memcpy(wr.views()[0..nviews], views[0..nviews]);
        @memcpy(wr.bufs()[0..count], rest[0..count]);
        if (part != 0) wr.bufs()[whole].len = @intCast(part);
        uv.setData(wr.req(), wr);

        const status = uv.uv_write(wr.req(), self.stream(), wr.bufs(), @intCast(count), onWritten);
        if (status < 0) {
            wr.free();
            if (owner == null) {
                releaseViews(views);
                return py.errUv(status);
            }
            forceClose(self, protocolmod.takeUvError(status));
            return;
        }
        owner = wr.owner;
        wr.owner.pending += 1;
        self.write_buffer_size += wr.size;
        remaining -= wr.size;
        rest = rest[whole..];
        if (part != 0) {
            rest[0].base += part;
            rest[0].len -= @intCast(part);
        }
    }
}

/// Whether a write may proceed, releasing `views` if it may not.
///
/// A close only drops the data; raising would turn an ordinary shutdown race
/// into an exception. `write_eof()` is the caller's mistake, and must be checked
/// here too because acquiring an iterable or buffer can reenter the transport.
fn acceptsWrite(self: *Transport, views: []c.Py_buffer) py.Error!bool {
    if (self.flags.eof_written) {
        releaseViews(views);
        return py.errRuntime("Cannot write after write_eof()");
    }
    if (self.flags.conn_lost or self.flags.closing) {
        releaseViews(views);
        return false;
    }
    return true;
}

/// Writes what the socket accepts immediately and queues the rest.
/// Takes ownership of `views` on every path.
fn writeBufs(self: *Transport, bufs: []uv.Buf, views: []c.Py_buffer) py.Error!void {
    if (!try acceptsWrite(self, views)) return;

    var pending = bufs;
    var sent_some = false;
    if (self.write_buffer_size == 0) {
        // Offer no more than libuv's limits: the buffers that fit under them,
        // or the start of the first one when that alone is over.
        var offered: usize = 0;
        var size: usize = 0;
        while (offered < pending.len and offered < bufs_max and size + pending[offered].len <= io_max) : (offered += 1) {
            size += pending[offered].len;
        }
        const first_len = pending[0].len;
        if (offered == 0) {
            pending[0].len = @intCast(io_max);
            offered = 1;
        }
        const written = uv.uv_try_write(self.stream(), pending.ptr, @intCast(offered));
        pending[0].len = first_len;
        if (written > 0) {
            sent_some = true;
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
            pending[0].len -= @intCast(remaining);
        } else if (written < 0 and written != uv.EAGAIN) {
            releaseViews(views);
            forceClose(self, protocolmod.takeUvError(written));
            return;
        }
    }
    queueWrite(self, pending, views) catch |err| {
        if (!sent_some) return err;
        // As in `queueWrite`: the start of the write is gone, so the failure
        // closes the transport rather than inviting a retry of the whole.
        forceClose(self, c.PyErr_GetRaisedException());
    };
}

/// Hands everything accepted this iteration to libuv as one vectored write, and
/// tells the protocol if that moved it across a water mark.
///
/// Called from the loop's prepare handle, once per turn.
pub fn flushPending(self: *Transport) void {
    sendPending(self);
    maybePauseProtocol(self);
    maybeResumeProtocol(self);
}

/// Empties the batch into the socket without telling the protocol anything.
///
/// Used inside an operation that is not finished - accepting a write, closing,
/// half-closing - where `pause_writing` or `resume_writing` would let the
/// protocol act on a transport that is halfway through changing state. The
/// caller reports flow control once its own state is consistent.
fn sendPending(self: *Transport) void {
    const count = self.pending_count;
    if (count == 0) return;
    self.pending_count = 0;
    self.pending_size = 0;
    writeBufs(self, self.pending_bufs[0..count], self.pending_views[0..count]) catch {
        // Nothing above us can act on a write failure - asyncio reports it
        // through the protocol - and the views are already released.
        const exc = c.PyErr_GetRaisedException();
        forceClose(self, exc);
    };
}

/// Drops writes that will never reach the socket, releasing what they pinned.
pub fn discardPending(self: *Transport) void {
    const count = self.pending_count;
    self.pending_count = 0;
    self.pending_size = 0;
    releaseViews(self.pending_views[0..count]);
}

/// Accepts a write, deferring it unless the complete vector exceeds the batch.
/// Takes ownership of `views` on every path.
fn submitWrite(self: *Transport, bufs: []uv.Buf, views: []c.Py_buffer) py.Error!void {
    if (self.pending_count == 0 and bufs.len > pending_max) {
        try writeBufs(self, bufs, views);
    } else {
        if (!try acceptsWrite(self, views)) return;
        for (bufs, views, 0..) |b, v, i| {
            if (self.pending_count == pending_max) {
                sendPending(self);
                // A failed write closes the transport, which leaves nowhere to send the rest.
                if (self.flags.conn_lost or self.flags.closing) {
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
        // Batching pays off only for consecutive writes from one callback, and a
        // caller outside the loop may block reading the peer before the flush could
        // ever run, so writes from a stopped loop go straight out.
        //
        // A batch that first crosses the high-water mark goes out too. Only the
        // socket can tell whether the protocol has to pause, and holding the batch
        // until the end of the turn would let `drain()` return with all of it still
        // buffered. Once paused, writes batch again.
        const crossed = !self.flags.protocol_paused and bufferedBytes(self) > self.high_water;
        if (!self.loopState().running or crossed) sendPending(self);
    }
    // Only now is the whole vector accepted, so whatever the protocol does from
    // either callback - closing included - comes after it.
    maybePauseProtocol(self);
    maybeResumeProtocol(self);
}

/// What `write()` has accepted and not yet handed back to the socket, whether
/// it is sitting in the pending batch or in libuv's queue.
inline fn bufferedBytes(self: *Transport) usize {
    // `write_buffer_size` only drops when a whole request completes, while libuv's
    // count drops as each request drains, so a slow reader still shows progress.
    // It is zero once nothing is outstanding, including right after an abort.
    const queued = if (self.write_buffer_size == 0) 0 else uv.uv_stream_get_write_queue_size(self.stream());
    return queued + self.pending_size;
}

fn maybePauseProtocol(self: *Transport) void {
    if (bufferedBytes(self) <= self.high_water) return;
    if (self.flags.protocol_paused) return;
    self.flags.protocol_paused = true;
    protocolmod.call(self.loop.?, @ptrCast(self), self.cb_pause_writing, null);
}

fn maybeResumeProtocol(self: *Transport) void {
    // A protocol that closed the transport from inside `pause_writing` - giving
    // up rather than buffering more - would otherwise be told to resume writing
    // to a connection that is already gone.
    if (self.flags.closing or self.flags.conn_lost) return;
    if (!self.flags.protocol_paused) return;
    if (bufferedBytes(self) > self.low_water) return;
    self.flags.protocol_paused = false;
    protocolmod.call(self.loop.?, @ptrCast(self), self.cb_resume_writing, null);
}

fn onShutdown(req: ?*uv.Shutdown, status: c_int) callconv(.c) void {
    _ = status;
    alloc.free(@as([*]align(16) u8, @ptrCast(@alignCast(req.?)))[0..uv.uv_req_size(.shutdown)]);
}

/// Half-closes through `uv_shutdown`, the spelling that also covers Windows,
/// where the write side of a stream is not a file descriptor to `shutdown()`.
/// Only called with an empty write queue, so libuv has nothing to drain first.
fn shutdownWrite(self: *Transport) void {
    // Failing to allocate closes the connection outright: skipping the
    // half-close silently would leave the peer waiting for an EOF forever.
    const raw = alloc.alignedAlloc(u8, .@"16", uv.uv_req_size(.shutdown)) catch {
        forceClose(self, null);
        return;
    };
    const req: *uv.Shutdown = @ptrCast(raw.ptr);
    if (uv.uv_shutdown(req, self.stream(), onShutdown) < 0) alloc.free(raw);
}

// ---------------------------------------------------------------------------
// teardown

/// libuv owns the descriptor, so the socket object handed out through
/// `get_extra_info("socket")` must be detached rather than closed. The object
/// is disarmed before libuv closes the number, preventing a second close after
/// another thread reuses it.
fn releaseSocketView(self: *Transport) void {
    const method = if (self.flags.pipe_owned) str_pipe_close else str_sock_detach;
    protocolmod.releaseSocketView(&self.socket_view, method);
}

fn onClosed(handle: ?*uv.Handle) callconv(.c) void {
    const self: *Transport = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.externalHandleClosed();
    st.pythonEnter();
    defer st.pythonExit();

    self.flags.conn_lost = true;
    protocolmod.scheduleCall(self.loop.?, self.context, self.cb_connection_lost, self.conn_lost_exc orelse py.none());
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
    self.flags.handle_ref = false;
    py.decref(self);
}

/// Releases the temporary reference that keeps an initialized libuv handle's
/// embedded Python object alive while an unsuccessful open is closed.
fn onOpenFailed(handle: ?*uv.Handle) callconv(.c) void {
    const self: *Transport = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.externalHandleClosed();
    st.pythonEnter();
    defer st.pythonExit();
    py.decref(self);
}

fn shutdownAndClose(self: *Transport) void {
    if (!self.flags.open) return;
    self.flags.open = false;
    if (self.flags.reading) {
        _ = uv.uv_read_stop(self.stream());
        self.flags.reading = false;
    }
    // uv_close closes a stream descriptor synchronously. Disarm the Python
    // object first, or code that still holds the accepted socket can close the
    // same number after another thread has already reused it.
    releaseSocketView(self);
    const handle = uv.asHandle(self.stream());
    self.loopState().closeExternalHandle(handle, onClosed);
}

/// Closes a transport discovered while the owning loop is shutting down.
/// The transport callback must be preserved because it releases the native
/// self-reference acquired by `makeTransport`.
pub fn closeFromLoop(handle: *uv.Handle) void {
    const self: *Transport = @ptrCast(@alignCast(uv.getData(handle)));
    self.flags.closing = true;
    self.flags.open = false;
    if (self.flags.reading) {
        _ = uv.uv_read_stop(self.stream());
        self.flags.reading = false;
    }
    releaseSocketView(self);
    self.loopState().closeExternalHandle(handle, onClosed);
}

fn closeTransport(self: *Transport) void {
    self.flags.close_requested = true;
    if (self.flags.closing) return;
    // Not a flush: a write from `pause_writing` or `resume_writing` would land in
    // a batch nothing sends again, and a closing transport owes the protocol neither.
    sendPending(self);
    self.flags.closing = true;
    if (self.flags.allocating_read_buffer) {
        self.flags.read_state_pending = true;
        return;
    }
    if (self.flags.reading) {
        _ = uv.uv_read_stop(self.stream());
        self.flags.reading = false;
    }
    if (self.write_buffer_size == 0) shutdownAndClose(self);
}

fn forceClose(self: *Transport, exc: ?*py.Object) void {
    self.flags.close_requested = true;
    discardPending(self);
    if (exc) |e| {
        py.clear(&self.conn_lost_exc);
        self.conn_lost_exc = e;
    } else {
        c.PyErr_Clear();
    }
    self.flags.closing = true;
    self.write_buffer_size = 0;
    if (self.flags.allocating_read_buffer) {
        self.flags.read_state_pending = true;
        return;
    }
    shutdownAndClose(self);
}

// ---------------------------------------------------------------------------
// python methods

fn asTransport(obj: *py.Object) *Transport {
    return @ptrCast(@alignCast(obj));
}

fn getExtraInfo(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    return protocolmod.extraInfo(asTransport(self_obj).extra, args);
}

fn isClosing(self_obj: *py.Object) py.Error!*py.Object {
    const flags = asTransport(self_obj).flags;
    return py.boolRef(flags.closing or flags.conn_lost);
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
    self.flags.pipe_owned = true;
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
    return py.boolRef(asTransport(self_obj).flags.reading);
}

fn pauseReading(self_obj: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    if (self.flags.reading) {
        if (self.flags.allocating_read_buffer) {
            self.flags.read_state_pending = true;
            return py.noneRef();
        }
        try py.errUvIfNeg(uv.uv_read_stop(self.stream()));
        self.flags.reading = false;
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
    // SAFETY: acquireWriteView initializes the only element or returns an error.
    var views = [_]c.Py_buffer{undefined};
    try acquireWriteView(data, &views[0]);
    if (@as(usize, @intCast(views[0].len)) > uv.Buf.max_len) {
        c.PyBuffer_Release(&views[0]);
        return py.errOverflow("a single buffer above 4 GiB cannot be written on Windows");
    }
    if (self.flags.eof_written) {
        c.PyBuffer_Release(&views[0]);
        return py.errRuntime("Cannot call write() after write_eof()");
    }
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
    if (self.flags.eof_written) return py.errRuntime("Cannot call writelines() after write_eof()");
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
        acquireWriteView(item, &views[filled]) catch {
            py.decref(item);
            releaseViews(views[0..filled]);
            return py.Error.Python;
        };
        py.decref(item);
        if (@as(usize, @intCast(views[filled].len)) > uv.Buf.max_len) {
            releaseViews(views[0 .. filled + 1]);
            return py.errOverflow("a single buffer above 4 GiB cannot be written on Windows");
        }
        bufs[filled] = .{ .base = @ptrCast(views[filled].buf), .len = @intCast(views[filled].len) };
    }
    try submitWrite(self, bufs, views);
    return py.noneRef();
}

fn writeEof(self_obj: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    if (self.flags.conn_lost or self.flags.eof_written or self.flags.closing) return py.noneRef();
    sendPending(self);
    self.flags.eof_written = true;
    // A half-close must follow all writes that were accepted before this call.
    // The final write callback performs it when libuv still owns queued data.
    if (self.write_buffer_size == 0) shutdownWrite(self);
    // After the half-close is recorded, so a write from `resume_writing` is
    // refused there instead of failing later and aborting the connection.
    maybeResumeProtocol(self);
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

fn setWriteBufferLimits(
    self_obj: *py.Object,
    args: []const ?*py.Object,
    nargs: usize,
    kwnames: ?*py.Object,
) py.Error!*py.Object {
    const marks = try protocolmod.parseWriteBufferLimits(args, nargs, kwnames);
    const self = asTransport(self_obj);
    self.high_water = marks.high;
    self.low_water = marks.low;
    maybePauseProtocol(self);
    return py.noneRef();
}

fn forceCloseMethod(self_obj: *py.Object, exc: *py.Object) py.Error!*py.Object {
    const self = asTransport(self_obj);
    forceClose(self, if (py.isNone(exc)) null else py.newref(exc));
    return py.noneRef();
}

fn getProtocolPaused(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.boolRef(asTransport(self_obj.?).flags.protocol_paused);
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
    self.high_water = protocolmod.default_high_water;
    self.low_water = protocolmod.default_high_water / 4;
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
    st.external_handles += 1;
    self.flags.open = true;
    uv.setData(self.stream(), self);

    const open_status = if (kind == KIND_TCP)
        uv.uv_tcp_open(@ptrCast(self.stream()), uv.asSock(fd))
    else
        uv.uv_pipe_open(@ptrCast(self.stream()), fd);
    if (open_status < 0) {
        // uv_close is asynchronous, and the handle's storage is embedded in
        // `obj`. Keep that storage alive until libuv has finished with it.
        py.incref(obj);
        st.closeExternalHandle(uv.asHandle(self.stream()), onOpenFailed);
        self.flags.open = false;
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
    self.flags.handle_ref = true;
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

// asyncio's name for this state; its sendfile machinery reads it off transports.
var getsets = [_]c.PyGetSetDef{
    .{
        .name = "_protocol_paused",
        .get = py.wrapGet(getProtocolPaused),
        .set = null,
        .doc = "Whether flow control has paused the protocol.",
        .closure = null,
    },
    .{ .name = null, .get = null, .set = null, .doc = null, .closure = null },
};

var slots = [_]c.PyType_Slot{
    .{ .slot = c.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&dealloc)) },
    .{ .slot = c.Py_tp_traverse, .pfunc = @ptrCast(@constCast(&traverse)) },
    .{ .slot = c.Py_tp_clear, .pfunc = @ptrCast(@constCast(&py.wrapClear(clear_))) },
    .{ .slot = c.Py_tp_repr, .pfunc = @ptrCast(@constCast(&py.wrapRepr(repr))) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_getset, .pfunc = @ptrCast(&getsets) },
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
