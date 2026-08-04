//! The native event loop: scheduling, timers, and the libuv run loop.
//!
//! GIL discipline: `_run` releases the GIL for the whole `uv_run` call and each
//! libuv callback reacquires it through `gilEnter`/`gilExit`. `gil_depth` starts
//! at 1 because the object is created while the caller holds the GIL, so nested
//! runs (as in `close`) see a non-zero depth and skip the save/restore.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const uv = @import("uv.zig");
const collections = @import("collections.zig");
const handlemod = @import("handle.zig");
const pollermod = @import("poller.zig");
const dns = @import("dns.zig");
const transportmod = @import("transport.zig");
const Handle = handlemod.Handle;

const alloc = std.heap.c_allocator;

/// Timers below this many cancelled entries are never compacted.
const min_cancelled_timers = 100;

/// Lock-free LIFO of handles pushed from other threads, drained by the waker.
const Inbox = struct {
    head: ?*Handle = null,

    fn push(self: *Inbox, h: *Handle) void {
        while (true) {
            const old = @atomicLoad(?*Handle, &self.head, .monotonic);
            h.next = old;
            if (@cmpxchgWeak(?*Handle, &self.head, old, h, .release, .monotonic) == null) return;
        }
    }

    /// Returns the queued handles in push order.
    fn takeAll(self: *Inbox) ?*Handle {
        var node = @atomicRmw(?*Handle, &self.head, .Xchg, null, .acquire);
        var reversed: ?*Handle = null;
        while (node) |h| {
            node = h.next;
            h.next = reversed;
            reversed = h;
        }
        return reversed;
    }
};

pub const State = struct {
    uvloop: *uv.Loop,
    idle: *uv.Idle,
    timer: *uv.Timer,
    sampler: *uv.Timer,
    waker: *uv.Async,
    block: []u8,

    ready: collections.Ready = .empty,
    timers: collections.Timers = .empty,
    inbox: Inbox = .{},
    pollers: pollermod.Map = .empty,
    read_buf: ?[*]u8 = null,

    tstate: ?*c.PyThreadState = null,
    gil_depth: c_int = 1,
    fatal: ?*py.Object = null,
    metrics_cb: ?*py.Object = null,

    running: bool = false,
    closed: bool = false,
    stopping: bool = false,
    debug: bool = false,
    idle_active: bool = false,
    timer_active: bool = false,
    sampler_active: bool = false,

    thread_id: c_ulong = 0,
    slow_callback_duration: f64 = 0.1,
    callbacks_run: u64 = 0,
    iterations: u64 = 0,

    pub inline fn gilEnter(self: *State) void {
        if (self.gil_depth == 0) {
            c.PyEval_RestoreThread(self.tstate);
            self.tstate = null;
        }
        self.gil_depth += 1;
    }

    pub inline fn gilExit(self: *State) void {
        self.gil_depth -= 1;
        if (self.gil_depth == 0) self.tstate = c.PyEval_SaveThread();
    }
};

pub const LoopObject = extern struct {
    ob_base: c.PyObject,
    st: ?*State,

    pub inline fn state(self: *LoopObject) *State {
        return self.st.?;
    }
};

pub var loop_type: ?*c.PyTypeObject = null;

var str_call_exception_handler: ?*py.Object = null;
var str_message: ?*py.Object = null;
var str_exception: ?*py.Object = null;
var str_handle: ?*py.Object = null;
var str_context: ?*py.Object = null;
var str_on_slow_callback: ?*py.Object = null;

pub inline fn asLoop(obj: *py.Object) *LoopObject {
    return @ptrCast(@alignCast(obj));
}

/// Seconds on the loop's monotonic clock.
pub inline fn now() f64 {
    return @as(f64, @floatFromInt(uv.uv_hrtime())) / 1e9;
}

fn isHandleCancelled(obj: *py.Object) bool {
    const h: *Handle = @ptrCast(@alignCast(obj));
    return h.isCancelled();
}

// ---------------------------------------------------------------------------
// libuv callbacks

fn onIdle(idle: ?*uv.Idle) callconv(.c) void {
    const self: *LoopObject = @ptrCast(@alignCast(uv.getData(idle.?)));
    const st = self.state();
    st.gilEnter();
    defer st.gilExit();
    runReady(self);
    if (st.ready.len == 0 and st.idle_active) {
        _ = uv.uv_idle_stop(st.idle);
        st.idle_active = false;
    }
}

fn onTimer(timer: ?*uv.Timer) callconv(.c) void {
    const self: *LoopObject = @ptrCast(@alignCast(uv.getData(timer.?)));
    const st = self.state();
    st.gilEnter();
    defer st.gilExit();
    st.timer_active = false;
    collectDueTimers(self);
    armTimer(self);
}

fn onWake(waker: ?*uv.Async) callconv(.c) void {
    const self: *LoopObject = @ptrCast(@alignCast(uv.getData(waker.?)));
    const st = self.state();
    st.gilEnter();
    defer st.gilExit();
    var node = st.inbox.takeAll();
    while (node) |h| {
        node = h.next;
        h.next = null;
        st.ready.push(@as(*py.Object, @ptrCast(h))) catch py.decref(h);
    }
    startIdle(st);
}

fn onCloseFreeState(handle: ?*uv.Handle) callconv(.c) void {
    _ = handle;
}

// ---------------------------------------------------------------------------
// scheduling internals

/// Shared landing buffer for stream reads; grown once, reused forever.
pub fn readBuffer(st: *State) ?[*]u8 {
    if (st.read_buf) |b| return b;
    const buf = alloc.alloc(u8, transportmod.read_buffer_size) catch {
        c.PyErr_Clear();
        return null;
    };
    st.read_buf = buf.ptr;
    return buf.ptr;
}

pub inline fn startIdle(st: *State) void {
    if (!st.idle_active and st.ready.len != 0) {
        _ = uv.uv_idle_start(st.idle, onIdle);
        st.idle_active = true;
    }
}

/// Runs exactly the callbacks queued on entry. asyncio guarantees everything
/// already scheduled runs even if one of them calls `stop()`, so the batch is
/// never cut short.
fn runReady(self: *LoopObject) void {
    const st = self.state();
    st.iterations += 1;
    var remaining = st.ready.len;
    st.callbacks_run += remaining;
    while (remaining != 0) : (remaining -= 1) {
        const obj = st.ready.pop() orelse break;
        const h: *Handle = @ptrCast(@alignCast(obj));
        if (st.debug) {
            const started = uv.uv_hrtime();
            handlemod.run(h);
            const elapsed = @as(f64, @floatFromInt(uv.uv_hrtime() - started)) / 1e9;
            if (elapsed > st.slow_callback_duration) reportSlowCallback(self, obj, elapsed);
        } else {
            handlemod.run(h);
        }
        py.decref(obj);
    }
}

fn reportSlowCallback(self: *LoopObject, h: *py.Object, elapsed: f64) void {
    const duration = py.float(elapsed) orelse {
        c.PyErr_Clear();
        return;
    };
    defer py.decref(duration);
    const args = [_]?*py.Object{ @ptrCast(self), h, duration };
    const res = c.PyObject_VectorcallMethod(
        str_on_slow_callback,
        &args,
        3 | c.PY_VECTORCALL_ARGUMENTS_OFFSET,
        null,
    );
    if (res) |r| py.decref(r) else py.writeUnraisable(@ptrCast(self));
}

fn collectDueTimers(self: *LoopObject) void {
    const st = self.state();
    const deadline = now();
    while (st.timers.peek()) |entry| {
        if (entry.when > deadline) break;
        _ = st.timers.pop();
        const h: *Handle = @ptrCast(@alignCast(entry.handle));
        if (h.isCancelled()) {
            py.decref(entry.handle);
            if (st.timers.cancelled != 0) st.timers.cancelled -= 1;
            continue;
        }
        st.ready.push(entry.handle) catch py.decref(entry.handle);
    }
    startIdle(st);
}

/// Points the single libuv timer at the earliest live deadline.
fn armTimer(self: *LoopObject) void {
    const st = self.state();
    while (st.timers.peek()) |entry| {
        const h: *Handle = @ptrCast(@alignCast(entry.handle));
        if (!h.isCancelled()) break;
        _ = st.timers.pop();
        py.decref(entry.handle);
        if (st.timers.cancelled != 0) st.timers.cancelled -= 1;
    }
    const entry = st.timers.peek() orelse {
        if (st.timer_active) {
            _ = uv.uv_timer_stop(st.timer);
            st.timer_active = false;
        }
        return;
    };
    const delta = entry.when - now();
    // libuv's millisecond clock can fire up to 1ms early; +1 keeps callbacks
    // from running before their deadline, which asyncio callers rely on.
    const ms: u64 = if (delta <= 0) 0 else @intFromFloat(@ceil(delta * 1000.0) + 1);
    _ = uv.uv_timer_start(st.timer, onTimer, ms, 0);
    st.timer_active = true;
}

pub fn noteTimerCancelled(self: *LoopObject) void {
    const st = self.st orelse return;
    st.timers.cancelled += 1;
    if (st.timers.cancelled > min_cancelled_timers and st.timers.cancelled * 2 > st.timers.len) {
        st.timers.compact(isHandleCancelled);
        armTimer(self);
    }
}

fn captureFatal(self: *LoopObject) void {
    const st = self.state();
    const exc = c.PyErr_GetRaisedException();
    if (st.fatal == null) st.fatal = exc else py.xdecref(exc);
    st.stopping = true;
    uv.uv_stop(st.uvloop);
}

/// Routes a failed callback to `call_exception_handler`, except for the two
/// exceptions asyncio propagates out of `run_forever`.
pub fn handleCallbackError(h: *Handle) void {
    const loop_obj = h.loop orelse {
        c.PyErr_Clear();
        return;
    };
    const self = asLoop(loop_obj);
    if (c.PyErr_ExceptionMatches(@ptrCast(c.PyExc_SystemExit)) != 0 or
        c.PyErr_ExceptionMatches(@ptrCast(c.PyExc_KeyboardInterrupt)) != 0)
    {
        captureFatal(self);
        return;
    }
    const exc = c.PyErr_GetRaisedException() orelse return;
    defer py.decref(exc);
    callExceptionHandler(self, "Exception in callback", exc, @ptrCast(h));
}

pub fn callExceptionHandler(self: *LoopObject, comptime message: [:0]const u8, exc: *py.Object, h: ?*py.Object) void {
    const ctx = c.PyDict_New() orelse {
        py.writeUnraisable(@ptrCast(self));
        return;
    };
    defer py.decref(ctx);
    const msg = py.str(message) orelse {
        c.PyErr_Clear();
        return;
    };
    defer py.decref(msg);
    if (c.PyDict_SetItem(ctx, str_message, msg) < 0) {
        c.PyErr_Clear();
        return;
    }
    if (c.PyDict_SetItem(ctx, str_exception, exc) < 0) {
        c.PyErr_Clear();
        return;
    }
    if (h) |hh| {
        if (c.PyDict_SetItem(ctx, str_handle, hh) < 0) {
            c.PyErr_Clear();
            return;
        }
    }
    const res = c.PyObject_CallMethodOneArg(@ptrCast(self), str_call_exception_handler, ctx);
    if (res) |r| py.decref(r) else py.writeUnraisable(@ptrCast(self));
}

// ---------------------------------------------------------------------------
// argument handling

const Parsed = struct {
    positional: []const ?*py.Object,
    context: ?*py.Object = null,
};

/// asyncio's schedulers are called as `call_soon(cb, *args, context=ctx)`;
/// `context` is the only keyword any of them pass.
fn parseCall(args: []const ?*py.Object, nargs: usize, kwnames: ?*py.Object, comptime skip: usize) py.Error!Parsed {
    var ctx: ?*py.Object = null;
    if (kwnames) |names| {
        const n: usize = @intCast(c.PyTuple_Size(names));
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const key = c.PyTuple_GetItem(names, @intCast(i)) orelse return py.Error.Python;
            if (c.PyObject_RichCompareBool(key, str_context, c.Py_EQ) != 1) {
                return py.errType("only the 'context' keyword argument is supported");
            }
            const value = args[nargs + i];
            if (!py.isNone(value)) ctx = value;
        }
    }
    return .{ .positional = args[skip..nargs], .context = ctx };
}

pub inline fn checkClosed(st: *State) py.Error!void {
    if (st.closed) return py.errRuntime("Event loop is closed");
}

fn scheduleSoon(self: *LoopObject, callback: *py.Object, p: Parsed) py.Error!*py.Object {
    const st = self.state();
    try checkClosed(st);
    const h = try handlemod.create(handlemod.handle_type.?, @ptrCast(self), callback, p.positional, p.context);
    py.incref(h);
    st.ready.push(@as(*py.Object, @ptrCast(h))) catch {
        py.decref(h);
        py.decref(h);
        return py.errNoMemory();
    };
    startIdle(st);
    return @ptrCast(h);
}

// ---------------------------------------------------------------------------
// methods

fn callSoon(self_obj: *py.Object, args: []const ?*py.Object, nargs: usize, kwnames: ?*py.Object) py.Error!*py.Object {
    if (nargs == 0) return py.errType("call_soon() requires a callback");
    const p = try parseCall(args, nargs, kwnames, 1);
    return scheduleSoon(asLoop(self_obj), args[0].?, p);
}

fn callSoonThreadsafe(self_obj: *py.Object, args: []const ?*py.Object, nargs: usize, kwnames: ?*py.Object) py.Error!*py.Object {
    if (nargs == 0) return py.errType("call_soon_threadsafe() requires a callback");
    const self = asLoop(self_obj);
    const st = self.state();
    try checkClosed(st);
    const p = try parseCall(args, nargs, kwnames, 1);
    const h = try handlemod.create(handlemod.handle_type.?, self_obj, args[0].?, p.positional, p.context);
    py.incref(h);
    st.inbox.push(h);
    _ = uv.uv_async_send(st.waker);
    return @ptrCast(h);
}

fn scheduleAt(self: *LoopObject, when: f64, callback: *py.Object, p: Parsed) py.Error!*py.Object {
    const st = self.state();
    try checkClosed(st);
    const h = try handlemod.create(handlemod.timer_type.?, @ptrCast(self), callback, p.positional, p.context);
    h.flags |= handlemod.IS_TIMER;
    h.when = when;
    py.incref(h);
    st.timers.push(when, @as(*py.Object, @ptrCast(h))) catch {
        py.decref(h);
        py.decref(h);
        return py.errNoMemory();
    };
    armTimer(self);
    return @ptrCast(h);
}

fn callLater(self_obj: *py.Object, args: []const ?*py.Object, nargs: usize, kwnames: ?*py.Object) py.Error!*py.Object {
    if (nargs < 2) return py.errType("call_later() requires a delay and a callback");
    const delay = try py.asF64(args[0].?);
    const p = try parseCall(args, nargs, kwnames, 2);
    const when = now() + @max(delay, 0);
    return scheduleAt(asLoop(self_obj), when, args[1].?, p);
}

fn callAt(self_obj: *py.Object, args: []const ?*py.Object, nargs: usize, kwnames: ?*py.Object) py.Error!*py.Object {
    if (nargs < 2) return py.errType("call_at() requires a time and a callback");
    const when = try py.asF64(args[0].?);
    const p = try parseCall(args, nargs, kwnames, 2);
    return scheduleAt(asLoop(self_obj), when, args[1].?, p);
}

fn time(self_obj: *py.Object) py.Error!*py.Object {
    _ = self_obj;
    return py.float(now()) orelse py.Error.Python;
}

fn runLoop(self_obj: *py.Object) py.Error!*py.Object {
    const self = asLoop(self_obj);
    const st = self.state();
    try checkClosed(st);
    if (st.running) return py.errRuntime("This event loop is already running");

    st.running = true;
    st.stopping = false;
    st.thread_id = c.PyThread_get_thread_ident();
    startIdle(st);
    armTimer(self);

    st.gilExit();
    _ = uv.uv_run(st.uvloop, .default);
    st.gilEnter();

    st.running = false;
    st.stopping = false;
    st.thread_id = 0;

    if (st.fatal) |exc| {
        st.fatal = null;
        c.PyErr_SetRaisedException(exc);
        return py.Error.Python;
    }
    return py.noneRef();
}

fn stop(self_obj: *py.Object) py.Error!*py.Object {
    const st = asLoop(self_obj).state();
    st.stopping = true;
    if (!st.closed) uv.uv_stop(st.uvloop);
    return py.noneRef();
}

fn isRunning(self_obj: *py.Object) py.Error!*py.Object {
    return py.boolRef(asLoop(self_obj).state().running);
}

fn isClosed(self_obj: *py.Object) py.Error!*py.Object {
    return py.boolRef(asLoop(self_obj).state().closed);
}

fn getDebug(self_obj: *py.Object) py.Error!*py.Object {
    return py.boolRef(asLoop(self_obj).state().debug);
}

fn setDebug(self_obj: *py.Object, value: *py.Object) py.Error!*py.Object {
    asLoop(self_obj).state().debug = try py.isTrue(value);
    return py.noneRef();
}

fn getSlowCallbackDuration(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.float(asLoop(self_obj.?).state().slow_callback_duration);
}

fn setSlowCallbackDuration(self_obj: ?*py.Object, value: ?*py.Object, _: ?*anyopaque) callconv(.c) c_int {
    const v = py.asF64(value orelse return -1) catch return -1;
    asLoop(self_obj.?).state().slow_callback_duration = v;
    return 0;
}

/// Loop counters plus libuv's own, for the instrumentation layer.
fn metrics(self_obj: *py.Object) py.Error!*py.Object {
    return buildMetrics(asLoop(self_obj).state()) orelse py.Error.Python;
}

fn buildMetrics(st: *State) ?*py.Object {
    var info: uv.Metrics = std.mem.zeroes(uv.Metrics);
    var idle_ns: u64 = 0;
    if (!st.closed) {
        _ = uv.uv_metrics_info(st.uvloop, &info);
        idle_ns = uv.uv_metrics_idle_time(st.uvloop);
    }
    return c.Py_BuildValue(
        "{s:K,s:K,s:K,s:K,s:K,s:n,s:n,s:n}",
        "loop_count",
        info.loop_count,
        "events",
        info.events,
        "events_waiting",
        info.events_waiting,
        "idle_time_ns",
        idle_ns,
        "callbacks_run",
        st.callbacks_run,
        "ready",
        @as(c.Py_ssize_t, @intCast(st.ready.len)),
        "timers",
        @as(c.Py_ssize_t, @intCast(st.timers.len)),
        "watchers",
        @as(c.Py_ssize_t, @intCast(st.pollers.count())),
    );
}

fn onSampler(timer: ?*uv.Timer) callconv(.c) void {
    const self: *LoopObject = @ptrCast(@alignCast(uv.getData(timer.?)));
    const st = self.state();
    st.gilEnter();
    defer st.gilExit();
    const callback = st.metrics_cb orelse return;
    const snapshot = buildMetrics(st) orelse {
        py.writeUnraisable(@ptrCast(self));
        return;
    };
    defer py.decref(snapshot);
    const result = c.PyObject_CallOneArg(callback, snapshot);
    if (result) |r| py.decref(r) else py.writeUnraisable(@ptrCast(self));
}

/// `_start_metrics(interval_seconds, callback)`: samples on a libuv timer, so
/// instrumentation never enters the callback queue.
fn startMetrics(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    try py.expectArgs(args, 2, "_start_metrics");
    const self = asLoop(self_obj);
    const st = self.state();
    try checkClosed(st);
    const interval = try py.asF64(args[0].?);
    if (!(interval > 0)) return py.errValue("the sampling interval must be positive");

    py.clear(&st.metrics_cb);
    py.incref(args[1].?);
    st.metrics_cb = args[1];

    const ms: u64 = @intFromFloat(@ceil(interval * 1000.0));
    try py.errUvIfNeg(uv.uv_timer_start(st.sampler, onSampler, ms, ms));
    // The sampler must not be what keeps the loop alive.
    uv.uv_unref(uv.asHandle(st.sampler));
    st.sampler_active = true;
    return py.noneRef();
}

fn stopMetrics(self_obj: *py.Object) py.Error!*py.Object {
    const st = asLoop(self_obj).state();
    if (st.sampler_active) {
        _ = uv.uv_timer_stop(st.sampler);
        st.sampler_active = false;
    }
    py.clear(&st.metrics_cb);
    return py.noneRef();
}

fn timerHandleCancelled(self_obj: *py.Object, _: *py.Object) py.Error!*py.Object {
    _ = self_obj;
    return py.noneRef();
}

fn closeLoop(self_obj: *py.Object) py.Error!*py.Object {
    const self = asLoop(self_obj);
    const st = self.state();
    if (st.running) return py.errRuntime("Cannot close a running event loop");
    if (st.closed) return py.noneRef();
    st.closed = true;

    st.ready.deinit();
    st.timers.deinit();
    drainInbox(st);
    py.clear(&st.fatal);
    py.clear(&st.metrics_cb);
    pollermod.closeAll(st);

    if (st.idle_active) {
        _ = uv.uv_idle_stop(st.idle);
        st.idle_active = false;
    }
    if (st.timer_active) {
        _ = uv.uv_timer_stop(st.timer);
        st.timer_active = false;
    }
    closeAllHandles(st);
    return py.noneRef();
}

fn drainInbox(st: *State) void {
    var node = st.inbox.takeAll();
    while (node) |h| {
        node = h.next;
        h.next = null;
        py.decref(h);
    }
}

fn walkClose(handle: ?*uv.Handle, _: ?*anyopaque) callconv(.c) void {
    const h = handle orelse return;
    if (uv.uv_is_closing(h) == 0) uv.uv_close(h, onCloseFreeState);
}

/// Closes every handle and drains their close callbacks so `uv_loop_close`
/// can succeed. Runs with the GIL held, hence the depth bump in the callbacks.
fn closeAllHandles(st: *State) void {
    uv.uv_walk(st.uvloop, walkClose, null);
    var guard: usize = 0;
    while (uv.uv_run(st.uvloop, .nowait) != 0 and guard < 1000) : (guard += 1) {}
    _ = uv.uv_loop_close(st.uvloop);
}

// ---------------------------------------------------------------------------
// type plumbing

fn newLoop(tp: ?*c.PyTypeObject, _: ?*py.Object, _: ?*py.Object) callconv(.c) ?*py.Object {
    const obj = c.PyType_GenericAlloc(tp, 0) orelse return null;
    const self = asLoop(obj);

    const loop_size = uv.uv_loop_size();
    const idle_size = uv.uv_handle_size(.idle);
    const timer_size = uv.uv_handle_size(.timer);
    const async_size = uv.uv_handle_size(.@"async");
    const words = alloc.alloc(u64, (loop_size + idle_size + 2 * timer_size + async_size + 7) / 8) catch {
        py.decref(obj);
        _ = c.PyErr_NoMemory();
        return null;
    };

    const block: []u8 = @as([*]u8, @ptrCast(words.ptr))[0 .. words.len * 8];
    const st = alloc.create(State) catch {
        alloc.free(words);
        py.decref(obj);
        _ = c.PyErr_NoMemory();
        return null;
    };
    st.* = .{
        .uvloop = @ptrCast(block.ptr),
        .idle = @ptrCast(block.ptr + loop_size),
        .timer = @ptrCast(block.ptr + loop_size + idle_size),
        .sampler = @ptrCast(block.ptr + loop_size + idle_size + timer_size),
        .waker = @ptrCast(block.ptr + loop_size + idle_size + 2 * timer_size),
        .block = block,
    };
    self.st = st;

    if (uv.uv_loop_init(st.uvloop) < 0 or
        uv.uv_idle_init(st.uvloop, st.idle) < 0 or
        uv.uv_timer_init(st.uvloop, st.timer) < 0 or
        uv.uv_timer_init(st.uvloop, st.sampler) < 0 or
        uv.uv_async_init(st.uvloop, st.waker, onWake) < 0)
    {
        py.decref(obj);
        c.PyErr_SetString(@ptrCast(c.PyExc_RuntimeError), "failed to initialise the libuv loop");
        return null;
    }
    _ = uv.uv_loop_configure(st.uvloop, .metrics_idle_time);
    uv.setData(st.idle, obj);
    uv.setData(st.timer, obj);
    uv.setData(st.sampler, obj);
    uv.setData(st.waker, obj);

    return obj;
}

fn dealloc(obj: ?*py.Object) callconv(.c) void {
    const self = asLoop(obj.?);
    const tp = py.typeOf(obj.?);
    c.PyObject_GC_UnTrack(obj);
    if (self.st) |st| {
        if (!st.closed) {
            st.closed = true;
            st.ready.deinit();
            st.timers.deinit();
            drainInbox(st);
            py.clear(&st.fatal);
            py.clear(&st.metrics_cb);
            pollermod.closeAll(st);
            closeAllHandles(st);
        }
        if (st.read_buf) |b| alloc.free(@as([]u8, b[0..transportmod.read_buffer_size]));
        alloc.free(@as([*]u64, @ptrCast(@alignCast(st.block.ptr)))[0 .. st.block.len / 8]);
        alloc.destroy(st);
        self.st = null;
    }
    tp.tp_free.?(obj);
    py.decref(tp);
}

fn traverse(obj: ?*py.Object, visitproc: c.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
    const self = asLoop(obj.?);
    if (self.st) |st| {
        var r = py.visit(st.fatal, visitproc, arg);
        if (r != 0) return r;
        r = py.visit(st.metrics_cb, visitproc, arg);
        if (r != 0) return r;
        var i: usize = 0;
        while (i < st.ready.len) : (i += 1) {
            r = py.visit(st.ready.items[(st.ready.head + i) & (st.ready.items.len - 1)], visitproc, arg);
            if (r != 0) return r;
        }
        i = 0;
        while (i < st.timers.len) : (i += 1) {
            r = py.visit(st.timers.items[i].handle, visitproc, arg);
            if (r != 0) return r;
        }
        r = pollermod.traverse(st, visitproc, arg);
        if (r != 0) return r;
    }
    return py.visit(@ptrCast(py.typeOf(obj.?)), visitproc, arg);
}

fn clear_(obj: ?*py.Object) callconv(.c) c_int {
    const self = asLoop(obj.?);
    if (self.st) |st| {
        st.ready.deinit();
        st.timers.deinit();
        py.clear(&st.fatal);
        py.clear(&st.metrics_cb);
    }
    return 0;
}

var methods = [_]c.PyMethodDef{
    py.methodKw("call_soon", callSoon, "Schedule a callback for the next iteration."),
    py.methodKw("call_soon_threadsafe", callSoonThreadsafe, "Schedule a callback from another thread."),
    py.methodKw("call_later", callLater, "Schedule a callback after a delay in seconds."),
    py.methodKw("call_at", callAt, "Schedule a callback at an absolute loop time."),
    py.methodNoArgs("time", time, "Return the loop's monotonic clock, in seconds."),
    py.methodNoArgs("_run", runLoop, "Run the libuv loop until stopped."),
    py.methodNoArgs("stop", stop, "Stop the loop."),
    py.methodNoArgs("is_running", isRunning, "Return True while the loop is running."),
    py.methodNoArgs("is_closed", isClosed, "Return True once the loop is closed."),
    py.methodNoArgs("get_debug", getDebug, "Return the debug mode flag."),
    py.methodO("set_debug", setDebug, "Set the debug mode flag."),
    py.methodO("_timer_handle_cancelled", timerHandleCancelled, "Compatibility no-op."),
    py.methodNoArgs("_metrics", metrics, "Return a snapshot of loop and libuv counters."),
    py.method("_start_metrics", startMetrics, "Sample loop counters on a native timer."),
    py.methodNoArgs("_stop_metrics", stopMetrics, "Stop sampling loop counters."),
    py.method("add_reader", pollermod.addReader, "Call a callback whenever a descriptor is readable."),
    py.method("add_writer", pollermod.addWriter, "Call a callback whenever a descriptor is writable."),
    py.methodO("remove_reader", pollermod.removeReader, "Stop watching a descriptor for readability."),
    py.methodO("remove_writer", pollermod.removeWriter, "Stop watching a descriptor for writability."),
    py.method("_getaddrinfo", dns.getaddrinfo, "Resolve a host on the libuv threadpool."),
    py.method("_getnameinfo", dns.getnameinfo, "Reverse-resolve an address on the libuv threadpool."),
    py.method("_make_transport", transportmod.makeTransport, "Wrap a connected descriptor in a stream transport."),
    py.methodNoArgs("_close", closeLoop, "Release the libuv loop."),
    py.sentinel,
};

var getsets = [_]c.PyGetSetDef{
    .{
        .name = "slow_callback_duration",
        .get = getSlowCallbackDuration,
        .set = setSlowCallbackDuration,
        .doc = "Seconds a callback may run before it is reported as slow.",
        .closure = null,
    },
    .{ .name = null, .get = null, .set = null, .doc = null, .closure = null },
};

var slots = [_]c.PyType_Slot{
    .{ .slot = c.Py_tp_new, .pfunc = @constCast(@ptrCast(&newLoop)) },
    .{ .slot = c.Py_tp_dealloc, .pfunc = @constCast(@ptrCast(&dealloc)) },
    .{ .slot = c.Py_tp_traverse, .pfunc = @constCast(@ptrCast(&traverse)) },
    .{ .slot = c.Py_tp_clear, .pfunc = @constCast(@ptrCast(&clear_)) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_getset, .pfunc = @ptrCast(&getsets) },
    .{ .slot = c.Py_tp_doc, .pfunc = @constCast(@ptrCast("libuv-backed event loop core.")) },
    .{ .slot = 0, .pfunc = null },
};

var spec = c.PyType_Spec{
    .name = "zuv._zuv.Loop",
    .basicsize = @sizeOf(LoopObject),
    .itemsize = 0,
    .flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_BASETYPE,
    .slots = &slots,
};

pub fn register(module: *py.Object) py.Error!void {
    str_call_exception_handler = py.intern("call_exception_handler") orelse return py.Error.Python;
    str_message = py.intern("message") orelse return py.Error.Python;
    str_exception = py.intern("exception") orelse return py.Error.Python;
    str_handle = py.intern("handle") orelse return py.Error.Python;
    str_context = py.intern("context") orelse return py.Error.Python;
    str_on_slow_callback = py.intern("_on_slow_callback") orelse return py.Error.Python;

    try dns.register();
    try transportmod.register(module);

    loop_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &spec, null) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "Loop", @ptrCast(loop_type)) < 0) return py.Error.Python;
}
