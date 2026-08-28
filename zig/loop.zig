//! The native event loop: scheduling, timers, and the libuv run loop.
//!
//! `_run` detaches the thread state for the whole `uv_run` call. Each libuv
//! callback reattaches it and enters the extension critical section. The depth
//! starts at 1 because object methods enter with an attached thread state, so
//! nested runs such as `close` skip the detach/attach pair.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const uv = @import("uv.zig");
const collections = @import("collections.zig");
const handlemod = @import("handle.zig");
const tshandle = @import("tshandle.zig");
const timermod = @import("timer.zig");
const pollermod = @import("poller.zig");
const dns = @import("dns.zig");
const transportmod = @import("transport.zig");
const datagrammod = @import("datagram.zig");
const processmod = @import("process.zig");
const Handle = handlemod.Handle;

const alloc = std.heap.c_allocator;

/// Timers below this many cancelled entries are never compacted.
const min_cancelled_timers = 100;
const context_pool_capacity = 16;
const ready_drain_batch_limit = 64;
const ready_drain_io_batch_limit = 8;
const ready_drain_callback_limit = 16 * 1024;

pub const State = struct {
    uvloop: *uv.Loop,
    idle: *uv.Idle,
    timer: *uv.Timer,
    sampler: *uv.Timer,
    waker: *uv.Async,
    flusher: *uv.Prepare,
    block: []u8,

    ready: collections.Ready = .empty,
    timers: collections.Timers = .empty,
    pollers: pollermod.Map = .empty,
    scratch: ?[*]u8 = null,
    empty_contexts: [context_pool_capacity]?*py.Object = .{null} ** context_pool_capacity,
    empty_contexts_len: usize = 0,
    threadsafe_candidate: ?*py.Object = null,
    dns_requests: ?*anyopaque = null,
    transport_head: ?*transportmod.Transport = null,
    flush_head: ?*transportmod.Transport = null,

    tstate: ?*c.PyThreadState = null,
    python_depth: c_int = 1,
    critical_section: py.CriticalSection,
    critical_section_active: bool = false,
    fatal: ?*py.Object = null,
    metrics_cb: ?*py.Object = null,
    task_factory: ?*py.Object = null,

    running: bool = false,
    closed: bool = false,
    stopping: bool = false,
    debug: bool = false,
    slow_callback_monitoring: bool = false,
    idle_active: bool = false,
    waker_pending: bool = false,
    timer_active: bool = false,
    sampler_active: bool = false,
    flusher_active: bool = false,

    /// 0 = synchronous ownership, 1 = reaper running with a live LoopObject,
    /// 2 = reaper running after LoopObject deallocation, 3 = reaper finished.
    reap_state: u8 = 0,

    thread_id: c_ulong = 0,
    slow_callback_duration: f64 = 0.1,
    callbacks_run: u64 = 0,
    iterations: u64 = 0,

    pub inline fn pythonEnter(self: *State) void {
        if (self.python_depth == 0) {
            c.PyEval_RestoreThread(self.tstate);
            self.tstate = null;
            py.beginCriticalSection(&self.critical_section);
            self.critical_section_active = true;
        }
        self.python_depth += 1;
    }

    pub inline fn pythonExit(self: *State) void {
        self.python_depth -= 1;
        if (self.python_depth == 0) {
            if (self.critical_section_active) {
                py.endCriticalSection(&self.critical_section);
                self.critical_section_active = false;
            }
            self.tstate = c.PyEval_SaveThread();
        }
    }

    pub inline fn pythonResume(self: *State) void {
        std.debug.assert(self.python_depth == 0 and !self.critical_section_active);
        c.PyEval_RestoreThread(self.tstate);
        self.tstate = null;
        self.python_depth = 1;
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

const ReadyActivity = struct {
    st: *State,
    external_handles: usize = 0,
    closing: bool = false,
};

var str_call_exception_handler: ?*py.Object = null;
var str_coro: ?*py.Object = null;
var str_message: ?*py.Object = null;
var str_exception: ?*py.Object = null;
var str_handle: ?*py.Object = null;
var str_context: ?*py.Object = null;
var str_loop: ?*py.Object = null;
var str_name: ?*py.Object = null;
var str_on_slow_callback: ?*py.Object = null;
var task_type: ?*py.Object = null;

pub inline fn asLoop(obj: *py.Object) *LoopObject {
    return @ptrCast(@alignCast(obj));
}

pub inline fn isReaping(st: *State) bool {
    return @atomicLoad(u8, &st.reap_state, .acquire) != 0;
}

/// Seconds on the clock `time.monotonic()` reads.
///
/// Being monotonic is not enough: asyncio code mixes `loop.time()` with
/// `time.monotonic()` freely - aiohttp dates its pooled connections that way -
/// so the two have to be the same clock, not merely two well-behaved ones.
/// `uv_hrtime()` is not: on macOS it counts time spent asleep, which
/// `time.monotonic()` does not, and the two drift apart by the machine's uptime.
pub inline fn now() f64 {
    var ns: c.PyTime_t = 0;
    _ = c.PyTime_MonotonicRaw(&ns);
    return @as(f64, @floatFromInt(ns)) / 1e9;
}

/// Converts a positive duration to libuv's millisecond clock without letting
/// an infinite or oversized Python float reach a trapping float-to-int cast.
fn timerMilliseconds(seconds: f64, guard_ms: f64) u64 {
    if (!(seconds > 0)) return 0;
    const rounded = @ceil(seconds * 1000.0) + guard_ms;
    const limit: f64 = @floatFromInt(std.math.maxInt(u64));
    if (!std.math.isFinite(rounded) or rounded >= limit) return std.math.maxInt(u64);
    return @intFromFloat(rounded);
}

/// Cancelled means dropped, and being dropped from the heap is what ends being
/// scheduled - so the flag is cleared on the way out, as it is everywhere else
/// a timer leaves.
fn retireIfCancelled(obj: *py.Object) bool {
    if (!timermod.isCancelled(obj)) return false;
    timermod.clearScheduled(obj);
    return true;
}

// ---------------------------------------------------------------------------
// libuv callbacks

fn onIdle(idle: ?*uv.Idle) callconv(.c) void {
    const self: *LoopObject = @ptrCast(@alignCast(uv.getData(idle.?)));
    const st = self.state();
    st.pythonEnter();
    defer st.pythonExit();
    var batches: usize = 0;
    var callbacks: usize = 0;
    var batch_limit: usize = ready_drain_batch_limit;
    while (st.ready.len != 0) {
        const batch = st.ready.len;
        if (batches != 0 and
            (batches >= batch_limit or
                callbacks >= ready_drain_callback_limit or
                batch > ready_drain_callback_limit - callbacks or
                (batch_limit == ready_drain_io_batch_limit and batch != 1))) break;
        runReady(self);
        batches += 1;
        callbacks += batch;
        if (st.stopping or st.fatal != null or st.ready.len == 0) break;
        if (st.timer_active or st.dns_requests != null or st.flush_head != null) break;
        if (batch_limit == ready_drain_batch_limit) {
            var activity = ReadyActivity{ .st = st };
            uv.uv_walk(st.uvloop, countExternalHandles, &activity);
            // The self-pipe poller is the one external handle every loop owns.
            if (activity.closing) break;
            if (activity.external_handles > 1) {
                if (batch != 1) break;
                batch_limit = ready_drain_io_batch_limit;
            }
        }
    }
    if (st.ready.len == 0 and st.idle_active) {
        _ = uv.uv_idle_stop(st.idle);
        st.idle_active = false;
    }
}

fn countExternalHandles(handle: ?*uv.Handle, arg: ?*anyopaque) callconv(.c) void {
    const activity: *ReadyActivity = @ptrCast(@alignCast(arg.?));
    const h = handle.?;
    const internal = [_]*uv.Handle{
        @ptrCast(activity.st.idle),
        @ptrCast(activity.st.timer),
        @ptrCast(activity.st.sampler),
        @ptrCast(activity.st.waker),
        @ptrCast(activity.st.flusher),
    };
    for (internal) |owned| {
        if (h == owned) return;
    }
    activity.external_handles += 1;
    if (uv.uv_is_closing(h) != 0) activity.closing = true;
}

fn onTimer(timer: ?*uv.Timer) callconv(.c) void {
    const self: *LoopObject = @ptrCast(@alignCast(uv.getData(timer.?)));
    const st = self.state();
    st.pythonEnter();
    defer st.pythonExit();
    st.timer_active = false;
    collectDueTimers(self);
    armTimer(self);
}

fn onWake(waker: ?*uv.Async) callconv(.c) void {
    const self: *LoopObject = @ptrCast(@alignCast(uv.getData(waker.?)));
    const st = self.state();
    st.pythonEnter();
    defer st.pythonExit();
    st.waker_pending = false;
    startIdle(st);
}

/// Sends everything written since the last turn, as one vectored write per
/// transport.
///
/// libuv computes the poll timeout after running prepare handles and before
/// running check handles, so this has to be a prepare: from a check, a loop with
/// nothing else to do would block for I/O while still holding data the peer is
/// waiting for. Running here, every accepted write reaches the socket before the
/// loop can sleep, whether it came from a task or from a read callback.
fn onFlush(prepare: ?*uv.Prepare) callconv(.c) void {
    const self: *LoopObject = @ptrCast(@alignCast(uv.getData(prepare.?)));
    const st = self.state();
    st.pythonEnter();
    defer st.pythonExit();
    drainFlushList(st);
    // Flushing runs protocol code - `pause_writing` above all - which is free to
    // write again, so the list can refill while it is being drained. Stopping
    // then would strand those writes with nothing left to send them.
    if (st.flush_head == null) {
        _ = uv.uv_prepare_stop(st.flusher);
        st.flusher_active = false;
    }
}

/// Releases pending writes without sending them. A closing loop cannot deliver
/// them, and the transports holding them are about to be closed.
fn dropFlushList(st: *State) void {
    var node = st.flush_head;
    st.flush_head = null;
    while (node) |transport| {
        node = transport.flush_next;
        transport.flush_next = null;
        transport.flags &= ~transportmod.FLUSH_QUEUED;
        transportmod.discardPending(transport);
        py.decref(transport);
    }
}

fn drainFlushList(st: *State) void {
    var node = st.flush_head;
    st.flush_head = null;
    while (node) |transport| {
        node = transport.flush_next;
        transport.flush_next = null;
        transport.flags &= ~transportmod.FLUSH_QUEUED;
        transportmod.flushPending(transport);
        py.decref(transport);
    }
}

/// Registers a transport as holding unsent writes. The reference keeps it alive
/// until the flush runs, so a protocol that writes and drops the transport in
/// the same turn still gets its data out.
pub fn scheduleFlush(st: *State, transport: *transportmod.Transport) void {
    if (st.closed) {
        // Releasing an earlier batch during close can run arbitrary Python
        // code. A write from that code has nowhere to go and must not recreate
        // the list that close just detached.
        transportmod.discardPending(transport);
        return;
    }
    if (transport.flags & transportmod.FLUSH_QUEUED != 0) return;
    transport.flags |= transportmod.FLUSH_QUEUED;
    py.incref(transport);
    transport.flush_next = st.flush_head;
    st.flush_head = transport;
    if (!st.flusher_active) {
        _ = uv.uv_prepare_start(st.flusher, onFlush);
        uv.uv_unref(uv.asHandle(st.flusher));
        st.flusher_active = true;
    }
}

fn onCloseFreeState(handle: ?*uv.Handle) callconv(.c) void {
    _ = handle;
}

// ---------------------------------------------------------------------------
// scheduling internals

/// Landing buffer for reads small enough that copying beats allocating a
/// right-sized object up front. Grown once, reused for the loop's lifetime.
pub fn scratchBuffer(st: *State) ?[*]u8 {
    if (st.scratch) |b| return b;
    const buf = alloc.alloc(u8, transportmod.copy_threshold) catch return null;
    st.scratch = buf.ptr;
    return buf.ptr;
}

pub inline fn startIdle(st: *State) void {
    if (!st.idle_active and st.ready.len != 0) {
        _ = uv.uv_idle_start(st.idle, onIdle);
        st.idle_active = true;
    }
}

pub fn takeEmptyContext(loop_obj: *py.Object) ?*py.Object {
    const st = asLoop(loop_obj).state();
    if (st.empty_contexts_len == 0) return null;
    st.empty_contexts_len -= 1;
    const context = st.empty_contexts[st.empty_contexts_len];
    st.empty_contexts[st.empty_contexts_len] = null;
    return context;
}

pub fn recycleEmptyContext(loop_obj: *py.Object, context: *py.Object) bool {
    const st = asLoop(loop_obj).state();
    if (st.closed or st.empty_contexts_len == context_pool_capacity) return false;
    st.empty_contexts[st.empty_contexts_len] = context;
    st.empty_contexts_len += 1;
    return true;
}

fn clearEmptyContexts(st: *State) void {
    while (st.empty_contexts_len > 0) {
        st.empty_contexts_len -= 1;
        py.clear(&st.empty_contexts[st.empty_contexts_len]);
    }
}

fn clearThreadsafeCandidate(st: *State, untrack_queue_only: bool) void {
    const candidate = st.threadsafe_candidate orelse return;
    st.threadsafe_candidate = null;
    if (untrack_queue_only) tshandle.untrackIfQueueOnly(candidate);
    py.decref(candidate);
}

/// The ready queue holds both handle types; only the type says which protocol
/// a callback runs under.
inline fn runOne(obj: *py.Object) void {
    if (timermod.owns(obj)) {
        timermod.run(obj);
    } else if (tshandle.owns(obj)) {
        tshandle.run(obj);
    } else {
        handlemod.run(@ptrCast(@alignCast(obj)));
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
        if (st.threadsafe_candidate == obj) clearThreadsafeCandidate(st, true);
        if ((st.debug or st.slow_callback_monitoring) and st.slow_callback_duration < std.math.inf(f64)) {
            const started = uv.uv_hrtime();
            runOne(obj);
            const elapsed = @as(f64, @floatFromInt(uv.uv_hrtime() - started)) / 1e9;
            if (elapsed > st.slow_callback_duration) reportSlowCallback(self, obj, elapsed);
        } else {
            runOne(obj);
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
        timermod.clearScheduled(entry.handle);
        if (timermod.isCancelled(entry.handle)) {
            py.decref(entry.handle);
            if (st.timers.cancelled != 0) st.timers.cancelled -= 1;
            continue;
        }
        st.ready.push(entry.handle) catch {
            py.errNoMemory() catch {};
            captureFatal(self);
            py.decref(entry.handle);
            return;
        };
    }
    startIdle(st);
}

/// Points the single libuv timer at the earliest live deadline.
fn armTimer(self: *LoopObject) void {
    const st = self.state();
    while (st.timers.peek()) |entry| {
        if (!timermod.isCancelled(entry.handle)) break;
        _ = st.timers.pop();
        timermod.clearScheduled(entry.handle);
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
    const ms = timerMilliseconds(delta, 1);
    _ = uv.uv_timer_start(st.timer, onTimer, ms, 0);
    st.timer_active = true;
}

pub fn noteTimerCancelled(self: *LoopObject) void {
    const st = self.st orelse return;
    st.timers.cancelled += 1;
    if (st.timers.cancelled > min_cancelled_timers and st.timers.cancelled * 2 > st.timers.len) {
        st.timers.compact(retireIfCancelled);
        armTimer(self);
    }
}

pub fn captureFatal(self: *LoopObject) void {
    const st = self.state();
    const exc = c.PyErr_GetRaisedException();
    if (st.fatal == null) st.fatal = exc else py.xdecref(exc);
    st.stopping = true;
    uv.uv_stop(st.uvloop);
}

/// Routes a failed callback to `call_exception_handler`, except for the two
/// exceptions asyncio propagates out of `run_forever`.
pub fn callbackFailed(loop_obj_opt: ?*py.Object, h: *py.Object) void {
    const loop_obj = loop_obj_opt orelse {
        c.PyErr_Clear();
        return;
    };
    const self = asLoop(loop_obj);
    if (c.PyErr_ExceptionMatches(py.exc_system_exit) != 0 or
        c.PyErr_ExceptionMatches(py.exc_keyboard_interrupt) != 0)
    {
        captureFatal(self);
        return;
    }
    const exc = c.PyErr_GetRaisedException() orelse return;
    defer py.decref(exc);
    callExceptionHandler(self, "Exception in callback", exc, h);
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
    clearThreadsafeCandidate(st, true);
    const h = try tshandle.create(self_obj, args[0].?, p.positional, p.context);
    py.incref(h);
    // One FIFO for both schedulers, so a `call_soon` issued after a
    // `call_soon_threadsafe` still runs after it. Producers and the loop touch
    // `ready` only inside the extension critical section.
    st.ready.push(h) catch {
        py.decref(h);
        py.decref(h);
        return py.errNoMemory();
    };
    // Keep the return value tracked until a later call proves only the queue retained it.
    py.incref(h);
    st.threadsafe_candidate = h;
    // libuv clears its pending bit before `onWake` enters Python. Keep one at
    // this layer so a producer cannot issue another kernel wake in that window.
    if (!st.idle_active and !st.waker_pending) {
        st.waker_pending = true;
        _ = uv.uv_async_send(st.waker);
    }
    return h;
}

fn scheduleAt(
    self: *LoopObject,
    when: f64,
    original_when: ?*py.Object,
    callback: *py.Object,
    p: Parsed,
) py.Error!*py.Object {
    const st = self.state();
    try checkClosed(st);
    const h = try timermod.create(@ptrCast(self), callback, p.positional, p.context, when, original_when);
    py.incref(h);
    st.timers.push(when, h) catch {
        py.decref(h);
        py.decref(h);
        return py.errNoMemory();
    };
    armTimer(self);
    return h;
}

fn callLater(self_obj: *py.Object, args: []const ?*py.Object, nargs: usize, kwnames: ?*py.Object) py.Error!*py.Object {
    if (nargs < 2) return py.errType("call_later() requires a delay and a callback");
    const delay = try py.asF64(args[0].?);
    const p = try parseCall(args, nargs, kwnames, 2);
    const when = now() + @max(delay, 0);
    return scheduleAt(asLoop(self_obj), when, null, args[1].?, p);
}

fn callAt(self_obj: *py.Object, args: []const ?*py.Object, nargs: usize, kwnames: ?*py.Object) py.Error!*py.Object {
    if (nargs < 2) return py.errType("call_at() requires a time and a callback");
    const when = try py.asF64(args[0].?);
    const p = try parseCall(args, nargs, kwnames, 2);
    return scheduleAt(asLoop(self_obj), when, args[0], args[1].?, p);
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

    st.pythonExit();
    _ = uv.uv_run(st.uvloop, .default);
    st.pythonResume();

    st.running = false;
    st.stopping = false;
    st.thread_id = 0;
    // A write from the callback that stopped the loop has had no iteration left
    // to flush it. Draining after clearing `running` sends anything those
    // callbacks write in turn.
    drainFlushList(st);

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

fn createTask(
    self_obj: *py.Object,
    args: []const ?*py.Object,
    nargs: usize,
    kwnames: ?*py.Object,
) py.Error!*py.Object {
    if (nargs > 1) return py.errType("create_task() takes exactly one positional argument");
    const self = asLoop(self_obj);
    const st = self.state();
    try checkClosed(st);
    const keyword_count: usize = if (kwnames) |names| @intCast(c.PyTuple_Size(names)) else 0;
    if (st.task_factory == null and st.running and nargs == 1 and keyword_count == 0) {
        return py.call1(task_type.?, args[0].?) orelse py.Error.Python;
    }

    var coro: ?*py.Object = if (nargs == 1) args[0] else null;
    const kwargs = c.PyDict_New() orelse return py.Error.Python;
    defer py.decref(kwargs);
    if (kwnames) |names| {
        for (0..keyword_count) |i| {
            const name = c.PyTuple_GetItem(names, @intCast(i)) orelse return py.Error.Python;
            const is_coro = c.PyObject_RichCompareBool(name, str_coro.?, c.Py_EQ);
            if (is_coro < 0) return py.Error.Python;
            if (is_coro != 0) {
                if (coro != null) return py.errType("create_task() got multiple values for argument 'coro'");
                coro = args[nargs + i];
            } else if (c.PyDict_SetItem(kwargs, name, args[nargs + i].?) < 0) {
                return py.Error.Python;
            }
        }
    }
    const coroutine = coro orelse return py.errType("create_task() missing required argument 'coro'");

    const factory = st.task_factory;
    const positional_count: usize = if (factory == null) 1 else 2;
    const positional = c.PyTuple_New(@intCast(positional_count)) orelse return py.Error.Python;
    defer py.decref(positional);
    if (factory) |_| {
        py.incref(self_obj);
        _ = c.PyTuple_SetItem(positional, 0, self_obj);
        py.incref(coroutine);
        _ = c.PyTuple_SetItem(positional, 1, coroutine);
    } else {
        py.incref(coroutine);
        _ = c.PyTuple_SetItem(positional, 0, coroutine);
    }
    const defaults = .{ str_name.?, str_context.? };
    inline for (defaults) |name| {
        const present = c.PyDict_Contains(kwargs, name);
        if (present < 0) return py.Error.Python;
        if (present == 0 and c.PyDict_SetItem(kwargs, name, py.none()) < 0) return py.Error.Python;
    }
    if (factory == null) {
        const has_loop = c.PyDict_Contains(kwargs, str_loop.?);
        if (has_loop < 0) return py.Error.Python;
        if (has_loop != 0) return py.errType("create_task() got multiple values for keyword argument 'loop'");
        if (c.PyDict_SetItem(kwargs, str_loop.?, self_obj) < 0) return py.Error.Python;
    }
    return c.PyObject_Call(factory orelse task_type.?, positional, kwargs) orelse py.Error.Python;
}

fn setTaskFactory(self_obj: *py.Object, value: *py.Object) py.Error!*py.Object {
    const st = asLoop(self_obj).state();
    const replacement: ?*py.Object = if (py.isNone(value)) null else value;
    if (replacement) |factory| py.incref(factory);
    py.clear(&st.task_factory);
    st.task_factory = replacement;
    return py.noneRef();
}

fn getTaskFactory(self_obj: *py.Object) py.Error!*py.Object {
    return py.newref(asLoop(self_obj).state().task_factory) orelse py.noneRef();
}

fn setSlowCallbackMonitoring(self_obj: *py.Object, value: *py.Object) py.Error!*py.Object {
    asLoop(self_obj).state().slow_callback_monitoring = try py.isTrue(value);
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
    st.pythonEnter();
    defer st.pythonExit();
    const callback = st.metrics_cb orelse return;
    const snapshot = buildMetrics(st) orelse {
        py.writeUnraisable(@ptrCast(self));
        return;
    };
    defer py.decref(snapshot);
    const result = c.PyObject_CallOneArg(callback, snapshot);
    if (result) |r| {
        py.decref(r);
        return;
    }
    const exc = c.PyErr_GetRaisedException() orelse return;
    defer py.decref(exc);
    callExceptionHandler(self, "Exception in the metrics sampler", exc, null);
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

    const ms = timerMilliseconds(interval, 0);
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

    clearThreadsafeCandidate(st, false);
    st.ready.deinit();
    st.timers.deinit();
    clearEmptyContexts(st);
    dropFlushList(st);
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
    if (st.flusher_active) {
        _ = uv.uv_prepare_stop(st.flusher);
        st.flusher_active = false;
    }
    closeAllHandles(st);
    return py.noneRef();
}

fn walkClose(handle: ?*uv.Handle, _: ?*anyopaque) callconv(.c) void {
    const h = handle orelse return;
    if (uv.uv_is_closing(h) != 0) return;
    switch (uv.uv_handle_get_type(h)) {
        .tcp, .named_pipe => transportmod.closeFromLoop(h),
        .udp => datagrammod.closeFromLoop(h),
        .process => processmod.closeFromLoop(h),
        else => uv.uv_close(h, onCloseFreeState),
    }
}

fn countHandle(handle: ?*uv.Handle, arg: ?*anyopaque) callconv(.c) void {
    _ = handle;
    const count: *usize = @ptrCast(@alignCast(arg.?));
    count.* += 1;
}

fn hasHandles(st: *State) bool {
    var count: usize = 0;
    uv.uv_walk(st.uvloop, countHandle, &count);
    return count != 0;
}

fn freeStateStorage(st: *State) void {
    if (st.scratch) |scratch| {
        const scratch_slice: []u8 = scratch[0..transportmod.copy_threshold];
        alloc.free(scratch_slice);
    }
    alloc.free(@as([*]u64, @ptrCast(@alignCast(st.block.ptr)))[0 .. st.block.len / 8]);
    alloc.destroy(st);
}

fn releaseStateFromLoopObject(st: *State) void {
    while (true) switch (@atomicLoad(u8, &st.reap_state, .acquire)) {
        0, 3 => {
            freeStateStorage(st);
            return;
        },
        1 => {
            // Transfer final freeing to the reaper. If it just finished, retry
            // and consume state 3 ourselves.
            if (@cmpxchgStrong(u8, &st.reap_state, 1, 2, .acq_rel, .acquire) == null) return;
        },
        else => @panic("invalid DNS reaper ownership state"),
    };
}

fn reapLoop(st: *State) void {
    _ = uv.uv_run(st.uvloop, .default);
    if (uv.uv_loop_close(st.uvloop) != 0) @panic("libuv loop still owns requests after DNS reaping");

    // This must be the reaper's final access when the LoopObject still exists:
    // deallocation may observe state 3 and immediately free the allocation.
    if (@cmpxchgStrong(u8, &st.reap_state, 1, 3, .acq_rel, .acquire)) |current| {
        if (current != 2) @panic("invalid DNS reaper ownership state");
        freeStateStorage(st);
    }
}

fn startDnsReaper(st: *State) bool {
    dns.releaseFutures(st);
    @atomicStore(u8, &st.reap_state, 1, .release);
    const thread = std.Thread.spawn(.{}, reapLoop, .{st}) catch {
        @atomicStore(u8, &st.reap_state, 0, .release);
        return false;
    };
    thread.detach();
    return true;
}

/// Closes every handle and drains Python-facing callbacks with Python attached.
/// Resolver work that libuv cannot cancel is handed to a native-only reaper so
/// a slow system resolver cannot hold up EventLoop.close().
fn closeAllHandles(st: *State) void {
    dns.cancelAll(st);
    uv.uv_walk(st.uvloop, walkClose, null);
    while (hasHandles(st)) _ = uv.uv_run(st.uvloop, .once);
    if (uv.uv_loop_close(st.uvloop) == 0) return;

    if (st.dns_requests != null and startDnsReaper(st)) return;

    // Thread creation can fail under extreme resource pressure. Synchronous
    // draining is the only safe fallback because libuv still owns each request.
    _ = uv.uv_run(st.uvloop, .default);
    if (uv.uv_loop_close(st.uvloop) != 0) @panic("libuv loop still owns resources after shutdown");
}

/// Runs on CPython's main interpreter thread. Signal-owning loops cannot be
/// closed by a finalizer on another thread because Python only permits signal
/// handlers to be changed from the main thread.
fn closeFromPending(arg: ?*anyopaque) callconv(.c) c_int {
    const callback: *py.Object = @ptrCast(@alignCast(arg.?));
    const result = c.PyObject_CallNoArgs(callback);
    if (result) |value| {
        py.decref(value);
    } else {
        py.writeUnraisable(callback);
    }
    py.decref(callback);
    return 0;
}

fn deferClose(_: *py.Object, callback: *py.Object) py.Error!*py.Object {
    // The callback independently owns the signal numbers and preserved wakeup
    // descriptor until closeFromPending restores process-global signal state.
    py.incref(callback);
    if (c.Py_AddPendingCall(closeFromPending, callback) != 0) {
        py.decref(callback);
        return py.errRuntime("failed to defer event loop close to the main thread");
    }
    return py.noneRef();
}

// ---------------------------------------------------------------------------
// type plumbing

fn newLoop(tp: ?*c.PyTypeObject, _: ?*py.Object, _: ?*py.Object) callconv(.c) ?*py.Object {
    const obj = c.PyType_GenericAlloc(tp, 0) orelse return null;
    const self = asLoop(obj);

    const loop_size = uv.uv_loop_size();
    const idle_size = uv.uv_handle_size(.idle);
    const timer_size = uv.uv_handle_size(.timer);
    const async_size = uv.uv_handle_size(.async);
    const prepare_size = uv.uv_handle_size(.prepare);
    const words = alloc.alloc(u64, (loop_size + idle_size + 2 * timer_size + async_size + prepare_size + 7) / 8) catch {
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
        .flusher = @ptrCast(block.ptr + loop_size + idle_size + 2 * timer_size + async_size),
        .block = block,
        // SAFETY: PyCriticalSection_BeginMutex initializes this storage before reading it.
        .critical_section = undefined,
    };

    if (uv.uv_loop_init(st.uvloop) < 0) {
        alloc.free(words);
        alloc.destroy(st);
        py.decref(obj);
        c.PyErr_SetString(py.exc_runtime_error, "failed to initialise the libuv loop");
        return null;
    }
    // Only publish State after the loop itself is valid: dealloc may safely
    // walk and close a partially initialized set of handles from here on.
    self.st = st;

    if (uv.uv_idle_init(st.uvloop, st.idle) < 0 or
        uv.uv_timer_init(st.uvloop, st.timer) < 0 or
        uv.uv_timer_init(st.uvloop, st.sampler) < 0 or
        uv.uv_async_init(st.uvloop, st.waker, onWake) < 0 or
        uv.uv_prepare_init(st.uvloop, st.flusher) < 0)
    {
        py.decref(obj);
        c.PyErr_SetString(py.exc_runtime_error, "failed to initialise the libuv loop");
        return null;
    }
    _ = uv.uv_loop_configure(st.uvloop, .metrics_idle_time);
    uv.setData(st.idle, obj);
    uv.setData(st.timer, obj);
    uv.setData(st.sampler, obj);
    uv.setData(st.waker, obj);
    uv.setData(st.flusher, obj);

    return obj;
}

fn dealloc(obj: ?*py.Object) callconv(.c) void {
    const self = asLoop(obj.?);
    const tp = py.typeOf(obj.?);
    c.PyObject_GC_UnTrack(obj);
    if (self.st) |st| {
        const needs_close = !st.closed;
        st.closed = true;
        if (needs_close) {
            clearThreadsafeCandidate(st, false);
            st.ready.deinit();
            st.timers.deinit();
            clearEmptyContexts(st);
        }
        // The flush list owns one Python reference per transport regardless of
        // how the loop reached deallocation, including partially completed
        // explicit close paths.
        dropFlushList(st);
        py.clear(&st.task_factory);
        if (needs_close) {
            py.clear(&st.fatal);
            py.clear(&st.metrics_cb);
            pollermod.closeAll(st);
            closeAllHandles(st);
        }
        self.st = null;
        releaseStateFromLoopObject(st);
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
        r = py.visit(st.task_factory, visitproc, arg);
        if (r != 0) return r;
        r = py.visit(st.threadsafe_candidate, visitproc, arg);
        if (r != 0) return r;
        var i: usize = 0;
        while (i < st.ready.len) : (i += 1) {
            const handle = st.ready.items[(st.ready.head + i) & (st.ready.items.len - 1)].?;
            r = if (tshandle.owns(handle))
                tshandle.traverseQueued(handle, visitproc, arg)
            else
                py.visit(handle, visitproc, arg);
            if (r != 0) return r;
        }
        i = 0;
        while (i < st.timers.len) : (i += 1) {
            r = py.visit(st.timers.items[i].handle, visitproc, arg);
            if (r != 0) return r;
        }
        var transport = st.flush_head;
        while (transport) |pending| {
            r = py.visit(@ptrCast(pending), visitproc, arg);
            if (r != 0) return r;
            transport = pending.flush_next;
        }
        transport = st.transport_head;
        while (transport) |owned| {
            r = py.visit(@ptrCast(owned), visitproc, arg);
            if (r != 0) return r;
            transport = owned.owner_next;
        }
        // Once close hands resolver requests to the native reaper, their
        // futures are gone and the request list mutates without Python attached.
        if (!isReaping(st)) {
            r = dns.traverse(st, visitproc, arg);
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
        py.clear(&st.task_factory);
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
    py.methodKw("create_task", createTask, "Schedule a coroutine as a task."),
    py.methodO("set_task_factory", setTaskFactory, "Set the task factory."),
    py.methodNoArgs("get_task_factory", getTaskFactory, "Return the task factory."),
    py.methodO("_set_slow_callback_monitoring", setSlowCallbackMonitoring, "Enable slow callback monitoring."),
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
    py.method("_make_datagram_transport", datagrammod.makeDatagram, "Adopt a bound datagram socket."),
    py.method("_spawn_process", processmod.spawnProcess, "Spawn a child process."),
    py.methodO("_defer_close", deferClose, "Defer loop closure to the main interpreter thread."),
    py.methodNoArgs("_close", closeLoop, "Release the libuv loop."),
    py.sentinel,
};

var getsets = [_]c.PyGetSetDef{
    .{
        .name = "slow_callback_duration",
        .get = py.wrapGet(getSlowCallbackDuration),
        .set = py.wrapSet(setSlowCallbackDuration),
        .doc = "Seconds a callback may run before it is reported as slow.",
        .closure = null,
    },
    .{ .name = null, .get = null, .set = null, .doc = null, .closure = null },
};

var slots = [_]c.PyType_Slot{
    .{ .slot = c.Py_tp_new, .pfunc = @ptrCast(@constCast(&newLoop)) },
    .{ .slot = c.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&py.wrapDealloc(dealloc))) },
    .{ .slot = c.Py_tp_traverse, .pfunc = @ptrCast(@constCast(&py.wrapTraverse(traverse))) },
    .{ .slot = c.Py_tp_clear, .pfunc = @ptrCast(@constCast(&py.wrapClear(clear_))) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_getset, .pfunc = @ptrCast(&getsets) },
    .{ .slot = c.Py_tp_doc, .pfunc = @ptrCast(@constCast("libuv-backed event loop core.")) },
    .{ .slot = 0, .pfunc = null },
};

var spec = c.PyType_Spec{
    .name = "zuvloop._zuvloop.Loop",
    .basicsize = @sizeOf(LoopObject),
    .itemsize = 0,
    .flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_BASETYPE,
    .slots = &slots,
};

pub fn register(module: *py.Object) py.Error!void {
    str_call_exception_handler = py.intern("call_exception_handler") orelse return py.Error.Python;
    str_coro = py.intern("coro") orelse return py.Error.Python;
    str_message = py.intern("message") orelse return py.Error.Python;
    str_exception = py.intern("exception") orelse return py.Error.Python;
    str_handle = py.intern("handle") orelse return py.Error.Python;
    str_context = py.intern("context") orelse return py.Error.Python;
    str_loop = py.intern("loop") orelse return py.Error.Python;
    str_name = py.intern("name") orelse return py.Error.Python;
    str_on_slow_callback = py.intern("_on_slow_callback") orelse return py.Error.Python;
    task_type = py.importFrom("asyncio", "Task") orelse return py.Error.Python;

    try dns.register();
    try transportmod.register(module);

    loop_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &spec, null) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "Loop", @ptrCast(loop_type)) < 0) return py.Error.Python;
}
