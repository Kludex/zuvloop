//! `ThreadSafeHandle`: the record `call_soon_threadsafe` returns.
//!
//! A native subtype of `asyncio.events._ThreadSafeHandle`, so the 3.14 contract
//! holds with none of its costs: `isinstance` agrees, and `cancel()` or
//! `cancelled()` from a foreign thread still blocks while the callback runs,
//! but through an atomic state machine and a futex instead of a per-handle
//! `threading.RLock`. The base class's slots hold the native payload and are
//! shadowed by read-only descriptors.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const contextmod = @import("context.zig");
const handlemod = @import("handle.zig");
const loopmod = @import("loop.zig");

/// The word `run` and a foreign `cancel` race for.
const RunState = enum(u32) {
    pending,
    running,
    /// Ran, or cancelled before it could run; either way it never will again.
    done,
};

const Flags = packed struct(u32) {
    cancelled: bool = false,
    /// The context was captured implicitly rather than supplied by the caller.
    captured: bool = false,
    _: u30 = 0,
};

/// One blocked `cancel` or `cancelled` call, parked on a lock it acquired
/// twice; the loop thread releases it when the callback finishes. Lives on the
/// blocked thread's stack, which the block itself keeps alive. The list is
/// only touched inside the handle's critical section, which serialises linking
/// against the loop thread's drain.
const Waiter = struct {
    lock: c.PyThread_type_lock,
    next: ?*Waiter,
};

/// Native state, at `payload_offset` past the base class's slot storage. The
/// offset is computed at registration because the base is a Python class whose
/// size is only known at runtime.
const Payload = extern struct {
    loop: ?*py.Object,
    callback: ?*py.Object,
    context: ?*py.Object,
    heap_args: ?[*]?*py.Object,
    waiters: ?*Waiter,
    nargs: c.Py_ssize_t,
    run_state: RunState,
    flags: Flags,
    args: [1]?*py.Object,

    pub inline fn argv(self: *Payload) [*]?*py.Object {
        return self.heap_args orelse @ptrCast(&self.args);
    }
};

pub var ts_type: ?*c.PyTypeObject = null;
const payload_offset = @sizeOf(c.PyObject);
threadlocal var running_payload: ?*Payload = null;

inline fn payload(obj: *py.Object) *Payload {
    return @ptrFromInt(@intFromPtr(obj) + payload_offset);
}

pub inline fn owns(obj: *py.Object) bool {
    return py.typeOf(obj) == ts_type.?;
}

/// Untracks a handle owned only by the ready queue and candidate slot.
pub fn untrackIfQueueOnly(obj: *py.Object) void {
    // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
    var critical_section: py.CriticalSection = undefined;
    py.beginCriticalSection(&critical_section, obj);
    defer py.endCriticalSection(&critical_section);
    if (c.Py_REFCNT(obj) == 2 and c.PyObject_GC_IsTracked(obj) != 0) c.PyObject_GC_UnTrack(obj);
}

pub fn create(
    loop: *py.Object,
    callback: *py.Object,
    args: []const ?*py.Object,
    context: ?*py.Object,
) py.Error!*py.Object {
    const obj = c._PyObject_GC_New(ts_type.?) orelse return py.Error.Python;
    errdefer py.decref(obj);
    const self = payload(obj);
    self.* = std.mem.zeroes(Payload);

    try handlemod.storeArgs(self, args);

    py.incref(loop);
    self.loop = loop;
    py.incref(callback);
    self.callback = callback;

    if (context == null) self.flags.captured = true;
    self.context = try contextmod.capture(context);

    c.PyObject_GC_Track(obj);
    return obj;
}

/// Runs the callback unless a cancellation already won the state word, then
/// releases any thread blocked in `cancel` or `cancelled`.
pub fn run(obj: *py.Object) void {
    // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
    var critical_section: py.CriticalSection = undefined;
    py.beginCriticalSection(&critical_section, obj);
    defer py.endCriticalSection(&critical_section);
    const self = payload(obj);
    if (@cmpxchgStrong(RunState, &self.run_state, .pending, .running, .acq_rel, .acquire) != null) return;
    const previous_running = running_payload;
    running_payload = self;
    defer {
        // Finalizers can wait for foreign cancellers, so publish completion before releasing references.
        @atomicStore(RunState, &self.run_state, .done, .release);
        var node = self.waiters;
        self.waiters = null;
        running_payload = previous_running;
        while (node) |w| {
            node = w.next;
            c.PyThread_release_lock(w.lock);
        }
        if (self.flags.cancelled) {
            py.clear(&self.callback);
            handlemod.clearStoredArgs(self);
        }
    }
    const callback = self.callback.?;
    const context = contextmod.materialize(self.loop, &self.context) catch {
        loopmod.callbackFailed(self.loop, obj);
        return;
    };
    handlemod.invoke(obj, self.loop, callback, self.argv(), self.nargs, context);
}

/// Parks until the running callback finishes. The thread state is detached for
/// the whole wait so the loop thread can finish the callback.
///
/// The parking lock is allocated per wait because waiting is the rare case;
/// the common cancel never gets here. Should even that allocation fail, fall
/// back to yielding until the state settles.
fn awaitCompletion(self: *Payload) void {
    const lock = c.PyThread_allocate_lock() orelse {
        const tstate = c.PyEval_SaveThread();
        while (@atomicLoad(RunState, &self.run_state, .acquire) != .done) std.Thread.yield() catch {};
        c.PyEval_RestoreThread(tstate);
        return;
    };
    _ = c.PyThread_acquire_lock(lock, c.WAIT_LOCK);
    var node = Waiter{ .lock = lock, .next = self.waiters };
    self.waiters = &node;
    const tstate = c.PyEval_SaveThread();
    _ = c.PyThread_acquire_lock(lock, c.WAIT_LOCK);
    c.PyEval_RestoreThread(tstate);
    c.PyThread_free_lock(lock);
}

/// True when the callback is mid-run on some other thread, in which case the
/// caller must wait out the run before reporting or acting on cancellation.
/// From the running thread itself there is nothing to wait for: that is the
/// reentrant `cancel` a callback may issue on its own handle.
fn mustWait(self: *Payload) bool {
    if (@atomicLoad(RunState, &self.run_state, .acquire) != .running) return false;
    return running_payload != self;
}

fn cancel(self_obj: *py.Object) py.Error!*py.Object {
    const self = payload(self_obj);
    while (true) {
        const s = @atomicLoad(RunState, &self.run_state, .acquire);
        if (s == .pending) {
            if (@cmpxchgStrong(RunState, &self.run_state, .pending, .done, .acq_rel, .acquire) == null) break;
            continue;
        }
        if (s != .done and running_payload != self) awaitCompletion(self);
        break;
    }
    if (!self.flags.cancelled) {
        self.flags.cancelled = true;
        if (@atomicLoad(RunState, &self.run_state, .acquire) != .running) {
            py.clear(&self.callback);
            handlemod.clearStoredArgs(self);
        }
    }
    return py.noneRef();
}

fn cancelled(self_obj: *py.Object) py.Error!*py.Object {
    const self = payload(self_obj);
    if (mustWait(self)) awaitCompletion(self);
    return py.boolRef(self.flags.cancelled);
}

fn runMethod(self_obj: *py.Object) py.Error!*py.Object {
    run(self_obj);
    return py.noneRef();
}

fn repr(obj: ?*py.Object) callconv(.c) ?*py.Object {
    const self = payload(obj.?);
    return handlemod.describe(obj.?, self.flags.cancelled, self.callback);
}

fn dealloc(obj: ?*py.Object) callconv(.c) void {
    const self = payload(obj.?);
    const tp = py.typeOf(obj.?);
    c.PyObject_GC_UnTrack(obj);
    c.PyObject_ClearWeakRefs(obj);
    handlemod.clearStoredArgs(self);
    py.clear(&self.callback);
    contextmod.release(self.loop, &self.context, self.flags.captured);
    py.clear(&self.loop);
    tp.tp_free.?(obj);
    py.decref(tp);
}

fn visitReferences(obj: *py.Object, visitproc: c.visitproc, arg: ?*anyopaque) c_int {
    const self = payload(obj);
    const refs = [_]?*py.Object{ self.loop, self.callback, self.context };
    return handlemod.visitHandle(obj, &refs, self.argv()[0..@intCast(self.nargs)], visitproc, arg);
}

/// Visits queued references transparently when the handle is untracked.
pub fn traverseQueued(obj: *py.Object, visitproc: c.visitproc, arg: ?*anyopaque) c_int {
    if (c.PyObject_GC_IsTracked(obj) != 0) return py.visit(obj, visitproc, arg);
    return visitReferences(obj, visitproc, arg);
}

fn traverse(obj: ?*py.Object, visitproc: c.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
    return visitReferences(obj.?, visitproc, arg);
}

fn clear_(obj: ?*py.Object) callconv(.c) c_int {
    const self = payload(obj.?);
    handlemod.clearStoredArgs(self);
    py.clear(&self.callback);
    contextmod.release(self.loop, &self.context, self.flags.captured);
    py.clear(&self.loop);
    return 0;
}

fn getCallback(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.newref(payload(self_obj.?).callback orelse py.none());
}

fn getArgs(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    const self = payload(self_obj.?);
    return handlemod.argsTuple(self.flags.cancelled, self.argv()[0..@intCast(self.nargs)]);
}

fn getCancelledSlot(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.boolRef(payload(self_obj.?).flags.cancelled);
}

fn getLoop(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.newref(payload(self_obj.?).loop orelse py.none());
}

fn getContext(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    const self = payload(self_obj.?);
    const context = contextmod.materialize(self.loop, &self.context) catch return null;
    return py.newref(context);
}

// Every base slot is shadowed read-only, so the payload is the single source of
// truth for traverse, clear and dealloc.
var getsets = [_]c.PyGetSetDef{
    .{
        .name = "_callback",
        .get = py.wrapGet(getCallback),
        .set = null,
        .doc = "The scheduled callable.",
        .closure = null,
    },
    .{
        .name = "_args",
        .get = py.wrapGet(getArgs),
        .set = null,
        .doc = "Arguments the callable receives.",
        .closure = null,
    },
    .{
        .name = "_cancelled",
        .get = py.wrapGet(getCancelledSlot),
        .set = null,
        .doc = "Whether cancel() was called.",
        .closure = null,
    },
    .{
        .name = "_loop",
        .get = py.wrapGet(getLoop),
        .set = null,
        .doc = "The loop that scheduled the callback.",
        .closure = null,
    },
    .{
        .name = "_context",
        .get = py.wrapGet(getContext),
        .set = null,
        .doc = "The context the callback runs in.",
        .closure = null,
    },
    .{ .name = "_source_traceback", .get = py.wrapGet(handlemod.getNone), .set = null, .doc = null, .closure = null },
    .{ .name = "_repr", .get = py.wrapGet(handlemod.getNone), .set = null, .doc = null, .closure = null },
    .{ .name = "_lock", .get = py.wrapGet(handlemod.getNone), .set = null, .doc = null, .closure = null },
    .{ .name = null, .get = null, .set = null, .doc = null, .closure = null },
};

var methods = [_]c.PyMethodDef{
    py.methodNoArgs("cancel", cancel, "Cancel the callback, waiting out a run in progress on another thread."),
    py.methodNoArgs("cancelled", cancelled, "Return True if the callback was cancelled."),
    py.methodNoArgs("_run", runMethod, "Run the callback unless it was cancelled."),
    py.sentinel,
};

var slots = [_]c.PyType_Slot{
    .{ .slot = c.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&dealloc)) },
    .{ .slot = c.Py_tp_traverse, .pfunc = @ptrCast(@constCast(&traverse)) },
    .{ .slot = c.Py_tp_clear, .pfunc = @ptrCast(@constCast(&py.wrapClear(clear_))) },
    .{ .slot = c.Py_tp_repr, .pfunc = @ptrCast(@constCast(&py.wrapRepr(repr))) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_getset, .pfunc = @ptrCast(&getsets) },
    .{ .slot = c.Py_tp_doc, .pfunc = @ptrCast(@constCast("A callback scheduled from another thread.")) },
    .{ .slot = 0, .pfunc = null },
};

// The base carries `__weakref__` through its `__slots__`, so unlike the other
// handle types this one must not ask for a managed weakref as well. It is also
// a mutable Python class, and CPython refuses an immutable subtype of one.
const flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_DISALLOW_INSTANTIATION;

// TODO: drop when 3.13 support ends. `_ThreadSafeHandle` arrived in 3.14, and
// `Handle` is one slot short of the payload, so the subtype grows itself.
const handle_base = c.PY_VERSION_HEX < 0x030E0000;

var spec = c.PyType_Spec{
    .name = "zuvloop._zuvloop.ThreadSafeHandle",
    .basicsize = if (handle_base) payload_offset + @sizeOf(Payload) else 0,
    .itemsize = 0,
    .flags = flags,
    .slots = &slots,
};

pub fn register(module: *py.Object) py.Error!void {
    const base = py.importFrom("asyncio.events", if (handle_base) "Handle" else "_ThreadSafeHandle") orelse
        return py.Error.Python;
    defer py.decref(base);
    const base_tp: *c.PyTypeObject = @ptrCast(base);
    if (base_tp.tp_basicsize < payload_offset + @sizeOf(Payload) and !handle_base) {
        return py.errRuntime("the asyncio handle base class has an incompatible layout");
    }
    ts_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &spec, base) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "ThreadSafeHandle", @ptrCast(ts_type)) < 0) return py.Error.Python;
}
