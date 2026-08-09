//! `ThreadSafeHandle`: the record `call_soon_threadsafe` returns.
//!
//! A native subtype of `asyncio.events._ThreadSafeHandle`, so the 3.14 contract
//! holds with none of its costs: `isinstance` agrees, and `cancel()` or
//! `cancelled()` from a foreign thread still blocks while the callback runs,
//! but through an atomic state machine and a futex instead of a per-handle
//! `threading.RLock`. The base class's slots are never written; every one is
//! shadowed by a read-only descriptor over the native payload appended after
//! the base layout.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const handlemod = @import("handle.zig");

const inline_args = 3;
const arg_alloc = std.heap.c_allocator;

const PENDING: u32 = 0;
const RUNNING: u32 = 1;
/// Ran, or cancelled before it could run; either way it never will again.
const DONE: u32 = 2;

/// One blocked `cancel` or `cancelled` call, parked on a lock it acquired
/// twice; the loop thread releases it when the callback finishes. Lives on the
/// blocked thread's stack, which the block itself keeps alive. The list is
/// only touched under the GIL, which is what serialises linking against the
/// loop thread's drain.
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
    run_state: u32,
    cancelled_flag: u32,
    runner: c_ulong,
    args: [inline_args]?*py.Object,

    pub inline fn argv(self: *Payload) [*]?*py.Object {
        return self.heap_args orelse @ptrCast(&self.args);
    }
};

pub var ts_type: ?*c.PyTypeObject = null;
var payload_offset: usize = 0;

inline fn payload(obj: *py.Object) *Payload {
    return @ptrFromInt(@intFromPtr(obj) + payload_offset);
}

pub inline fn owns(obj: *py.Object) bool {
    return py.typeOf(obj) == ts_type.?;
}

pub fn create(
    loop: *py.Object,
    callback: *py.Object,
    args: []const ?*py.Object,
    context: ?*py.Object,
) py.Error!*py.Object {
    const obj = c.PyType_GenericAlloc(ts_type.?, 0) orelse return py.Error.Python;
    const self = payload(obj);

    if (args.len > inline_args) {
        const buf = arg_alloc.alloc(?*py.Object, args.len) catch {
            py.decref(obj);
            return py.errNoMemory();
        };
        self.heap_args = buf.ptr;
    }
    const dst = self.argv();
    for (args, 0..) |a, i| {
        py.incref(a.?);
        dst[i] = a;
    }
    self.nargs = @intCast(args.len);

    py.incref(loop);
    self.loop = loop;
    py.incref(callback);
    self.callback = callback;

    if (context) |ctx| {
        py.incref(ctx);
        self.context = ctx;
    } else {
        self.context = c.PyContext_CopyCurrent() orelse {
            py.decref(obj);
            return py.Error.Python;
        };
    }

    return obj;
}

/// Runs the callback unless a cancellation already won the state word, then
/// releases any thread blocked in `cancel` or `cancelled`.
pub fn run(obj: *py.Object) void {
    const self = payload(obj);
    self.runner = c.PyThread_get_thread_ident();
    if (@cmpxchgStrong(u32, &self.run_state, PENDING, RUNNING, .acq_rel, .acquire) != null) return;
    const callback = self.callback.?;
    handlemod.invoke(obj, self.loop, callback, self.argv(), self.nargs, self.context.?);
    @atomicStore(u32, &self.run_state, DONE, .release);
    var node = self.waiters;
    self.waiters = null;
    while (node) |w| {
        node = w.next;
        c.PyThread_release_lock(w.lock);
    }
}

/// Parks until the running callback finishes. The GIL is released for the
/// whole wait: the loop thread needs it to finish the callback, and a foreign
/// `threading.RLock.acquire` would release it here for the same reason.
///
/// The parking lock is allocated per wait because waiting is the rare case;
/// the common cancel never gets here. Should even that allocation fail, fall
/// back to yielding until the state settles.
fn awaitCompletion(self: *Payload) void {
    const lock = c.PyThread_allocate_lock() orelse {
        const tstate = c.PyEval_SaveThread();
        while (@atomicLoad(u32, &self.run_state, .acquire) != DONE) std.Thread.yield() catch {};
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
    if (@atomicLoad(u32, &self.run_state, .acquire) != RUNNING) return false;
    return self.runner != c.PyThread_get_thread_ident();
}

fn clearArgs(self: *Payload) void {
    const n: usize = @intCast(self.nargs);
    const dst = self.argv();
    var i: usize = 0;
    while (i < n) : (i += 1) py.clear(&dst[i]);
    self.nargs = 0;
    if (self.heap_args) |buf| {
        arg_alloc.free(buf[0..n]);
        self.heap_args = null;
    }
}

fn cancel(self_obj: *py.Object) py.Error!*py.Object {
    const self = payload(self_obj);
    while (true) {
        const s = @atomicLoad(u32, &self.run_state, .acquire);
        if (s == PENDING) {
            if (@cmpxchgStrong(u32, &self.run_state, PENDING, DONE, .acq_rel, .acquire) == null) break;
            continue;
        }
        if (s != DONE and self.runner != c.PyThread_get_thread_ident()) awaitCompletion(self);
        break;
    }
    if (self.cancelled_flag == 0) {
        self.cancelled_flag = 1;
        py.clear(&self.callback);
        clearArgs(self);
    }
    return py.noneRef();
}

fn cancelled(self_obj: *py.Object) py.Error!*py.Object {
    const self = payload(self_obj);
    if (mustWait(self)) awaitCompletion(self);
    return py.boolRef(self.cancelled_flag != 0);
}

fn runMethod(self_obj: *py.Object) py.Error!*py.Object {
    run(self_obj);
    return py.noneRef();
}

fn repr(obj: ?*py.Object) callconv(.c) ?*py.Object {
    const self = payload(obj.?);
    const name = py.typeOf(obj.?).tp_name;
    if (self.cancelled_flag != 0) return c.PyUnicode_FromFormat("<%s cancelled>", name);
    return c.PyUnicode_FromFormat("<%s %R>", name, self.callback orelse py.none());
}

fn dealloc(obj: ?*py.Object) callconv(.c) void {
    const self = payload(obj.?);
    const tp = py.typeOf(obj.?);
    c.PyObject_GC_UnTrack(obj);
    c.PyObject_ClearWeakRefs(obj);
    clearArgs(self);
    py.clear(&self.loop);
    py.clear(&self.callback);
    py.clear(&self.context);
    tp.tp_free.?(obj);
    py.decref(tp);
}

fn traverse(obj: ?*py.Object, visitproc: c.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
    const self = payload(obj.?);
    var r = py.visit(self.loop, visitproc, arg);
    if (r != 0) return r;
    r = py.visit(self.callback, visitproc, arg);
    if (r != 0) return r;
    r = py.visit(self.context, visitproc, arg);
    if (r != 0) return r;
    const n: usize = @intCast(self.nargs);
    const items = self.argv();
    var i: usize = 0;
    while (i < n) : (i += 1) {
        r = py.visit(items[i], visitproc, arg);
        if (r != 0) return r;
    }
    return py.visit(@ptrCast(py.typeOf(obj.?)), visitproc, arg);
}

fn clear_(obj: ?*py.Object) callconv(.c) c_int {
    const self = payload(obj.?);
    clearArgs(self);
    py.clear(&self.loop);
    py.clear(&self.callback);
    py.clear(&self.context);
    return 0;
}

fn getCallback(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.newref(payload(self_obj.?).callback orelse py.none());
}

fn getArgs(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    const self = payload(self_obj.?);
    const n: usize = @intCast(self.nargs);
    const items = self.argv();
    const tuple = c.PyTuple_New(self.nargs) orelse return null;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        _ = c.PyTuple_SetItem(tuple, @intCast(i), py.newref(items[i]));
    }
    return tuple;
}

fn getCancelledSlot(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.boolRef(payload(self_obj.?).cancelled_flag != 0);
}

fn getLoop(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.newref(payload(self_obj.?).loop orelse py.none());
}

fn getContext(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.newref(payload(self_obj.?).context orelse py.none());
}

fn getNone(_: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.newref(py.none());
}

// Every base slot is shadowed read-only, so their storage stays empty and the
// payload is the single source of truth for traverse, clear and dealloc.
var getsets = [_]c.PyGetSetDef{
    .{ .name = "_callback", .get = getCallback, .set = null, .doc = "The scheduled callable.", .closure = null },
    .{ .name = "_args", .get = getArgs, .set = null, .doc = "Arguments the callable receives.", .closure = null },
    .{ .name = "_cancelled", .get = getCancelledSlot, .set = null, .doc = "Whether cancel() was called.", .closure = null },
    .{ .name = "_loop", .get = getLoop, .set = null, .doc = "The loop that scheduled the callback.", .closure = null },
    .{ .name = "_context", .get = getContext, .set = null, .doc = "The context the callback runs in.", .closure = null },
    .{ .name = "_source_traceback", .get = getNone, .set = null, .doc = null, .closure = null },
    .{ .name = "_repr", .get = getNone, .set = null, .doc = null, .closure = null },
    .{ .name = "_lock", .get = getNone, .set = null, .doc = null, .closure = null },
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
    .{ .slot = c.Py_tp_clear, .pfunc = @ptrCast(@constCast(&clear_)) },
    .{ .slot = c.Py_tp_repr, .pfunc = @ptrCast(@constCast(&repr)) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_getset, .pfunc = @ptrCast(&getsets) },
    .{ .slot = c.Py_tp_doc, .pfunc = @ptrCast(@constCast("A callback scheduled from another thread.")) },
    .{ .slot = 0, .pfunc = null },
};

// The base carries `__weakref__` through its `__slots__`, so unlike the other
// handle types this one must not ask for a managed weakref as well. It is also
// a mutable Python class, and CPython refuses an immutable subtype of one.
const flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_DISALLOW_INSTANTIATION;

var spec = c.PyType_Spec{
    .name = "zuvloop._zuvloop.ThreadSafeHandle",
    .basicsize = 0,
    .itemsize = 0,
    .flags = flags,
    .slots = &slots,
};

pub fn register(module: *py.Object) py.Error!void {
    const base = py.importFrom("asyncio.events", "_ThreadSafeHandle") orelse return py.Error.Python;
    defer py.decref(base);
    const base_tp: *c.PyTypeObject = @ptrCast(base);
    payload_offset = std.mem.alignForward(usize, @intCast(base_tp.tp_basicsize), @alignOf(Payload));
    spec.basicsize = @intCast(payload_offset + @sizeOf(Payload));
    ts_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &spec, base) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "ThreadSafeHandle", @ptrCast(ts_type)) < 0) return py.Error.Python;
}
