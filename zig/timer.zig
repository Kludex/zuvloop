//! `TimerHandle`: the record `call_later` and `call_at` return.
//!
//! A native subtype of `asyncio.TimerHandle`, so `isinstance` agrees and the
//! ordering the base defines over `_when` - which is how asyncio code sorts and
//! compares scheduled callbacks - works without reimplementing it. The native
//! payload is appended after the base layout and every base slot is shadowed by
//! a read-only descriptor, so the slot storage stays empty and the payload is
//! the only source of truth.
//!
//! `call_soon` keeps the leaner `handle.zig` type. The base's dead slot storage
//! is the price of that ordering - 168 bytes against 88 - which a timer can
//! afford at 3.7x uvloop and the scheduling hot path cannot.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const handlemod = @import("handle.zig");
const loopmod = @import("loop.zig");

const Flags = packed struct(u32) {
    cancelled: bool = false,
    /// What asyncio reports as `_scheduled`.
    scheduled: bool = false,
    in_heap: bool = false,
    /// Vectorcall is borrowing the callback and its arguments.
    running: bool = false,
    _: u28 = 0,
};

const Payload = extern struct {
    loop: ?*py.Object,
    callback: ?*py.Object,
    context: ?*py.Object,
    when_obj: ?*py.Object,
    heap_args: ?[*]?*py.Object,
    nargs: c.Py_ssize_t,
    flags: Flags,
    when: f64,
    args: [3]?*py.Object,

    pub inline fn argv(self: *Payload) [*]?*py.Object {
        return self.heap_args orelse @ptrCast(&self.args);
    }
};

pub var timer_type: ?*c.PyTypeObject = null;
var payload_offset: usize = 0;

inline fn payload(obj: *py.Object) *Payload {
    return @ptrFromInt(@intFromPtr(obj) + payload_offset);
}

pub inline fn owns(obj: *py.Object) bool {
    return py.typeOf(obj) == timer_type.?;
}

pub fn isCancelled(obj: *py.Object) bool {
    return payload(obj).flags.cancelled;
}

pub fn deadline(obj: *py.Object) f64 {
    return payload(obj).when;
}

/// Marks an already-due timer as ready without losing its scheduled state.
pub fn markReady(obj: *py.Object) void {
    payload(obj).flags.in_heap = false;
}

/// Leaving the scheduler ends the scheduled state, whether by running, expiry,
/// or compaction. Cancellation alone leaves the state set until retirement.
pub fn clearScheduled(obj: *py.Object) void {
    const self = payload(obj);
    self.flags.scheduled = false;
    self.flags.in_heap = false;
}

pub fn create(
    loop: *py.Object,
    callback: *py.Object,
    args: []const ?*py.Object,
    context: ?*py.Object,
    at: f64,
    original_when: ?*py.Object,
) py.Error!*py.Object {
    const obj = c.PyType_GenericAlloc(timer_type.?, 0) orelse return py.Error.Python;
    errdefer py.decref(obj);
    const self = payload(obj);
    self.when = at;
    self.flags = .{ .scheduled = true, .in_heap = true };
    if (original_when) |value| {
        py.incref(value);
        self.when_obj = value;
    }

    try handlemod.storeArgs(self, args);

    py.incref(loop);
    self.loop = loop;
    py.incref(callback);
    self.callback = callback;

    if (context) |ctx| {
        py.incref(ctx);
        self.context = ctx;
    } else {
        self.context = c.PyContext_CopyCurrent() orelse return py.Error.Python;
    }

    return obj;
}

/// Runs the callback, deferring self-cancellation cleanup until it returns.
pub fn run(obj: *py.Object) void {
    const self = payload(obj);
    if (self.flags.cancelled) return;
    const callback = self.callback orelse return;
    self.flags.running = true;
    defer {
        self.flags.running = false;
        if (self.flags.cancelled) {
            handlemod.clearStoredArgs(self);
            py.clear(&self.callback);
        }
    }
    handlemod.invoke(obj, self.loop, callback, self.argv(), self.nargs, self.context.?);
}

fn cancel(self_obj: *py.Object) py.Error!*py.Object {
    const self = payload(self_obj);
    if (!self.flags.cancelled) {
        self.flags.cancelled = true;
        if (!self.flags.running) {
            handlemod.clearStoredArgs(self);
            py.clear(&self.callback);
        }
        if (self.flags.in_heap) {
            if (self.loop) |l| loopmod.noteTimerCancelled(@ptrCast(@alignCast(l)));
        }
    }
    return py.noneRef();
}

fn cancelled(self_obj: *py.Object) py.Error!*py.Object {
    return py.boolRef(payload(self_obj).flags.cancelled);
}

fn when(self_obj: *py.Object) py.Error!*py.Object {
    const self = payload(self_obj);
    if (self.when_obj) |value| return py.newref(value).?;
    return py.float(self.when) orelse py.Error.Python;
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
    py.clear(&self.loop);
    py.clear(&self.callback);
    py.clear(&self.context);
    py.clear(&self.when_obj);
    tp.tp_free.?(obj);
    py.decref(tp);
}

fn traverse(obj: ?*py.Object, visitproc: c.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
    const self = payload(obj.?);
    const refs = [_]?*py.Object{ self.loop, self.callback, self.context, self.when_obj };
    return handlemod.visitHandle(obj.?, &refs, self.argv()[0..@intCast(self.nargs)], visitproc, arg);
}

fn clear_(obj: ?*py.Object) callconv(.c) c_int {
    const self = payload(obj.?);
    handlemod.clearStoredArgs(self);
    py.clear(&self.loop);
    py.clear(&self.callback);
    py.clear(&self.context);
    py.clear(&self.when_obj);
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
    return py.newref(payload(self_obj.?).context orelse py.none());
}

fn getWhen(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    const self = payload(self_obj.?);
    if (self.when_obj) |value| return py.newref(value);
    return py.float(self.when);
}

fn getScheduled(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.boolRef(payload(self_obj.?).flags.scheduled);
}

// `_when` above all: the base's ordering and equality read it, and that ordering
// is the reason this type is worth its extra bytes.
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
    .{
        .name = "_when",
        .get = py.wrapGet(getWhen),
        .set = null,
        .doc = "The deadline, on the loop's clock.",
        .closure = null,
    },
    .{ .name = "_scheduled", .get = py.wrapGet(getScheduled), .set = null, .doc = null, .closure = null },
    .{ .name = "_source_traceback", .get = py.wrapGet(handlemod.getNone), .set = null, .doc = null, .closure = null },
    .{ .name = "_repr", .get = py.wrapGet(handlemod.getNone), .set = null, .doc = null, .closure = null },
    .{ .name = null, .get = null, .set = null, .doc = null, .closure = null },
};

var methods = [_]c.PyMethodDef{
    py.methodNoArgs("cancel", cancel, "Cancel the callback."),
    py.methodNoArgs("cancelled", cancelled, "Return True if the callback was cancelled."),
    py.methodNoArgs("when", when, "Return the scheduled time, on the loop's clock."),
    py.sentinel,
};

var slots = [_]c.PyType_Slot{
    .{ .slot = c.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&dealloc)) },
    .{ .slot = c.Py_tp_traverse, .pfunc = @ptrCast(@constCast(&traverse)) },
    .{ .slot = c.Py_tp_clear, .pfunc = @ptrCast(@constCast(&py.wrapClear(clear_))) },
    .{ .slot = c.Py_tp_repr, .pfunc = @ptrCast(@constCast(&py.wrapRepr(repr))) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_getset, .pfunc = @ptrCast(&getsets) },
    .{ .slot = c.Py_tp_doc, .pfunc = @ptrCast(@constCast("A callback scheduled for a deadline.")) },
    .{ .slot = 0, .pfunc = null },
};

// The base carries `__weakref__` through its `__slots__`, so this must not ask
// for a managed weakref as well. It is also a mutable Python class, and CPython
// refuses an immutable subtype of one.
const flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_DISALLOW_INSTANTIATION;

var spec = c.PyType_Spec{
    .name = "zuvloop._zuvloop.TimerHandle",
    .basicsize = 0,
    .itemsize = 0,
    .flags = flags,
    .slots = &slots,
};

pub fn register(module: *py.Object) py.Error!void {
    const base = py.importFrom("asyncio", "TimerHandle") orelse return py.Error.Python;
    defer py.decref(base);
    const base_tp: *c.PyTypeObject = @ptrCast(base);
    payload_offset = std.mem.alignForward(usize, @intCast(base_tp.tp_basicsize), @alignOf(Payload));
    spec.basicsize = @intCast(payload_offset + @sizeOf(Payload));
    timer_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &spec, base) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "TimerHandle", @ptrCast(timer_type)) < 0) return py.Error.Python;
}
