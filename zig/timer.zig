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

const inline_args = 3;
const arg_alloc = std.heap.c_allocator;

const CANCELLED: u32 = 1 << 0;
const SCHEDULED: u32 = 1 << 1;

const Payload = extern struct {
    loop: ?*py.Object,
    callback: ?*py.Object,
    context: ?*py.Object,
    heap_args: ?[*]?*py.Object,
    nargs: c.Py_ssize_t,
    flags: u32,
    when: f64,
    args: [inline_args]?*py.Object,

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
    return payload(obj).flags & CANCELLED != 0;
}

pub fn deadline(obj: *py.Object) f64 {
    return payload(obj).when;
}

/// Leaving the heap is what ends being scheduled, by whichever route: run, due
/// but cancelled, or compacted away. Cancelling alone does not, because the
/// entry is still in the heap - the same order `BaseEventLoop` clears it in.
pub fn clearScheduled(obj: *py.Object) void {
    payload(obj).flags &= ~SCHEDULED;
}

pub fn create(
    loop: *py.Object,
    callback: *py.Object,
    args: []const ?*py.Object,
    context: ?*py.Object,
    at: f64,
) py.Error!*py.Object {
    const obj = c.PyType_GenericAlloc(timer_type.?, 0) orelse return py.Error.Python;
    const self = payload(obj);
    self.when = at;
    self.flags = SCHEDULED;

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

pub fn run(obj: *py.Object) void {
    const self = payload(obj);
    if (self.flags & CANCELLED != 0) return;
    const callback = self.callback orelse return;
    handlemod.invoke(obj, self.loop, callback, self.argv(), self.nargs, self.context.?);
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
    if (self.flags & CANCELLED == 0) {
        self.flags |= CANCELLED;
        clearArgs(self);
        py.clear(&self.callback);
        if (self.loop) |l| loopmod.noteTimerCancelled(@ptrCast(@alignCast(l)));
    }
    return py.noneRef();
}

fn cancelled(self_obj: *py.Object) py.Error!*py.Object {
    return py.boolRef(payload(self_obj).flags & CANCELLED != 0);
}

fn when(self_obj: *py.Object) py.Error!*py.Object {
    return py.float(payload(self_obj).when) orelse py.Error.Python;
}

fn repr(obj: ?*py.Object) callconv(.c) ?*py.Object {
    const self = payload(obj.?);
    const name = py.typeOf(obj.?).tp_name;
    if (self.flags & CANCELLED != 0) return c.PyUnicode_FromFormat("<%s cancelled>", name);
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
    // Cancelling drops the arguments rather than emptying them, which is the
    // difference asyncio reports as None instead of an empty tuple.
    if (self.flags & CANCELLED != 0) return py.newref(py.none());
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
    return py.boolRef(payload(self_obj.?).flags & CANCELLED != 0);
}

fn getLoop(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.newref(payload(self_obj.?).loop orelse py.none());
}

fn getContext(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.newref(payload(self_obj.?).context orelse py.none());
}

fn getWhen(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.float(payload(self_obj.?).when);
}

fn getScheduled(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.boolRef(payload(self_obj.?).flags & SCHEDULED != 0);
}

fn getNone(_: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.newref(py.none());
}

// `_when` above all: the base's ordering and equality read it, and that ordering
// is the reason this type is worth its extra bytes.
var getsets = [_]c.PyGetSetDef{
    .{ .name = "_callback", .get = getCallback, .set = null, .doc = "The scheduled callable.", .closure = null },
    .{ .name = "_args", .get = getArgs, .set = null, .doc = "Arguments the callable receives.", .closure = null },
    .{ .name = "_cancelled", .get = getCancelledSlot, .set = null, .doc = "Whether cancel() was called.", .closure = null },
    .{ .name = "_loop", .get = getLoop, .set = null, .doc = "The loop that scheduled the callback.", .closure = null },
    .{ .name = "_context", .get = getContext, .set = null, .doc = "The context the callback runs in.", .closure = null },
    .{ .name = "_when", .get = getWhen, .set = null, .doc = "The deadline, on the loop's clock.", .closure = null },
    .{ .name = "_scheduled", .get = getScheduled, .set = null, .doc = null, .closure = null },
    .{ .name = "_source_traceback", .get = getNone, .set = null, .doc = null, .closure = null },
    .{ .name = "_repr", .get = getNone, .set = null, .doc = null, .closure = null },
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
    .{ .slot = c.Py_tp_clear, .pfunc = @ptrCast(@constCast(&clear_)) },
    .{ .slot = c.Py_tp_repr, .pfunc = @ptrCast(@constCast(&repr)) },
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
