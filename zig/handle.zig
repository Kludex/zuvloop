//! `Handle` and `TimerHandle`: the callback records the loop schedules.
//!
//! Both share one layout. Arguments up to `inline_args` live inside the object,
//! so the common `call_soon(cb, a, b)` never allocates a tuple and the callback
//! is invoked straight through the vectorcall protocol.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const loopmod = @import("loop.zig");

const inline_args = 3;
const arg_alloc = std.heap.c_allocator;

pub const CANCELLED: u32 = 1 << 0;
pub const IS_TIMER: u32 = 1 << 1;

pub const Handle = extern struct {
    ob_base: c.PyObject,
    loop: ?*py.Object,
    callback: ?*py.Object,
    context: ?*py.Object,
    heap_args: ?[*]?*py.Object,
    next: ?*Handle,
    nargs: c.Py_ssize_t,
    flags: u32,
    when: f64,
    args: [inline_args]?*py.Object,

    pub inline fn argv(self: *Handle) [*]?*py.Object {
        return self.heap_args orelse @ptrCast(&self.args);
    }

    pub inline fn isCancelled(self: *const Handle) bool {
        return self.flags & CANCELLED != 0;
    }
};

pub var handle_type: ?*c.PyTypeObject = null;
pub var timer_type: ?*c.PyTypeObject = null;

/// Allocates a handle and takes ownership of nothing: all references are new.
pub fn create(
    tp: *c.PyTypeObject,
    loop: *py.Object,
    callback: *py.Object,
    args: []const ?*py.Object,
    context: ?*py.Object,
) py.Error!*Handle {
    const obj = c.PyType_GenericAlloc(tp, 0) orelse return py.Error.Python;
    const self: *Handle = @ptrCast(@alignCast(obj));

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

    return self;
}

/// Runs the callback inside its context, routing failures to the loop.
pub fn run(self: *Handle) void {
    if (self.isCancelled()) return;
    const callback = self.callback orelse return;
    const ctx = self.context.?;

    if (c.PyContext_Enter(ctx) < 0) {
        loopmod.handleCallbackError(self);
        return;
    }
    const result = c.PyObject_Vectorcall(callback, self.argv(), @intCast(self.nargs), null);
    if (result) |r| {
        py.decref(r);
    } else {
        loopmod.handleCallbackError(self);
    }
    if (c.PyContext_Exit(ctx) < 0) py.writeUnraisable(@ptrCast(self));
}

fn clearArgs(self: *Handle) void {
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

fn dealloc(obj: ?*py.Object) callconv(.c) void {
    const self: *Handle = @ptrCast(@alignCast(obj.?));
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
    const self: *Handle = @ptrCast(@alignCast(obj.?));
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
    const self: *Handle = @ptrCast(@alignCast(obj.?));
    clearArgs(self);
    py.clear(&self.loop);
    py.clear(&self.callback);
    py.clear(&self.context);
    return 0;
}

fn cancel(self_obj: *py.Object) py.Error!*py.Object {
    const self: *Handle = @ptrCast(@alignCast(self_obj));
    if (!self.isCancelled()) {
        self.flags |= CANCELLED;
        clearArgs(self);
        py.clear(&self.callback);
        if (self.flags & IS_TIMER != 0) {
            if (self.loop) |l| loopmod.noteTimerCancelled(@ptrCast(@alignCast(l)));
        }
    }
    return py.noneRef();
}

fn cancelled(self_obj: *py.Object) py.Error!*py.Object {
    const self: *Handle = @ptrCast(@alignCast(self_obj));
    return py.boolRef(self.isCancelled());
}

fn when(self_obj: *py.Object) py.Error!*py.Object {
    const self: *Handle = @ptrCast(@alignCast(self_obj));
    return py.float(self.when) orelse py.Error.Python;
}

fn getCallback(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    const self: *Handle = @ptrCast(@alignCast(self_obj.?));
    return py.newref(self.callback orelse py.none());
}

fn getArgs(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    const self: *Handle = @ptrCast(@alignCast(self_obj.?));
    const n: usize = @intCast(self.nargs);
    const items = self.argv();
    const tuple = c.PyTuple_New(self.nargs) orelse return null;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        _ = c.PyTuple_SetItem(tuple, @intCast(i), py.newref(items[i]));
    }
    return tuple;
}

var getsets = [_]c.PyGetSetDef{
    .{ .name = "_callback", .get = getCallback, .set = null, .doc = "The scheduled callable.", .closure = null },
    .{ .name = "_args", .get = getArgs, .set = null, .doc = "Arguments the callable receives.", .closure = null },
    .{ .name = null, .get = null, .set = null, .doc = null, .closure = null },
};

fn repr(obj: ?*py.Object) callconv(.c) ?*py.Object {
    const self: *Handle = @ptrCast(@alignCast(obj.?));
    const name = py.typeOf(obj.?).tp_name;
    if (self.isCancelled()) return c.PyUnicode_FromFormat("<%s cancelled>", name);
    return c.PyUnicode_FromFormat("<%s %R>", name, self.callback orelse py.none());
}

var handle_methods = [_]c.PyMethodDef{
    py.methodNoArgs("cancel", cancel, "Cancel the callback."),
    py.methodNoArgs("cancelled", cancelled, "Return True if the callback was cancelled."),
    py.sentinel,
};

var timer_methods = [_]c.PyMethodDef{
    py.methodNoArgs("cancel", cancel, "Cancel the callback."),
    py.methodNoArgs("cancelled", cancelled, "Return True if the callback was cancelled."),
    py.methodNoArgs("when", when, "Return the scheduled time, on the loop's clock."),
    py.sentinel,
};

fn slots(methods: [*]c.PyMethodDef) [8]c.PyType_Slot {
    return .{
        .{ .slot = c.Py_tp_dealloc, .pfunc = @constCast(@ptrCast(&dealloc)) },
        .{ .slot = c.Py_tp_traverse, .pfunc = @constCast(@ptrCast(&traverse)) },
        .{ .slot = c.Py_tp_clear, .pfunc = @constCast(@ptrCast(&clear_)) },
        .{ .slot = c.Py_tp_repr, .pfunc = @constCast(@ptrCast(&repr)) },
        .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(methods) },
        .{ .slot = c.Py_tp_getset, .pfunc = @ptrCast(&getsets) },
        .{ .slot = c.Py_tp_doc, .pfunc = @constCast(@ptrCast("A scheduled callback.")) },
        .{ .slot = 0, .pfunc = null },
    };
}

var handle_slots = slots(&handle_methods);
var timer_slots = slots(&timer_methods);

// asyncio's handles are weak-referenceable, so these must be too.
const flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_MANAGED_WEAKREF |
    c.Py_TPFLAGS_IMMUTABLETYPE | c.Py_TPFLAGS_DISALLOW_INSTANTIATION;

var handle_spec = c.PyType_Spec{
    .name = "zuv._zuv.Handle",
    .basicsize = @sizeOf(Handle),
    .itemsize = 0,
    .flags = flags,
    .slots = &handle_slots,
};

var timer_spec = c.PyType_Spec{
    .name = "zuv._zuv.TimerHandle",
    .basicsize = @sizeOf(Handle),
    .itemsize = 0,
    .flags = flags,
    .slots = &timer_slots,
};

pub fn register(module: *py.Object) py.Error!void {
    handle_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &handle_spec, null) orelse return py.Error.Python);
    timer_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &timer_spec, null) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "Handle", @ptrCast(handle_type)) < 0) return py.Error.Python;
    if (c.PyModule_AddObjectRef(module, "TimerHandle", @ptrCast(timer_type)) < 0) return py.Error.Python;
}
