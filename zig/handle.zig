//! `Handle`: the callback record `call_soon` returns.
//!
//! Arguments live in the object's variable tail, so a zero-argument callback
//! reserves no argument slots and no call needs a secondary allocation. Calls
//! still avoid a tuple and invoke the callback through vectorcall. Deliberately
//! not a subtype of `asyncio.Handle`, whose slots are storage this never writes.
//! `timer.zig` pays that price, where the ordering the base defines earns it.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const contextmod = @import("context.zig");
const loopmod = @import("loop.zig");

const arg_alloc = std.heap.c_allocator;

pub const Flags = packed struct(u32) {
    cancelled: bool = false,
    /// The context was captured implicitly rather than supplied by the caller.
    captured: bool = false,
    /// Vectorcall is borrowing the callback and its arguments.
    running: bool = false,
    args_cleared: bool = false,
    _: u28 = 0,
};

pub const Handle = extern struct {
    ob_base: c.PyVarObject,
    loop: ?*py.Object,
    callback: ?*py.Object,
    context: ?*py.Object,
    flags: Flags,

    fn argv(self: *Handle) [*]?*py.Object {
        return @ptrCast(@alignCast(c.PyObject_GetItemData(@ptrCast(self))));
    }

    fn nargs(self: *const Handle) c.Py_ssize_t {
        return if (self.flags.args_cleared) 0 else self.ob_base.ob_size;
    }

    /// Reports whether cancellation prevents this handle from running.
    pub fn isCancelled(self: *const Handle) bool {
        return self.flags.cancelled;
    }
};

pub var handle_type: ?*c.PyTypeObject = null;

/// Allocates a handle and takes ownership of nothing: all references are new.
pub fn create(
    tp: *c.PyTypeObject,
    loop: *py.Object,
    callback: *py.Object,
    args: []const ?*py.Object,
    context: ?*py.Object,
) py.Error!*Handle {
    const obj = c.PyType_GenericAlloc(tp, @intCast(args.len)) orelse return py.Error.Python;
    const self: *Handle = @ptrCast(@alignCast(obj));

    const dst = self.argv();
    for (args, 0..) |a, i| {
        py.incref(a.?);
        dst[i] = a;
    }

    py.incref(loop);
    self.loop = loop;
    py.incref(callback);
    self.callback = callback;

    if (context == null) self.flags.captured = true;
    self.context = contextmod.capture(context) catch {
        py.decref(obj);
        return py.Error.Python;
    };

    return self;
}

/// Runs the callback inside its context, routing failures to the loop.
pub fn run(self: *Handle) void {
    if (self.isCancelled()) return;
    const callback = self.callback orelse return;
    self.flags.running = true;
    defer {
        self.flags.running = false;
        if (self.isCancelled()) {
            clearArgs(self);
            py.clear(&self.callback);
        }
    }
    const context = contextmod.materializeToRun(@ptrCast(self), self.loop, &self.context) catch {
        loopmod.callbackFailed(self.loop, @ptrCast(self));
        return;
    };
    invoke(@ptrCast(self), self.loop, callback, self.argv(), self.nargs(), context);
}

/// Calls `callback(*argv)` inside `ctx`, routing failures to `loop` with
/// `owner` named as the failing handle.
pub fn invoke(
    owner: *py.Object,
    loop: ?*py.Object,
    callback: *py.Object,
    argv: [*]?*py.Object,
    nargs: c.Py_ssize_t,
    ctx: *py.Object,
) void {
    if (c.PyContext_Enter(ctx) < 0) {
        loopmod.callbackFailed(loop, owner);
        return;
    }
    const result = c.PyObject_Vectorcall(callback, argv, @intCast(nargs), null);
    const failure = if (result) |r| blk: {
        py.decref(r);
        break :blk null;
    } else c.PyErr_GetRaisedException();
    if (c.PyContext_Exit(ctx) < 0) py.writeUnraisable(owner);
    // asyncio reports callback failures after Context.run() has returned. Do
    // the same so an exception handler can re-enter the handle's context.
    if (failure) |exc| {
        c.PyErr_SetRaisedException(exc);
        loopmod.callbackFailed(loop, owner);
    }
}

/// Copies `args` into a payload's inline slots, or onto the heap when they do
/// not fit. Shared by the handle types that store arguments in their payload:
/// each declares `args`, `heap_args`, `nargs` and `argv()`.
pub fn storeArgs(payload: anytype, args: []const ?*py.Object) py.Error!void {
    if (args.len > payload.args.len) {
        const buf = arg_alloc.alloc(?*py.Object, args.len) catch return py.errNoMemory();
        payload.heap_args = buf.ptr;
    }
    const dst = payload.argv();
    for (args, 0..) |a, i| {
        py.incref(a.?);
        dst[i] = a;
    }
    payload.nargs = @intCast(args.len);
}

/// Releases what `storeArgs` stored.
pub fn clearStoredArgs(payload: anytype) void {
    const n: usize = @intCast(payload.nargs);
    const dst = payload.argv();
    var i: usize = 0;
    while (i < n) : (i += 1) py.clear(&dst[i]);
    payload.nargs = 0;
    if (payload.heap_args) |buf| {
        arg_alloc.free(buf[0..n]);
        payload.heap_args = null;
    }
}

/// The `_args` a handle reports. Cancelling drops the arguments rather than
/// emptying them, which is the difference asyncio reports as None instead of
/// an empty tuple.
pub fn argsTuple(is_cancelled: bool, args: []const ?*py.Object) ?*py.Object {
    if (is_cancelled) return py.newref(py.none());
    const tuple = c.PyTuple_New(@intCast(args.len)) orelse return null;
    for (args, 0..) |a, i| _ = c.PyTuple_SetItem(tuple, @intCast(i), py.newref(a));
    return tuple;
}

/// Visits a handle's own references, then its arguments, then its heap type.
pub fn visitHandle(
    obj: *py.Object,
    refs: []const ?*py.Object,
    args: []const ?*py.Object,
    visitproc: c.visitproc,
    arg: ?*anyopaque,
) c_int {
    for (refs) |ref| {
        const r = py.visit(ref, visitproc, arg);
        if (r != 0) return r;
    }
    for (args) |a| {
        const r = py.visit(a, visitproc, arg);
        if (r != 0) return r;
    }
    return py.visit(@ptrCast(py.typeOf(obj)), visitproc, arg);
}

/// The `repr` every handle type shares.
pub fn describe(obj: *py.Object, is_cancelled: bool, callback: ?*py.Object) ?*py.Object {
    const name = py.typeOf(obj).tp_name;
    if (is_cancelled) return c.PyUnicode_FromFormat("<%s cancelled>", name);
    return c.PyUnicode_FromFormat("<%s %R>", name, callback orelse py.none());
}

/// Getter for the base-class slots a native handle has nothing to report in.
pub fn getNone(_: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    return py.newref(py.none());
}

fn clearArgs(self: *Handle) void {
    if (self.flags.args_cleared) return;
    const n: usize = @intCast(self.ob_base.ob_size);
    const dst = self.argv();
    var i: usize = 0;
    while (i < n) : (i += 1) py.clear(&dst[i]);
    self.flags.args_cleared = true;
}

fn dealloc(obj: ?*py.Object) callconv(.c) void {
    const self: *Handle = @ptrCast(@alignCast(obj.?));
    const tp = py.typeOf(obj.?);
    c.PyObject_GC_UnTrack(obj);
    c.PyObject_ClearWeakRefs(obj);
    clearArgs(self);
    py.clear(&self.callback);
    contextmod.release(self.loop, &self.context, self.flags.captured);
    py.clear(&self.loop);
    tp.tp_free.?(obj);
    py.decref(tp);
}

fn traverse(obj: ?*py.Object, visitproc: c.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
    const self: *Handle = @ptrCast(@alignCast(obj.?));
    const refs = [_]?*py.Object{ self.loop, self.callback, self.context };
    return visitHandle(obj.?, &refs, self.argv()[0..@intCast(self.nargs())], visitproc, arg);
}

fn clear_(obj: ?*py.Object) callconv(.c) c_int {
    const self: *Handle = @ptrCast(@alignCast(obj.?));
    clearArgs(self);
    py.clear(&self.callback);
    contextmod.release(self.loop, &self.context, self.flags.captured);
    py.clear(&self.loop);
    return 0;
}

fn cancel(self_obj: *py.Object) py.Error!*py.Object {
    const self: *Handle = @ptrCast(@alignCast(self_obj));
    if (!self.isCancelled()) {
        self.flags.cancelled = true;
        // Vectorcall borrows these references. A callback may cancel its own
        // handle, so release them when run() regains control instead.
        if (!self.flags.running) {
            clearArgs(self);
            py.clear(&self.callback);
        }
    }
    return py.noneRef();
}

fn cancelled(self_obj: *py.Object) py.Error!*py.Object {
    const self: *Handle = @ptrCast(@alignCast(self_obj));
    return py.boolRef(self.isCancelled());
}

fn getContext(self_obj: *py.Object) py.Error!*py.Object {
    const self: *Handle = @ptrCast(@alignCast(self_obj));
    const context = try contextmod.materialize(self.loop, &self.context);
    return py.newref(context).?;
}

fn getCallback(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    const self: *Handle = @ptrCast(@alignCast(self_obj.?));
    return py.newref(self.callback orelse py.none());
}

fn getArgs(self_obj: ?*py.Object, _: ?*anyopaque) callconv(.c) ?*py.Object {
    const self: *Handle = @ptrCast(@alignCast(self_obj.?));
    return argsTuple(self.isCancelled(), self.argv()[0..@intCast(self.nargs())]);
}

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
    .{ .name = null, .get = null, .set = null, .doc = null, .closure = null },
};

fn repr(obj: ?*py.Object) callconv(.c) ?*py.Object {
    const self: *Handle = @ptrCast(@alignCast(obj.?));
    return describe(obj.?, self.isCancelled(), self.callback);
}

var handle_methods = [_]c.PyMethodDef{
    py.methodNoArgs("cancel", cancel, "Cancel the callback."),
    py.methodNoArgs("cancelled", cancelled, "Return True if the callback was cancelled."),
    py.methodNoArgs("get_context", getContext, "Return the context the callback runs in."),
    py.sentinel,
};

fn slots(methods: [*]c.PyMethodDef) [8]c.PyType_Slot {
    return .{
        .{ .slot = c.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&dealloc)) },
        .{ .slot = c.Py_tp_traverse, .pfunc = @ptrCast(@constCast(&traverse)) },
        .{ .slot = c.Py_tp_clear, .pfunc = @ptrCast(@constCast(&py.wrapClear(clear_))) },
        .{ .slot = c.Py_tp_repr, .pfunc = @ptrCast(@constCast(&py.wrapRepr(repr))) },
        .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(methods) },
        .{ .slot = c.Py_tp_getset, .pfunc = @ptrCast(&getsets) },
        .{ .slot = c.Py_tp_doc, .pfunc = @ptrCast(@constCast("A scheduled callback.")) },
        .{ .slot = 0, .pfunc = null },
    };
}

var handle_slots = slots(&handle_methods);

// asyncio's handles are weak-referenceable, so these must be too.
const flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_MANAGED_WEAKREF | c.Py_TPFLAGS_ITEMS_AT_END |
    c.Py_TPFLAGS_IMMUTABLETYPE | c.Py_TPFLAGS_DISALLOW_INSTANTIATION;

var handle_spec = c.PyType_Spec{
    .name = "zuvloop._zuvloop.Handle",
    .basicsize = @sizeOf(Handle),
    .itemsize = @sizeOf(?*py.Object),
    .flags = flags,
    .slots = &handle_slots,
};

pub fn register(module: *py.Object) py.Error!void {
    handle_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &handle_spec, null) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "Handle", @ptrCast(handle_type)) < 0) return py.Error.Python;
}
