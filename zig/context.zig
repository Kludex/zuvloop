//! Callback context capture shared by regular and thread-safe handles.
//!
//! Handles own materialized contexts. Empty captured contexts can move into
//! the loop's bounded reuse pool when a handle releases them.

const py = @import("py.zig");
const c = py.c;
const loopmod = @import("loop.zig");

/// Marks a context as implicitly captured rather than explicitly supplied.
pub const captured_flag: u32 = 1 << 1;

const ContextObject = extern struct {
    ob_base: c.PyObject,
    previous: ?*ContextObject,
    variables: ?*py.Object,
    weakrefs: ?*py.Object,
    entered: c_int,
};

/// Captures a non-empty current context and defers allocating an empty one.
pub fn capture(explicit: ?*py.Object, flags: *u32) py.Error!?*py.Object {
    if (explicit) |context| {
        py.incref(context);
        return context;
    }
    flags.* |= captured_flag;
    const thread_state = c.PyThreadState_Get();
    const current_slot: *?*py.Object = @ptrFromInt(
        @intFromPtr(thread_state) + c.ZUVLOOP_PYTHREADSTATE_CONTEXT_OFFSET,
    );
    const current = current_slot.*;
    const context_size = if (current) |context| c.PyObject_Size(context) else 0;
    if (context_size < 0) return py.Error.Python;
    if (context_size == 0) return null;
    return c.PyContext_CopyCurrent() orelse return py.Error.Python;
}

/// Returns the context in `context`, allocating or borrowing an empty one.
pub fn materialize(loop: ?*py.Object, context: *?*py.Object) py.Error!*py.Object {
    if (context.*) |existing| return existing;
    const created = if (loop) |loop_obj| loopmod.takeEmptyContext(loop_obj) else null;
    context.* = created orelse c.PyContext_New() orelse return py.Error.Python;
    return context.*.?;
}

/// Releases a context or moves an unused captured one into the loop pool.
pub fn release(loop: ?*py.Object, context: *?*py.Object, flags: u32) void {
    const current = context.* orelse return;
    context.* = null;
    if (flags & captured_flag != 0 and c.Py_REFCNT(current) == 1) {
        const context_obj: *ContextObject = @ptrCast(@alignCast(current));
        const size = c.PyObject_Size(current);
        if (size == 0 and context_obj.weakrefs == null and context_obj.entered == 0) {
            if (loop) |loop_obj| {
                if (loopmod.recycleEmptyContext(loop_obj, current)) return;
            }
        } else if (size < 0) {
            c.PyErr_Clear();
        }
    }
    py.decref(current);
}
