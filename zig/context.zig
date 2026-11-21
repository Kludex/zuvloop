//! Callback context capture shared by regular and thread-safe handles.
//!
//! Handles own materialized contexts. Empty captured contexts can move into
//! the loop's bounded reuse pool when a handle releases them.

const py = @import("py.zig");
const c = py.c;
const loopmod = @import("loop.zig");

const ContextObject = extern struct {
    ob_base: c.PyObject,
    previous: ?*ContextObject,
    variables: ?*py.Object,
    weakrefs: ?*py.Object,
    entered: c_int,
};

/// The map every empty context shares: CPython hands out one immortal empty HAMT.
/// Left null if it ever stops, which makes every context read as non-empty.
var empty_variables: ?*py.Object = null;

/// Reports whether `context` holds no variables, without calling into the interpreter.
fn isEmpty(context: *py.Object) bool {
    const variables = @as(*ContextObject, @ptrCast(@alignCast(context))).variables;
    return variables != null and variables == empty_variables;
}

/// Returns the supplied context, or captures a non-empty current one and
/// defers allocating an empty one.
pub fn capture(explicit: ?*py.Object) py.Error!?*py.Object {
    if (explicit) |context| {
        py.incref(context);
        return context;
    }
    const thread_state = c.PyThreadState_Get();
    const current_slot: *?*py.Object = @ptrFromInt(
        @intFromPtr(thread_state) + c.ZUVLOOP_PYTHREADSTATE_CONTEXT_OFFSET,
    );
    const current = current_slot.* orelse return null;
    if (isEmpty(current)) return null;
    return c.PyContext_CopyCurrent() orelse return py.Error.Python;
}

/// Returns the context in `context`, allocating or borrowing an empty one.
pub fn materialize(loop: ?*py.Object, context: *?*py.Object) py.Error!*py.Object {
    if (context.*) |existing| return existing;
    if (loop) |loop_obj| {
        if (loopmod.isRunningThread(loop_obj)) {
            if (loopmod.takeEmptyContext(loop_obj)) |pooled| return publish(context, pooled);
        }
    }
    return allocate(context);
}

/// Taking from the pool allocates nothing, so the slot is still the empty one the caller saw.
inline fn publish(context: *?*py.Object, context_obj: *py.Object) *py.Object {
    @atomicStore(?*py.Object, context, context_obj, .release);
    return context_obj;
}

fn allocate(context: *?*py.Object) py.Error!*py.Object {
    const context_obj = c.PyContext_New() orelse return py.Error.Python;
    // Allocating can suspend the caller's critical section, so another thread may have won.
    if (context.*) |winner| {
        py.decref(context_obj);
        return winner;
    }
    return publish(context, context_obj);
}

/// `materialize` for the path that runs the callback. A getter on another thread materializes
/// inside `owner`'s critical section, so entering it here stops both from creating a context.
pub fn materializeToRun(owner: *py.Object, loop: ?*py.Object, context: *?*py.Object) py.Error!*py.Object {
    // A context is published once and outlives the run, so only an empty slot needs the section.
    if (@atomicLoad(?*py.Object, context, .acquire)) |existing| return existing;
    // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
    var critical_section: py.CriticalSection = undefined;
    py.beginCriticalSection(&critical_section, owner);
    defer py.endCriticalSection(&critical_section);
    if (context.*) |existing| return existing;
    // Only the loop's own thread runs a handle, so the pool is taken from without asking whose thread this is.
    if (loop) |loop_obj| {
        if (loopmod.takeEmptyContext(loop_obj)) |pooled| return publish(context, pooled);
    }
    return allocate(context);
}

/// Releases a context or moves an unused captured one into the loop pool.
/// `captured` says the context was not supplied by the caller.
pub fn release(loop: ?*py.Object, context: *?*py.Object, captured: bool) void {
    const current = context.* orelse return;
    context.* = null;
    const context_obj: *ContextObject = @ptrCast(@alignCast(current));
    // `Py_REFCNT(o) == 1` proves nothing without the GIL: another thread can still take a
    // reference from the collector's view of the object while this one decides.
    // The GC may clear the context before its handle, which leaves it without the shared empty map.
    if (captured and
        (if (@hasDecl(c, "Py_GIL_DISABLED"))
            c.PyUnstable_Object_IsUniquelyReferenced(current) != 0
        else
            c.Py_REFCNT(current) == 1) and
        isEmpty(current) and context_obj.weakrefs == null and context_obj.entered == 0)
    {
        if (loop) |loop_obj| {
            if (loopmod.isRunningThread(loop_obj) and loopmod.recycleEmptyContext(loop_obj, current)) return;
        }
    }
    py.decref(current);
}

/// Refuses to load against a `contextvars.Context` whose layout is not the one
/// `ContextObject` mirrors. The struct is private to CPython, and reading it
/// through the wrong shape would recycle the wrong contexts rather than raise.
pub fn register() py.Error!void {
    const probe = c.PyContext_New() orelse return py.Error.Python;
    defer py.decref(probe);
    const mirror: *ContextObject = @ptrCast(@alignCast(probe));
    // A new context holds a variable map and nothing else, which places `variables`.
    var matches = py.typeOf(probe).tp_basicsize == @sizeOf(ContextObject) and
        mirror.previous == null and mirror.variables != null and mirror.weakrefs == null and mirror.entered == 0;

    // Two fresh contexts share their map only while CPython keeps the empty one a singleton.
    // Read before the probe is entered, after which a context watcher may share it with another thread.
    const second = c.PyContext_New() orelse return py.Error.Python;
    defer py.decref(second);
    const shared_map = if (matches and @as(*ContextObject, @ptrCast(@alignCast(second))).variables == mirror.variables)
        mirror.variables
    else
        null;

    // The other fields `release` reads are each driven to a value only they
    // can hold, so one that moved is caught even when the size did not change.
    const reference = c.PyWeakref_NewRef(probe, null) orelse return py.Error.Python;
    defer py.decref(reference);
    matches = matches and mirror.weakrefs != null;
    // Entering notifies any installed context watcher, which is free to share the
    // probe with another thread, so this read must not race a write made there.
    if (c.PyContext_Enter(probe) < 0) return py.Error.Python;
    matches = matches and @atomicLoad(c_int, &mirror.entered, .seq_cst) == 1;
    if (c.PyContext_Exit(probe) < 0) return py.Error.Python;

    if (!matches) {
        return py.errRuntime("contextvars.Context instance layout is not the one zuvloop was built against");
    }
    empty_variables = shared_map;
}
