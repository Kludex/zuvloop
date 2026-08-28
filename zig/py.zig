//! Thin ergonomic layer over the CPython C API.

const std = @import("std");
pub const c = @import("c.zig").c;
const uv = @import("uv.zig");

pub const Object = c.PyObject;
pub const Ref = *Object;
pub const CriticalSection = c.zuvloop_critical_section;

pub inline fn beginCriticalSection(section: *CriticalSection, object: *Object) void {
    if (@hasDecl(c, "Py_GIL_DISABLED")) c.zuvloop_critical_section_begin(section, object);
}

pub inline fn endCriticalSection(section: *CriticalSection) void {
    if (@hasDecl(c, "Py_GIL_DISABLED")) c.zuvloop_critical_section_end(section);
}

/// The interpreter's singletons and exception types, captured through function
/// calls in `initConstants`. Referencing the DLL's data symbols directly would
/// need cross-library data imports, which the linker's auto-import machinery
/// does not fix up for this extension on Windows - every read would fault.
// SAFETY: module exec initializes this cache before registering any code that reads it.
var none_object: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
var true_object: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
var false_object: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
pub var exc_system_exit: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
pub var exc_keyboard_interrupt: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
pub var exc_runtime_error: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
pub var exc_value_error: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
pub var exc_type_error: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
pub var exc_overflow_error: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
pub var exc_not_implemented_error: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
pub var exc_os_error: *Object = undefined;
// SAFETY: module exec initializes this cache before registering any code that reads it.
pub var exc_process_lookup_error: *Object = undefined;

/// Must run before anything else touches this module; `exec` calls it first.
/// Re-executing overwrites the previous round's references rather than keeping
/// them - like every cached global in this module. Skipping the re-fetch would
/// hold pointers into a finalized interpreter across Py_Finalize/Py_Initialize,
/// which is a crash; overwriting costs a bounded leak on a same-interpreter
/// re-import, which is what CPython itself does with its static caches.
pub fn initConstants() Error!void {
    none_object = c.Py_GetConstantBorrowed(c.Py_CONSTANT_NONE) orelse return Error.Python;
    true_object = c.Py_GetConstantBorrowed(c.Py_CONSTANT_TRUE) orelse return Error.Python;
    false_object = c.Py_GetConstantBorrowed(c.Py_CONSTANT_FALSE) orelse return Error.Python;
    const builtins = c.PyImport_ImportModule("builtins") orelse return Error.Python;
    defer decref(builtins);
    const wanted = .{
        .{ "SystemExit", &exc_system_exit },
        .{ "KeyboardInterrupt", &exc_keyboard_interrupt },
        .{ "RuntimeError", &exc_runtime_error },
        .{ "ValueError", &exc_value_error },
        .{ "TypeError", &exc_type_error },
        .{ "OverflowError", &exc_overflow_error },
        .{ "NotImplementedError", &exc_not_implemented_error },
        .{ "OSError", &exc_os_error },
        .{ "ProcessLookupError", &exc_process_lookup_error },
    };
    inline for (wanted) |entry| {
        entry[1].* = c.PyObject_GetAttrString(builtins, entry[0]) orelse return Error.Python;
    }
}

pub inline fn incref(o: anytype) void {
    if (@hasDecl(c, "Py_GIL_DISABLED")) c.zuvloop_Py_INCREF(@ptrCast(o)) else c.Py_INCREF(@ptrCast(o));
}

pub inline fn decref(o: anytype) void {
    if (@hasDecl(c, "Py_GIL_DISABLED")) c.zuvloop_Py_DECREF(@ptrCast(o)) else c.Py_DECREF(@ptrCast(o));
}

pub inline fn xdecref(o: ?*Object) void {
    if (o) |p| decref(p);
}

pub inline fn clear(slot: *?*Object) void {
    const old = slot.*;
    slot.* = null;
    if (old) |p| decref(p);
}

pub inline fn newref(o: ?*Object) ?*Object {
    if (o) |p| incref(p);
    return o;
}

pub inline fn none() *Object {
    return none_object;
}

pub inline fn noneRef() *Object {
    const n = none();
    incref(n);
    return n;
}

pub inline fn isNone(o: ?*Object) bool {
    return o == @as(?*Object, none());
}

pub inline fn boolRef(v: bool) *Object {
    const o: *Object = if (v) true_object else false_object;
    incref(o);
    return o;
}

pub inline fn typeOf(o: *Object) *c.PyTypeObject {
    return @ptrCast(o.ob_type);
}

pub inline fn isBytes(o: *Object) bool {
    const result = if (@hasDecl(c, "Py_GIL_DISABLED")) c.zuvloop_PyBytes_Check(o) else c.PyBytes_Check(o);
    return result != 0;
}

pub inline fn isExactBytes(o: *Object) bool {
    const result = if (@hasDecl(c, "Py_GIL_DISABLED")) c.zuvloop_PyBytes_CheckExact(o) else c.PyBytes_CheckExact(o);
    return result != 0;
}

pub inline fn isLong(o: *Object) bool {
    const result = if (@hasDecl(c, "Py_GIL_DISABLED")) c.zuvloop_PyLong_Check(o) else c.PyLong_Check(o);
    return result != 0;
}

pub inline fn isTuple(o: *Object) bool {
    const result = if (@hasDecl(c, "Py_GIL_DISABLED")) c.zuvloop_PyTuple_Check(o) else c.PyTuple_Check(o);
    return result != 0;
}

pub inline fn isUnicode(o: *Object) bool {
    const result = if (@hasDecl(c, "Py_GIL_DISABLED")) c.zuvloop_PyUnicode_Check(o) else c.PyUnicode_Check(o);
    return result != 0;
}

pub fn attr(o: *Object, name: [*:0]const u8) ?*Object {
    return c.PyObject_GetAttrString(o, name);
}

/// Imports `module` and returns its `name` attribute.
pub fn importFrom(module: [*:0]const u8, name: [*:0]const u8) ?*Object {
    const m = c.PyImport_ImportModule(module) orelse return null;
    defer decref(m);
    return c.PyObject_GetAttrString(m, name);
}

pub fn call0(callable: *Object) ?*Object {
    return c.PyObject_CallNoArgs(callable);
}

pub fn call1(callable: *Object, arg: *Object) ?*Object {
    return c.PyObject_CallOneArg(callable, arg);
}

pub fn callMethod0(o: *Object, name: *Object) ?*Object {
    return c.PyObject_CallMethodNoArgs(o, name);
}

pub fn callMethod1(o: *Object, name: *Object, arg: *Object) ?*Object {
    return c.PyObject_CallMethodOneArg(o, name, arg);
}

pub fn intern(comptime s: [:0]const u8) ?*Object {
    return c.PyUnicode_InternFromString(s.ptr);
}

pub fn str(s: []const u8) ?*Object {
    return c.PyUnicode_FromStringAndSize(s.ptr, @intCast(s.len));
}

pub fn strZ(s: [*:0]const u8) ?*Object {
    return c.PyUnicode_FromString(s);
}

pub fn bytes(b: []const u8) ?*Object {
    return c.PyBytes_FromStringAndSize(b.ptr, @intCast(b.len));
}

pub fn int(v: anytype) ?*Object {
    return switch (@typeInfo(@TypeOf(v))) {
        .int => |i| if (i.signedness == .signed)
            c.PyLong_FromLongLong(@intCast(v))
        else
            c.PyLong_FromUnsignedLongLong(@intCast(v)),
        else => @compileError("py.int expects an integer"),
    };
}

pub fn float(v: f64) ?*Object {
    return c.PyFloat_FromDouble(v);
}

/// Errors used to unwind Zig frames; the CPython error indicator carries the detail.
pub const Error = error{Python};

pub inline fn raise(comptime T: type) Error {
    _ = T;
    return Error.Python;
}

pub fn err(exc: *Object, comptime msg: [:0]const u8) Error {
    c.PyErr_SetString(exc, msg.ptr);
    return Error.Python;
}

pub fn errRuntime(comptime msg: [:0]const u8) Error {
    return err(exc_runtime_error, msg);
}

pub fn errValue(comptime msg: [:0]const u8) Error {
    return err(exc_value_error, msg);
}

pub fn errType(comptime msg: [:0]const u8) Error {
    return err(exc_type_error, msg);
}

pub fn errOverflow(comptime msg: [:0]const u8) Error {
    return err(exc_overflow_error, msg);
}

pub fn errNotImplemented(comptime msg: [:0]const u8) Error {
    return err(exc_not_implemented_error, msg);
}

pub fn errNoMemory() Error {
    _ = c.PyErr_NoMemory();
    return Error.Python;
}

/// Raises the OSError subclass matching a libuv status code.
pub fn errUv(status: c_int) Error {
    var buf: [128]u8 = undefined;
    const msg = uv.strerror(status, &buf);
    const args = c.Py_BuildValue("is", uv.toErrno(status), msg.ptr) orelse return Error.Python;
    defer decref(args);
    c.PyErr_SetObject(exc_os_error, args);
    return Error.Python;
}

pub fn errUvIfNeg(status: c_int) Error!void {
    if (status < 0) return errUv(status);
}

/// Wraps a Zig-native entry point so `Error.Python` becomes a NULL return.
pub fn wrap(comptime f: anytype) fn (?*Object, [*c]?*Object, c.Py_ssize_t) callconv(.c) ?*Object {
    const Inner = struct {
        fn call(self: ?*Object, args: [*c]?*Object, nargs: c.Py_ssize_t) callconv(.c) ?*Object {
            // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
            var critical_section: CriticalSection = undefined;
            beginCriticalSection(&critical_section, self.?);
            defer endCriticalSection(&critical_section);
            return f(self.?, args[0..@intCast(nargs)]) catch |e| switch (e) {
                Error.Python => null,
            };
        }
    };
    return Inner.call;
}

/// Vectorcall passes keyword values after the positional ones, so the callee
/// receives the full array plus the positional count.
pub fn wrapKw(comptime f: anytype) fn (?*Object, [*c]?*Object, c.Py_ssize_t, ?*Object) callconv(.c) ?*Object {
    const Inner = struct {
        fn call(self: ?*Object, args: [*c]?*Object, nargs: c.Py_ssize_t, kwnames: ?*Object) callconv(.c) ?*Object {
            // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
            var critical_section: CriticalSection = undefined;
            beginCriticalSection(&critical_section, self.?);
            defer endCriticalSection(&critical_section);
            const nkw: usize = if (kwnames) |names| @intCast(c.PyTuple_Size(names)) else 0;
            const total: usize = @as(usize, @intCast(nargs)) + nkw;
            return f(self.?, args[0..total], @intCast(nargs), kwnames) catch |e| switch (e) {
                Error.Python => null,
            };
        }
    };
    return Inner.call;
}

/// Wraps a keyword method that does not require wrapper-level synchronization.
pub fn wrapKwUnlocked(comptime f: anytype) fn (?*Object, [*c]?*Object, c.Py_ssize_t, ?*Object) callconv(.c) ?*Object {
    const Inner = struct {
        fn call(self: ?*Object, args: [*c]?*Object, nargs: c.Py_ssize_t, kwnames: ?*Object) callconv(.c) ?*Object {
            const nkw: usize = if (kwnames) |names| @intCast(c.PyTuple_Size(names)) else 0;
            const total: usize = @as(usize, @intCast(nargs)) + nkw;
            return f(self.?, args[0..total], @intCast(nargs), kwnames) catch |e| switch (e) {
                Error.Python => null,
            };
        }
    };
    return Inner.call;
}

pub fn wrapNoArgs(comptime f: anytype) fn (?*Object, ?*Object) callconv(.c) ?*Object {
    const Inner = struct {
        fn call(self: ?*Object, _: ?*Object) callconv(.c) ?*Object {
            // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
            var critical_section: CriticalSection = undefined;
            beginCriticalSection(&critical_section, self.?);
            defer endCriticalSection(&critical_section);
            return f(self.?) catch |e| switch (e) {
                Error.Python => null,
            };
        }
    };
    return Inner.call;
}

/// Wraps a no-argument method that does not require wrapper-level synchronization.
pub fn wrapNoArgsUnlocked(comptime f: anytype) fn (?*Object, ?*Object) callconv(.c) ?*Object {
    const Inner = struct {
        fn call(self: ?*Object, _: ?*Object) callconv(.c) ?*Object {
            return f(self.?) catch |e| switch (e) {
                Error.Python => null,
            };
        }
    };
    return Inner.call;
}

pub fn wrapO(comptime f: anytype) fn (?*Object, ?*Object) callconv(.c) ?*Object {
    const Inner = struct {
        fn call(self: ?*Object, arg: ?*Object) callconv(.c) ?*Object {
            // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
            var critical_section: CriticalSection = undefined;
            beginCriticalSection(&critical_section, self.?);
            defer endCriticalSection(&critical_section);
            return f(self.?, arg.?) catch |e| switch (e) {
                Error.Python => null,
            };
        }
    };
    return Inner.call;
}

pub fn wrapDealloc(comptime f: anytype) fn (?*Object) callconv(.c) void {
    const Inner = struct {
        fn call(self: ?*Object) callconv(.c) void {
            f(self);
        }
    };
    return Inner.call;
}

pub fn wrapTraverse(comptime f: anytype) fn (?*Object, c.visitproc, ?*anyopaque) callconv(.c) c_int {
    const Inner = struct {
        fn call(self: ?*Object, visitproc: c.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
            // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
            var critical_section: CriticalSection = undefined;
            beginCriticalSection(&critical_section, self.?);
            defer endCriticalSection(&critical_section);
            return f(self, visitproc, arg);
        }
    };
    return Inner.call;
}

pub fn wrapClear(comptime f: anytype) fn (?*Object) callconv(.c) c_int {
    const Inner = struct {
        fn call(self: ?*Object) callconv(.c) c_int {
            // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
            var critical_section: CriticalSection = undefined;
            beginCriticalSection(&critical_section, self.?);
            defer endCriticalSection(&critical_section);
            return f(self);
        }
    };
    return Inner.call;
}

pub fn wrapRepr(comptime f: anytype) fn (?*Object) callconv(.c) ?*Object {
    const Inner = struct {
        fn call(self: ?*Object) callconv(.c) ?*Object {
            // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
            var critical_section: CriticalSection = undefined;
            beginCriticalSection(&critical_section, self.?);
            defer endCriticalSection(&critical_section);
            return f(self);
        }
    };
    return Inner.call;
}

pub fn wrapGet(comptime f: anytype) fn (?*Object, ?*anyopaque) callconv(.c) ?*Object {
    const Inner = struct {
        fn call(self: ?*Object, closure: ?*anyopaque) callconv(.c) ?*Object {
            // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
            var critical_section: CriticalSection = undefined;
            beginCriticalSection(&critical_section, self.?);
            defer endCriticalSection(&critical_section);
            return f(self, closure);
        }
    };
    return Inner.call;
}

pub fn wrapSet(comptime f: anytype) fn (?*Object, ?*Object, ?*anyopaque) callconv(.c) c_int {
    const Inner = struct {
        fn call(self: ?*Object, value: ?*Object, closure: ?*anyopaque) callconv(.c) c_int {
            // SAFETY: PyCriticalSection_Begin initializes this storage before reading it.
            var critical_section: CriticalSection = undefined;
            beginCriticalSection(&critical_section, self.?);
            defer endCriticalSection(&critical_section);
            return f(self, value, closure);
        }
    };
    return Inner.call;
}

pub fn method(comptime name: [:0]const u8, comptime f: anytype, comptime doc: ?[*:0]const u8) c.PyMethodDef {
    return .{
        .ml_name = name.ptr,
        .ml_meth = @ptrCast(&wrap(f)),
        .ml_flags = c.METH_FASTCALL,
        .ml_doc = doc,
    };
}

pub fn methodKw(comptime name: [:0]const u8, comptime f: anytype, comptime doc: ?[*:0]const u8) c.PyMethodDef {
    return .{
        .ml_name = name.ptr,
        .ml_meth = @ptrCast(&wrapKw(f)),
        .ml_flags = c.METH_FASTCALL | c.METH_KEYWORDS,
        .ml_doc = doc,
    };
}

/// Defines a keyword method that does not require wrapper-level synchronization.
pub fn methodKwUnlocked(comptime name: [:0]const u8, comptime f: anytype, comptime doc: ?[*:0]const u8) c.PyMethodDef {
    return .{
        .ml_name = name.ptr,
        .ml_meth = @ptrCast(&wrapKwUnlocked(f)),
        .ml_flags = c.METH_FASTCALL | c.METH_KEYWORDS,
        .ml_doc = doc,
    };
}

pub fn methodNoArgs(comptime name: [:0]const u8, comptime f: anytype, comptime doc: ?[*:0]const u8) c.PyMethodDef {
    return .{
        .ml_name = name.ptr,
        .ml_meth = @ptrCast(&wrapNoArgs(f)),
        .ml_flags = c.METH_NOARGS,
        .ml_doc = doc,
    };
}

/// Defines a no-argument method that does not require wrapper-level synchronization.
pub fn methodNoArgsUnlocked(
    comptime name: [:0]const u8,
    comptime f: anytype,
    comptime doc: ?[*:0]const u8,
) c.PyMethodDef {
    return .{
        .ml_name = name.ptr,
        .ml_meth = @ptrCast(&wrapNoArgsUnlocked(f)),
        .ml_flags = c.METH_NOARGS,
        .ml_doc = doc,
    };
}

pub fn methodO(comptime name: [:0]const u8, comptime f: anytype, comptime doc: ?[*:0]const u8) c.PyMethodDef {
    return .{
        .ml_name = name.ptr,
        .ml_meth = @ptrCast(&wrapO(f)),
        .ml_flags = c.METH_O,
        .ml_doc = doc,
    };
}

pub const sentinel = c.PyMethodDef{ .ml_name = null, .ml_meth = null, .ml_flags = 0, .ml_doc = null };

pub fn expectArgs(args: []const ?*Object, comptime n: usize, comptime what: [:0]const u8) Error!void {
    if (args.len != n) return errType(what ++ "() takes exactly " ++ std.fmt.comptimePrint("{d}", .{n}) ++ " arguments");
}

pub fn asF64(o: *Object) Error!f64 {
    const v = c.PyFloat_AsDouble(o);
    if (v == -1.0 and c.PyErr_Occurred() != null) return Error.Python;
    return v;
}

pub fn asIsize(o: *Object) Error!c.Py_ssize_t {
    const v = c.PyNumber_AsSsize_t(o, exc_overflow_error);
    if (v == -1 and c.PyErr_Occurred() != null) return Error.Python;
    return v;
}

/// Like `asIsize`, but a value too wide saturates rather than raising, so a
/// caller with its own range to enforce reports that range instead of the width.
pub fn asIsizeClamped(o: *Object) Error!c.Py_ssize_t {
    const v = c.PyNumber_AsSsize_t(o, null);
    if (v == -1 and c.PyErr_Occurred() != null) return Error.Python;
    return v;
}

pub fn asCInt(o: *Object) Error!c_int {
    const v = c.PyLong_AsLong(o);
    if (v == -1 and c.PyErr_Occurred() != null) return Error.Python;
    if (v > std.math.maxInt(c_int) or v < std.math.minInt(c_int)) return errValue("integer out of range");
    return @intCast(v);
}

pub fn isTrue(o: *Object) Error!bool {
    const v = c.PyObject_IsTrue(o);
    if (v < 0) return Error.Python;
    return v != 0;
}

/// Extracts a file descriptor from an int or an object with `fileno()`.
pub fn asFd(o: *Object) Error!c_int {
    const fd = c.PyObject_AsFileDescriptor(o);
    if (fd < 0) return Error.Python;
    return fd;
}

/// `Py_VISIT` is a macro translate-c cannot render; this is the same contract.
pub inline fn visit(o: ?*Object, visitproc: c.visitproc, arg: ?*anyopaque) c_int {
    if (o) |p| return visitproc.?(p, arg);
    return 0;
}

pub fn writeUnraisable(context: ?*Object) void {
    c.PyErr_WriteUnraisable(context orelse none());
}
