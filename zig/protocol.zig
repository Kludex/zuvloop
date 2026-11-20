//! What the stream and datagram transports share: how protocol callbacks are
//! cached, called and scheduled, how failures reach the loop, and how the
//! flow-control marks and `get_extra_info` are answered.
//!
//! The transports keep what differs between them: their read paths, their
//! write queues and their teardown.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const handlemod = @import("handle.zig");
const loopmod = @import("loop.zig");

pub const default_high_water: usize = 64 * 1024;

var str_high: ?*py.Object = null;
var str_low: ?*py.Object = null;

/// The flow-control marks `set_write_buffer_limits` resolves to.
pub const WaterMarks = struct {
    high: usize,
    low: usize,
};

/// Caches `protocol.<name>` as a bound method, leaving the slot empty when the
/// protocol does not define it.
pub fn cacheCallback(protocol: *py.Object, name: ?*py.Object, slot: *?*py.Object) void {
    py.clear(slot);
    slot.* = c.PyObject_GetAttr(protocol, name);
    if (slot.* == null) c.PyErr_Clear();
}

/// Hands the raised exception to the loop's exception handler, naming `owner`
/// as the transport it came from.
pub fn reportError(loop: *py.Object, owner: *py.Object, comptime message: [:0]const u8) void {
    const exc = c.PyErr_GetRaisedException() orelse return;
    defer py.decref(exc);
    loopmod.callExceptionHandler(loopmod.asLoop(loop), message, exc, owner);
}

/// Turns a libuv status into the exception asyncio would have raised, so it can
/// be handed to the protocol instead.
pub fn takeUvError(status: c_int) ?*py.Object {
    switch (py.errUv(status)) {
        error.Python => {},
    }
    return c.PyErr_GetRaisedException();
}

/// Calls `callback(arg)` in whichever context is already active, which is how
/// asyncio invokes the callbacks it makes synchronously from a write.
pub fn call(loop: *py.Object, owner: *py.Object, callback: ?*py.Object, arg: ?*py.Object) void {
    const cb = callback orelse return;
    const result = if (arg) |a| c.PyObject_CallOneArg(cb, a) else c.PyObject_CallNoArgs(cb);
    if (result) |r| py.decref(r) else reportError(loop, owner, "Error in protocol callback");
}

/// Queues `callback(arg)` on the loop rather than reentering protocol code
/// from inside a libuv callback.
pub fn scheduleCall(loop: *py.Object, context: ?*py.Object, callback: ?*py.Object, arg: ?*py.Object) void {
    const cb = callback orelse return;
    const self = loopmod.asLoop(loop);
    const st = self.state();
    if (st.closed) return;
    var argv: [1]?*py.Object = .{arg};
    const n: usize = if (arg == null) 0 else 1;
    const h = handlemod.create(handlemod.handle_type.?, loop, cb, argv[0..n], context) catch {
        loopmod.captureFatal(self);
        return;
    };
    st.ready.push(@ptrCast(h)) catch {
        py.decref(h);
        py.errNoMemory() catch {};
        loopmod.captureFatal(self);
        return;
    };
    loopmod.startIdle(st);
}

/// Disarms the Python object mirroring a descriptor libuv owns, by calling
/// `method` on it. This has to happen before libuv closes the number, or the
/// object could close it again after another thread has reused it.
pub fn releaseSocketView(slot: *?*py.Object, method: ?*py.Object) void {
    const view = slot.* orelse return;
    slot.* = null;
    const result = c.PyObject_CallMethodNoArgs(view, method);
    if (result) |r| py.decref(r) else c.PyErr_Clear();
    py.decref(view);
}

/// `get_extra_info(name, default=None)` over a transport's `extra` dict.
pub fn extraInfo(extra: ?*py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    if (args.len < 1 or args.len > 2) return py.errType("get_extra_info() takes 1 or 2 arguments");
    const fallback: *py.Object = if (args.len == 2) args[1].? else py.none();
    const dict = extra orelse return py.newref(fallback).?;
    if (py.isNone(dict)) return py.newref(fallback).?;
    const found = c.PyDict_GetItemWithError(dict, args[0].?) orelse {
        if (c.PyErr_Occurred() != null) return py.Error.Python;
        return py.newref(fallback).?;
    };
    return py.newref(found).?;
}

/// Parses `set_write_buffer_limits(high=None, low=None)`, matching asyncio: an
/// omitted low mark defaults to a quarter of the high mark, and an omitted high
/// mark to four times the low one.
pub fn parseWriteBufferLimits(args: []const ?*py.Object, nargs: usize, kwnames: ?*py.Object) py.Error!WaterMarks {
    if (nargs > 2) return py.errType("set_write_buffer_limits() takes at most 2 arguments");
    var high: ?*py.Object = if (nargs > 0) args[0] else null;
    var low: ?*py.Object = if (nargs > 1) args[1] else null;
    if (kwnames) |names| {
        const n: usize = @intCast(c.PyTuple_Size(names));
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const key = c.PyTuple_GetItem(names, @intCast(i)) orelse return py.Error.Python;
            if (c.PyObject_RichCompareBool(key, str_high, c.Py_EQ) == 1) {
                high = args[nargs + i];
            } else if (c.PyObject_RichCompareBool(key, str_low, c.Py_EQ) == 1) {
                low = args[nargs + i];
            } else {
                return py.errType("set_write_buffer_limits() got an unexpected keyword argument");
            }
        }
    }

    var marks: WaterMarks = .{ .high = default_high_water, .low = 0 };
    if (high) |value| {
        if (!py.isNone(value)) {
            const parsed = try py.asIsize(value);
            if (parsed < 0) return py.errValue("high water mark must be non-negative");
            marks.high = @intCast(parsed);
        }
    }
    marks.low = marks.high / 4;
    if (low) |value| {
        if (!py.isNone(value)) {
            const parsed = try py.asIsize(value);
            if (parsed < 0) return py.errValue("low water mark must be non-negative");
            marks.low = @intCast(parsed);
        }
    }
    if (high == null or py.isNone(high.?)) {
        if (low) |value| {
            if (!py.isNone(value)) {
                if (marks.low > std.math.maxInt(c.Py_ssize_t) / 4) {
                    return py.errOverflow("high water mark is too large");
                }
                marks.high = marks.low * 4;
            }
        }
    }
    if (marks.low > marks.high) return py.errValue("high water mark must be >= low water mark");
    return marks;
}

pub fn register() py.Error!void {
    str_high = py.intern("high") orelse return py.Error.Python;
    str_low = py.intern("low") orelse return py.Error.Python;
}
