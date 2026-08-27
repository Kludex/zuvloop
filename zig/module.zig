const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const uv = @import("uv.zig");
const loop = @import("loop.zig");
const datagram = @import("datagram.zig");
const process = @import("process.zig");
const handle = @import("handle.zig");
const tshandle = @import("tshandle.zig");
const timer = @import("timer.zig");

fn libuvVersion(_: ?*py.Object, _: ?*py.Object) callconv(.c) ?*py.Object {
    return py.strZ(uv.uv_version_string());
}

var methods = [_]c.PyMethodDef{
    .{ .ml_name = "libuv_version", .ml_meth = @ptrCast(&libuvVersion), .ml_flags = c.METH_NOARGS, .ml_doc = "Return the linked libuv version." },
    py.sentinel,
};

fn exec(module: ?*py.Object) callconv(.c) c_int {
    const m = module orelse return -1;
    py.initConstants() catch return -1;
    handle.register(m) catch return -1;
    tshandle.register(m) catch return -1;
    timer.register(m) catch return -1;
    loop.register(m) catch return -1;
    datagram.register(m) catch return -1;
    process.register(m) catch return -1;
    return 0;
}

var slots = [_]c.PyModuleDef_Slot{
    .{ .slot = c.Py_mod_exec, .value = @ptrCast(@constCast(&exec)) },
    .{ .slot = c.Py_mod_multiple_interpreters, .value = c.Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED },
    .{ .slot = c.Py_mod_gil, .value = c.Py_MOD_GIL_NOT_USED },
    .{ .slot = 0, .value = null },
};

var moddef = c.PyModuleDef{
    .m_base = std.mem.zeroes(c.PyModuleDef_Base),
    .m_name = "zuvloop._zuvloop",
    .m_doc = "libuv bindings backing the zuvloop event loop.",
    .m_size = 0,
    .m_methods = &methods,
    .m_slots = &slots,
};

export fn PyInit__zuvloop() ?*py.Object {
    return c.PyModuleDef_Init(&moddef);
}
