//! Native process spawning over `uv_process_t`.
//!
//! libuv reaps the child itself and reports the status through `exit_cb`, so
//! there is no watcher thread and no pidfd to poll. The descriptors the child
//! inherits are created in Python; this only forks and execs.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const uv = @import("uv.zig");
const handlemod = @import("handle.zig");
const loopmod = @import("loop.zig");

const alloc = std.heap.c_allocator;

const OPEN: u32 = 1 << 0;
const EXITED: u32 = 1 << 1;

pub var process_type: ?*c.PyTypeObject = null;

pub const Process = extern struct {
    ob_base: c.PyObject,
    loop: ?*py.Object,
    state: ?*loopmod.State,
    on_exit: ?*py.Object,
    context: ?*py.Object,
    returncode: c_int,
    flags: u32,

    inline fn handle(self: *Process) *uv.Process {
        return @ptrCast(@as([*]u8, @ptrCast(self)) + handle_offset);
    }

    inline fn loopState(self: *Process) *loopmod.State {
        return self.state.?;
    }
};

var handle_offset: usize = 0;

/// Everything `uv_spawn` reads, kept alive for exactly the duration of the call.
/// libuv copies what it needs, so this is freed as soon as `uv_spawn` returns.
const SpawnArgs = struct {
    storage: std.ArrayListUnmanaged(u8) = .empty,
    argv: std.ArrayListUnmanaged(?[*:0]const u8) = .empty,
    envp: std.ArrayListUnmanaged(?[*:0]const u8) = .empty,
    /// Where each argument and each environment entry starts in `storage`.
    /// Offsets rather than pointers, because appending can move the buffer.
    argv_at: std.ArrayListUnmanaged(usize) = .empty,
    envp_at: std.ArrayListUnmanaged(usize) = .empty,
    stdio: [3]uv.StdioContainer = @splat(.{ .flags = uv.StdioFlags.ignore, .data = .{ .fd = -1 } }),

    fn deinit(self: *SpawnArgs) void {
        self.storage.deinit(alloc);
        self.argv.deinit(alloc);
        self.envp.deinit(alloc);
        self.argv_at.deinit(alloc);
        self.envp_at.deinit(alloc);
    }

    /// Turns the recorded offsets into the null-terminated pointer array
    /// `uv_spawn` wants. Called once every string is in place, so the buffer is
    /// done moving and the pointers cannot be invalidated afterwards.
    fn resolve(self: *SpawnArgs, list: *std.ArrayListUnmanaged(?[*:0]const u8), offsets: []const usize) py.Error!void {
        list.ensureTotalCapacity(alloc, offsets.len + 1) catch return py.errNoMemory();
        for (offsets) |offset| list.appendAssumeCapacity(@ptrCast(self.storage.items.ptr + offset));
        list.appendAssumeCapacity(null);
    }

    fn at(self: *SpawnArgs, offset: usize) [*:0]const u8 {
        return @ptrCast(self.storage.items.ptr + offset);
    }

    /// Copies a string into the arena and returns where it starts.
    ///
    /// An offset rather than a pointer: the arena is reserved up front, but
    /// nothing enforces that reservation, and one append past it would move
    /// every string already copied - leaving `uv_spawn` reading freed memory.
    fn dup(self: *SpawnArgs, text: []const u8) py.Error!usize {
        const start = self.storage.items.len;
        self.storage.appendSlice(alloc, text) catch return py.errNoMemory();
        self.storage.append(alloc, 0) catch return py.errNoMemory();
        return start;
    }
};

fn reserve(sequence: *py.Object, what: [:0]const u8) py.Error!usize {
    var total: usize = 0;
    const n: usize = @intCast(c.PySequence_Size(sequence));
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const item = c.PySequence_GetItem(sequence, @intCast(i)) orelse return py.Error.Python;
        defer py.decref(item);
        var len: c.Py_ssize_t = 0;
        if (c.PyUnicode_Check(item) != 0) {
            _ = c.PyUnicode_AsUTF8AndSize(item, &len) orelse return py.Error.Python;
        } else if (c.PyBytes_Check(item) != 0) {
            len = c.PyBytes_Size(item);
        } else {
            _ = c.PyErr_Format(@ptrCast(c.PyExc_TypeError), "%s must contain str or bytes", what.ptr);
            return py.Error.Python;
        }
        total += @as(usize, @intCast(len)) + 1;
    }
    return total;
}

fn appendAll(args: *SpawnArgs, list: *std.ArrayListUnmanaged(usize), sequence: *py.Object) py.Error!void {
    const n: usize = @intCast(c.PySequence_Size(sequence));
    list.ensureTotalCapacity(alloc, n) catch return py.errNoMemory();
    var i: usize = 0;
    while (i < n) : (i += 1) {
        const item = c.PySequence_GetItem(sequence, @intCast(i)) orelse return py.Error.Python;
        defer py.decref(item);
        var len: c.Py_ssize_t = 0;
        const text: [*c]const u8 = if (c.PyUnicode_Check(item) != 0)
            c.PyUnicode_AsUTF8AndSize(item, &len) orelse return py.Error.Python
        else
            c.PyBytes_AsString(item);
        if (c.PyBytes_Check(item) != 0) len = c.PyBytes_Size(item);
        list.appendAssumeCapacity(try args.dup(text[0..@intCast(len)]));
    }
}

// ---------------------------------------------------------------------------
// libuv callbacks

fn onExit(handle: ?*uv.Process, status: i64, signal: c_int) callconv(.c) void {
    const self: *Process = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();

    // asyncio reports a signalled child as a negative signal number, the same
    // convention `subprocess` uses.
    self.returncode = if (signal != 0) -signal else @intCast(status);
    self.flags |= EXITED;

    if (self.on_exit) |callback| {
        const code = py.int(@as(c.Py_ssize_t, self.returncode)) orelse {
            c.PyErr_Clear();
            closeHandle(self);
            return;
        };
        defer py.decref(code);
        var argv = [_]?*py.Object{code};
        const h = handlemod.create(handlemod.handle_type.?, self.loop.?, callback, argv[0..1], self.context) catch {
            c.PyErr_Clear();
            closeHandle(self);
            return;
        };
        st.ready.push(@ptrCast(h)) catch py.decref(h);
        loopmod.startIdle(st);
    }
    closeHandle(self);
}

fn onClosed(handle: ?*uv.Handle) callconv(.c) void {
    const self: *Process = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loopState();
    st.gilEnter();
    defer st.gilExit();
    py.decref(self);
}

fn closeHandle(self: *Process) void {
    if (self.flags & OPEN == 0) return;
    self.flags &= ~OPEN;
    uv.uv_close(uv.asHandle(self.handle()), onClosed);
}

/// Closes a process handle discovered while the owning loop shuts down.
pub fn closeFromLoop(handle: *uv.Handle) void {
    const self: *Process = @ptrCast(@alignCast(uv.getData(handle)));
    self.flags &= ~OPEN;
    uv.uv_close(handle, onClosed);
}

// ---------------------------------------------------------------------------
// python methods

fn asProcess(obj: *py.Object) *Process {
    return @ptrCast(@alignCast(obj));
}

fn getPid(self_obj: *py.Object) py.Error!*py.Object {
    const self = asProcess(self_obj);
    return py.int(@as(c.Py_ssize_t, uv.uv_process_get_pid(self.handle()))) orelse py.Error.Python;
}

fn getReturncode(self_obj: *py.Object) py.Error!*py.Object {
    const self = asProcess(self_obj);
    if (self.flags & EXITED == 0) return py.noneRef();
    return py.int(@as(c.Py_ssize_t, self.returncode)) orelse py.Error.Python;
}

fn sendSignal(self_obj: *py.Object, signum: *py.Object) py.Error!*py.Object {
    const self = asProcess(self_obj);
    if (self.flags & EXITED != 0) {
        c.PyErr_SetString(@ptrCast(c.PyExc_ProcessLookupError), "process already exited");
        return py.Error.Python;
    }
    try py.errUvIfNeg(uv.uv_process_kill(self.handle(), try py.asCInt(signum)));
    return py.noneRef();
}

// ---------------------------------------------------------------------------
// spawning

fn optionalString(args: *SpawnArgs, value: ?*py.Object) py.Error!?usize {
    const obj = value orelse return null;
    if (py.isNone(obj)) return null;
    var len: c.Py_ssize_t = 0;
    const text = c.PyUnicode_AsUTF8AndSize(obj, &len) orelse return py.Error.Python;
    return try args.dup(text[0..@intCast(len)]);
}

/// `loop._spawn_process(file, args, env, cwd, stdio, flags, uid, gid, on_exit)`
///
/// `stdio` is a three-element sequence of descriptors for the child's stdin,
/// stdout and stderr; -1 leaves one closed. They are created in Python, which is
/// where the rest of the socket and pipe setup lives.
pub fn spawnProcess(self_obj: *py.Object, args_in: []const ?*py.Object) py.Error!*py.Object {
    try py.expectArgs(args_in, 9, "_spawn_process");
    const loop = loopmod.asLoop(self_obj);
    const st = loop.state();
    try loopmod.checkClosed(st);

    var spawn: SpawnArgs = .{};
    defer spawn.deinit();

    // Size the arena before copying so no string can move afterwards.
    var needed = try reserve(args_in[1].?, "args");
    if (!py.isNone(args_in[2].?)) needed += try reserve(args_in[2].?, "env");
    needed = 0; // STRESS: reserve nothing, so every append reallocates
    spawn.storage.ensureTotalCapacity(alloc, needed) catch return py.errNoMemory();

    const file_at = try optionalString(&spawn, args_in[0]) orelse return py.errValue("file is required");
    try appendAll(&spawn, &spawn.argv_at, args_in[1].?);
    if (!py.isNone(args_in[2].?)) try appendAll(&spawn, &spawn.envp_at, args_in[2].?);
    const cwd_at = try optionalString(&spawn, args_in[3]);

    // Every string is in the arena now, so it will not move again.
    try spawn.resolve(&spawn.argv, spawn.argv_at.items);
    if (spawn.envp_at.items.len != 0) try spawn.resolve(&spawn.envp, spawn.envp_at.items);
    const file = spawn.at(file_at);
    const cwd: ?[*:0]const u8 = if (cwd_at) |offset| spawn.at(offset) else null;

    var i: usize = 0;
    while (i < 3) : (i += 1) {
        const item = c.PySequence_GetItem(args_in[4].?, @intCast(i)) orelse return py.Error.Python;
        defer py.decref(item);
        const fd = try py.asCInt(item);
        spawn.stdio[i] = if (fd < 0)
            .{ .flags = uv.StdioFlags.ignore, .data = .{ .fd = -1 } }
        else
            .{ .flags = uv.StdioFlags.inherit_fd, .data = .{ .fd = fd } };
    }

    const obj = c.PyType_GenericAlloc(process_type, 0) orelse return py.Error.Python;
    const self = asProcess(obj);
    errdefer py.decref(obj);

    self.state = st;
    self.returncode = 0;
    self.context = c.PyContext_CopyCurrent();
    if (self.context == null) return py.Error.Python;
    if (!py.isNone(args_in[8].?)) {
        py.incref(args_in[8].?);
        self.on_exit = args_in[8];
    }
    st.ready.ensureUnusedCapacity(1) catch return py.errNoMemory();

    var options: uv.ProcessOptions = std.mem.zeroes(uv.ProcessOptions);
    options.exit_cb = onExit;
    options.file = file;
    options.args = spawn.argv.items.ptr;
    options.env = if (spawn.envp.items.len != 0) spawn.envp.items.ptr else null;
    options.cwd = cwd;
    options.flags = @intCast(try py.asCInt(args_in[5].?));
    options.stdio_count = 3;
    options.stdio = &spawn.stdio;
    options.uid = @intCast(try py.asCInt(args_in[6].?));
    options.gid = @intCast(try py.asCInt(args_in[7].?));

    uv.setData(self.handle(), self);
    const status = uv.uv_spawn(st.uvloop, self.handle(), &options);
    if (status < 0) {
        // `uv_spawn` links the handle into the loop before anything in it can
        // fail, and only `uv_close` unlinks it. Freeing the object here would
        // leave the loop holding a queue node inside it.
        py.incref(obj); // released by the close callback
        uv.uv_close(uv.asHandle(self.handle()), onClosed);
        return py.errUv(status);
    }

    self.flags |= OPEN;
    py.incref(obj); // released by the close callback
    py.incref(self_obj);
    self.loop = self_obj;
    return obj;
}

// ---------------------------------------------------------------------------
// type

fn dealloc(obj: ?*py.Object) callconv(.c) void {
    const self = asProcess(obj.?);
    const tp = py.typeOf(obj.?);
    c.PyObject_GC_UnTrack(obj);
    c.PyObject_ClearWeakRefs(obj);
    py.clear(&self.loop);
    py.clear(&self.on_exit);
    py.clear(&self.context);
    tp.tp_free.?(obj);
    py.decref(tp);
}

fn traverse(obj: ?*py.Object, visitproc: c.visitproc, arg: ?*anyopaque) callconv(.c) c_int {
    const self = asProcess(obj.?);
    for ([_]?*py.Object{ self.loop, self.on_exit, self.context }) |slot| {
        const r = py.visit(slot, visitproc, arg);
        if (r != 0) return r;
    }
    return py.visit(@ptrCast(py.typeOf(obj.?)), visitproc, arg);
}

fn clear_(obj: ?*py.Object) callconv(.c) c_int {
    const self = asProcess(obj.?);
    py.clear(&self.on_exit);
    return 0;
}

var methods = [_]c.PyMethodDef{
    py.methodNoArgs("get_pid", getPid, "Return the child's process id."),
    py.methodNoArgs("get_returncode", getReturncode, "Return the exit status, or None while it runs."),
    py.methodO("send_signal", sendSignal, "Send a signal to the child."),
    py.sentinel,
};

var slots = [_]c.PyType_Slot{
    .{ .slot = c.Py_tp_dealloc, .pfunc = @constCast(@ptrCast(&dealloc)) },
    .{ .slot = c.Py_tp_traverse, .pfunc = @constCast(@ptrCast(&traverse)) },
    .{ .slot = c.Py_tp_clear, .pfunc = @constCast(@ptrCast(&clear_)) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = c.Py_tp_doc, .pfunc = @constCast(@ptrCast("A libuv-backed child process.")) },
    .{ .slot = 0, .pfunc = null },
};

var spec = c.PyType_Spec{
    .name = "zuvloop._zuvloop.Process",
    .basicsize = 0,
    .itemsize = 0,
    .flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_HAVE_GC | c.Py_TPFLAGS_MANAGED_WEAKREF |
        c.Py_TPFLAGS_IMMUTABLETYPE | c.Py_TPFLAGS_DISALLOW_INSTANTIATION,
    .slots = &slots,
};

pub fn register(module: *py.Object) py.Error!void {
    handle_offset = std.mem.alignForward(usize, @sizeOf(Process), 16);
    spec.basicsize = @intCast(handle_offset + uv.uv_handle_size(.process));

    process_type = @ptrCast(c.PyType_FromModuleAndSpec(module, &spec, null) orelse return py.Error.Python);
    if (c.PyModule_AddObjectRef(module, "Process", @ptrCast(process_type)) < 0) return py.Error.Python;
    if (c.PyModule_AddIntConstant(module, "PROCESS_DETACHED", uv.ProcessFlags.detached) < 0) return py.Error.Python;
    if (c.PyModule_AddIntConstant(module, "PROCESS_SETUID", uv.ProcessFlags.setuid) < 0) return py.Error.Python;
    if (c.PyModule_AddIntConstant(module, "PROCESS_SETGID", uv.ProcessFlags.setgid) < 0) return py.Error.Python;
}
