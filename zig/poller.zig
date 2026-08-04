//! `add_reader`/`add_writer` support, backed by one `uv_poll_t` per descriptor.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const uv = @import("uv.zig");
const handlemod = @import("handle.zig");
const Handle = handlemod.Handle;
const loopmod = @import("loop.zig");
const LoopObject = loopmod.LoopObject;
const State = loopmod.State;

const alloc = std.heap.c_allocator;

pub const Poller = struct {
    loop: *LoopObject,
    reader: ?*Handle = null,
    writer: ?*Handle = null,
    fd: c_int,
    events: c_int = 0,

    inline fn uvPoll(self: *Poller) *uv.Poll {
        return @ptrCast(@as([*]u8, @ptrCast(self)) + poll_offset);
    }
};

const poll_offset = std.mem.alignForward(usize, @sizeOf(Poller), 8);

pub const Map = std.AutoHashMapUnmanaged(c_int, *Poller);

fn onPoll(handle: ?*uv.Poll, status: c_int, events: c_int) callconv(.c) void {
    const self: *Poller = @ptrCast(@alignCast(uv.getData(handle.?)));
    const st = self.loop.state();
    st.gilEnter();
    defer st.gilExit();
    // A polling error has no direction, so both sides are woken to let their
    // next syscall surface it.
    const fire_read = status < 0 or events & uv.READABLE != 0;
    const fire_write = status < 0 or events & uv.WRITABLE != 0;
    if (fire_read) schedule(st, self.reader);
    if (fire_write) schedule(st, self.writer);
    loopmod.startIdle(st);
}

fn schedule(st: *State, maybe: ?*Handle) void {
    const h = maybe orelse return;
    if (h.isCancelled()) return;
    py.incref(h);
    st.ready.push(@ptrCast(h)) catch py.decref(h);
}

fn onClosed(handle: ?*uv.Handle) callconv(.c) void {
    const self: *Poller = @ptrCast(@alignCast(uv.getData(handle.?)));
    alloc.free(@as([*]u8, @ptrCast(self))[0 .. poll_offset + uv.uv_handle_size(.poll)]);
}

fn destroy(self: *Poller) void {
    if (self.reader) |h| {
        h.flags |= handlemod.CANCELLED;
        py.decref(h);
        self.reader = null;
    }
    if (self.writer) |h| {
        h.flags |= handlemod.CANCELLED;
        py.decref(h);
        self.writer = null;
    }
    uv.uv_close(uv.asHandle(self.uvPoll()), onClosed);
}

fn get(st: *State, loop: *LoopObject, fd: c_int) py.Error!*Poller {
    if (st.pollers.get(fd)) |p| return p;

    const size = poll_offset + uv.uv_handle_size(.poll);
    const raw = alloc.alignedAlloc(u8, .@"8", size) catch return py.errNoMemory();
    const self: *Poller = @ptrCast(raw.ptr);
    self.* = .{ .loop = loop, .fd = fd };
    const status = uv.uv_poll_init(st.uvloop, self.uvPoll(), fd);
    if (status < 0) {
        alloc.free(raw);
        return py.errUv(status);
    }
    uv.setData(self.uvPoll(), self);
    st.pollers.put(alloc, fd, self) catch {
        uv.uv_close(uv.asHandle(self.uvPoll()), onClosed);
        return py.errNoMemory();
    };
    return self;
}

fn rearm(st: *State, self: *Poller) py.Error!void {
    var events: c_int = 0;
    if (self.reader != null) events |= uv.READABLE;
    if (self.writer != null) events |= uv.WRITABLE;
    if (events == self.events) return;
    self.events = events;
    if (events == 0) {
        _ = st.pollers.remove(self.fd);
        destroy(self);
        return;
    }
    try py.errUvIfNeg(uv.uv_poll_start(self.uvPoll(), events, onPoll));
}

fn add(self_obj: *py.Object, args: []const ?*py.Object, comptime writer: bool) py.Error!*py.Object {
    if (args.len < 2) return py.errType("a file descriptor and a callback are required");
    const loop = loopmod.asLoop(self_obj);
    const st = loop.state();
    try loopmod.checkClosed(st);
    const fd = try py.asFd(args[0].?);

    const poller = try get(st, loop, fd);
    const h = try handlemod.create(handlemod.handle_type.?, self_obj, args[1].?, args[2..], null);
    const slot = if (writer) &poller.writer else &poller.reader;
    if (slot.*) |old| {
        old.flags |= handlemod.CANCELLED;
        py.decref(old);
    }
    slot.* = h;
    try rearm(st, poller);
    return py.noneRef();
}

fn remove(self_obj: *py.Object, arg: *py.Object, comptime writer: bool) py.Error!*py.Object {
    const st = loopmod.asLoop(self_obj).state();
    const fd = try py.asFd(arg);
    const poller = st.pollers.get(fd) orelse return py.boolRef(false);
    const slot = if (writer) &poller.writer else &poller.reader;
    const h = slot.* orelse return py.boolRef(false);
    h.flags |= handlemod.CANCELLED;
    py.decref(h);
    slot.* = null;
    try rearm(st, poller);
    return py.boolRef(true);
}

pub fn addReader(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    return add(self_obj, args, false);
}

pub fn addWriter(self_obj: *py.Object, args: []const ?*py.Object) py.Error!*py.Object {
    return add(self_obj, args, true);
}

pub fn removeReader(self_obj: *py.Object, arg: *py.Object) py.Error!*py.Object {
    return remove(self_obj, arg, false);
}

pub fn removeWriter(self_obj: *py.Object, arg: *py.Object) py.Error!*py.Object {
    return remove(self_obj, arg, true);
}

pub fn closeAll(st: *State) void {
    var it = st.pollers.valueIterator();
    while (it.next()) |entry| destroy(entry.*);
    st.pollers.deinit(alloc);
    st.pollers = .empty;
}

pub fn traverse(st: *State, visitproc: c.visitproc, arg: ?*anyopaque) c_int {
    var it = st.pollers.valueIterator();
    while (it.next()) |entry| {
        var r = py.visit(@ptrCast(entry.*.reader), visitproc, arg);
        if (r != 0) return r;
        r = py.visit(@ptrCast(entry.*.writer), visitproc, arg);
        if (r != 0) return r;
    }
    return 0;
}
