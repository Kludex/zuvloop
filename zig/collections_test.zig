//! Unit tests for the ready queue and the timer heap.
//!
//! These link no interpreter. The containers reach CPython only to release what
//! they own, so standing in for that one operation is the whole of what the
//! binary needs - and it is also the observable the tests want. Every object goes
//! in at one reference, so a release is an object reaching zero, and the pool
//! below records which objects got there and in what order.
//!
//! Which call to stand in for depends on the build. A standard interpreter's
//! `py.decref` inlines CPython's `Py_DECREF`, which calls `_Py_Dealloc` at zero.
//! A free-threaded one routes through `zuvloop_Py_DECREF` in python_shim.c,
//! whose refcount handling is the interpreter's own. Both are exported below, so
//! `zig build test` works against either, and neither pulls in libpython.
//!
//! Those exports are also why this is a separate file rather than tests beside
//! the code: they must not reach the extension.

const std = @import("std");

const collections = @import("collections.zig");
const py = @import("py.zig");

/// Stand-in objects, and the order in which the code under test released them.
///
/// One global instance, because `_Py_Dealloc` has C linkage and so no way to be
/// handed anything. Every test resets it first.
const Pool = struct {
    objects: [512]py.Object,
    /// Whether `compact` should treat each object as a cancelled handle.
    cancelled: [512]bool,
    /// Only used on a free-threaded build, where the exports below do the
    /// counting that CPython's inline `Py_DECREF` does on a standard one.
    counts: [512]usize,
    released: [512]usize,
    release_count: usize,

    fn reset(self: *Pool) void {
        self.objects = std.mem.zeroes(@TypeOf(self.objects));
        self.cancelled = @splat(false);
        self.counts = @splat(0);
        self.release_count = 0;
    }

    /// A new reference for a container to take ownership of.
    fn ref(self: *Pool, index: usize) *py.Object {
        const object = &self.objects[index];
        py.incref(object);
        return object;
    }

    fn record(self: *Pool, object: *const py.Object) void {
        self.released[self.release_count] = self.indexOf(object);
        self.release_count += 1;
    }

    /// Objects are identified by index throughout, so that what a failure prints
    /// is the order they were created in rather than a list of addresses.
    fn indexOf(self: *const Pool, object: *const py.Object) usize {
        return (@intFromPtr(object) - @intFromPtr(&self.objects[0])) / @sizeOf(py.Object);
    }

    fn releases(self: *const Pool) []const usize {
        return self.released[0..self.release_count];
    }
};

// SAFETY: every test resets the pool before it reaches anything under test.
var pool: Pool = undefined;

export fn _Py_Dealloc(object: [*c]py.Object) void {
    pool.record(@ptrCast(object));
}

export fn zuvloop_Py_INCREF(object: [*c]py.Object) void {
    pool.counts[pool.indexOf(@ptrCast(object))] += 1;
}

export fn zuvloop_Py_DECREF(object: [*c]py.Object) void {
    const index = pool.indexOf(@ptrCast(object));
    pool.counts[index] -= 1;
    if (pool.counts[index] == 0) pool.record(&pool.objects[index]);
}

fn isCancelled(handle: *py.Object) bool {
    return pool.cancelled[pool.indexOf(handle)];
}

test "the ready queue gives callbacks back in the order they were queued" {
    pool.reset();
    var ready: collections.Ready = .empty;
    defer ready.deinit();

    for (0..200) |index| try ready.push(pool.ref(index));

    for (0..200) |index| try std.testing.expectEqual(&pool.objects[index], ready.pop());
    try std.testing.expectEqual(null, ready.pop());
}

test "the ready queue keeps its order across the head wrapping round" {
    pool.reset();
    var ready: collections.Ready = .empty;
    defer ready.deinit();

    // 64 is the first capacity, so this fills the buffer exactly, and draining
    // part of it leaves the head somewhere in the middle for the next 40 to wrap
    // around past the end.
    for (0..64) |index| try ready.push(pool.ref(index));
    for (0..40) |index| try std.testing.expectEqual(&pool.objects[index], ready.pop());
    for (64..104) |index| try ready.push(pool.ref(index));

    for (40..104) |index| try std.testing.expectEqual(&pool.objects[index], ready.pop());
    try std.testing.expectEqual(null, ready.pop());
}

test "growing the ready queue keeps the queued callbacks in order" {
    pool.reset();
    var ready: collections.Ready = .empty;
    defer ready.deinit();

    for (0..64) |index| try ready.push(pool.ref(index));
    for (0..10) |index| try std.testing.expectEqual(&pool.objects[index], ready.pop());
    // Grows with the head at 10, so the copy has to start there rather than at 0.
    for (64..200) |index| try ready.push(pool.ref(index));

    for (10..200) |index| try std.testing.expectEqual(&pool.objects[index], ready.pop());
}

test "closing the ready queue releases the callbacks still in it, and only those" {
    pool.reset();
    var ready: collections.Ready = .empty;

    for (0..64) |index| try ready.push(pool.ref(index));
    for (0..40) |_| _ = ready.pop();
    for (64..104) |index| try ready.push(pool.ref(index));
    // The 40 popped ones are the caller's to release now, so nothing has been
    // released yet and the queue owns exactly what wrapped around.
    try std.testing.expectEqual(0, pool.release_count);

    ready.deinit();

    // Everything from 40 up, in ring order: the 24 left from the first batch,
    // then the 40 that wrapped.
    var expected: [64]usize = undefined;
    for (&expected, 40..) |*slot, index| slot.* = index;
    try std.testing.expectEqualSlices(usize, &expected, pool.releases());
}

test "space reserved for a whole batch is enough for all of it" {
    pool.reset();
    var ready: collections.Ready = .empty;
    defer ready.deinit();

    // How `drainThreadsafe` moves the cross-thread inbox across (loop.zig:380):
    // reserve once for the whole batch, then push without re-checking capacity.
    // Nothing else here reserves for more than one entry at a time.
    try ready.ensureUnusedCapacity(100);
    for (0..100) |index| ready.pushAssumeCapacity(pool.ref(index));
    for (0..30) |index| try std.testing.expectEqual(&pool.objects[index], ready.pop());

    // And again with the head part-way along, so the batch has to wrap.
    try ready.ensureUnusedCapacity(50);
    for (100..150) |index| ready.pushAssumeCapacity(pool.ref(index));

    for (30..150) |index| try std.testing.expectEqual(&pool.objects[index], ready.pop());
    try std.testing.expectEqual(null, ready.pop());
}

test "closing an empty ready queue releases nothing" {
    pool.reset();
    var ready: collections.Ready = .empty;
    ready.deinit();
    try std.testing.expectEqual(0, pool.release_count);
}

test "timers surface in deadline order" {
    pool.reset();
    var timers: collections.Timers = .empty;
    defer timers.deinit();

    const deadlines = [_]f64{ 5.0, 1.0, 4.0, 0.5, 3.0, 2.0 };
    for (deadlines, 0..) |when, index| try timers.push(when, pool.ref(index));

    var last: f64 = -1.0;
    for (0..deadlines.len) |_| {
        const entry = timers.pop().?;
        try std.testing.expect(entry.when > last);
        last = entry.when;
    }
    try std.testing.expectEqual(null, timers.pop());
}

test "timers sharing a deadline surface in the order they were scheduled" {
    pool.reset();
    var timers: collections.Timers = .empty;
    defer timers.deinit();

    for (0..32) |index| try timers.push(1.0, pool.ref(index));

    for (0..32) |index| try std.testing.expectEqual(&pool.objects[index], timers.pop().?.handle);
}

test "peeking leaves the next timer where it is" {
    pool.reset();
    var timers: collections.Timers = .empty;
    defer timers.deinit();

    try std.testing.expectEqual(null, timers.peek());
    try timers.push(2.0, pool.ref(0));
    try timers.push(1.0, pool.ref(1));

    try std.testing.expectEqual(&pool.objects[1], timers.peek().?.handle);
    try std.testing.expectEqual(&pool.objects[1], timers.peek().?.handle);
    try std.testing.expectEqual(&pool.objects[1], timers.pop().?.handle);
}

test "closing the timer heap releases every scheduled handle" {
    pool.reset();
    var timers: collections.Timers = .empty;

    for (0..100) |index| try timers.push(@floatFromInt(100 - index), pool.ref(index));
    _ = timers.pop();
    try std.testing.expectEqual(0, pool.release_count);

    timers.deinit();

    // The one popped entry belongs to the caller, so 99 of the 100 are released.
    // Heap order decides which slot each sits in, so compare as a set.
    try std.testing.expectEqual(99, pool.release_count);
    var seen: [100]bool = @splat(false);
    for (pool.releases()) |index| {
        try std.testing.expect(!seen[index]);
        seen[index] = true;
    }
    try std.testing.expect(!seen[99]); // the last pushed had the earliest deadline
}

test "compacting drops the cancelled timers and leaves the rest in deadline order" {
    pool.reset();
    var timers: collections.Timers = .empty;
    defer timers.deinit();

    for (0..40) |index| {
        try timers.push(@floatFromInt(40 - index), pool.ref(index));
        pool.cancelled[index] = index % 3 == 0;
    }

    timers.compact(&isCancelled);

    try std.testing.expectEqual(14, pool.release_count);
    for (pool.releases()) |index| try std.testing.expect(pool.cancelled[index]);

    var last: f64 = -1.0;
    var surviving: usize = 0;
    while (timers.pop()) |entry| {
        try std.testing.expect(entry.when > last);
        try std.testing.expect(!pool.cancelled[pool.indexOf(entry.handle)]);
        last = entry.when;
        surviving += 1;
        py.decref(entry.handle);
    }
    try std.testing.expectEqual(26, surviving);
}

test "compacting an entirely cancelled heap empties it" {
    pool.reset();
    var timers: collections.Timers = .empty;
    defer timers.deinit();

    for (0..20) |index| {
        try timers.push(@floatFromInt(index), pool.ref(index));
        pool.cancelled[index] = true;
    }

    timers.compact(&isCancelled);

    try std.testing.expectEqual(20, pool.release_count);
    try std.testing.expectEqual(null, timers.pop());
    try std.testing.expectEqual(null, timers.peek());
}

test "compacting a heap with nothing cancelled keeps all of it" {
    pool.reset();
    var timers: collections.Timers = .empty;
    defer timers.deinit();

    for (0..20) |index| try timers.push(@floatFromInt(20 - index), pool.ref(index));

    timers.compact(&isCancelled);

    try std.testing.expectEqual(0, pool.release_count);
    var surviving: usize = 0;
    while (timers.pop()) |entry| {
        surviving += 1;
        py.decref(entry.handle);
    }
    try std.testing.expectEqual(20, surviving);
}

test "a timer scheduled for NaN still surfaces and is still released" {
    pool.reset();
    var timers: collections.Timers = .empty;

    // `TimerEntry.before` compares with `<`, which is false in both directions
    // against NaN, so a NaN deadline is ordered against nothing. The heap has to
    // stay whole regardless: every entry drains, and closing releases the rest.
    const deadlines = [_]f64{ 3.0, std.math.nan(f64), 1.0, std.math.nan(f64), 2.0 };
    for (deadlines, 0..) |when, index| try timers.push(when, pool.ref(index));

    var drained: usize = 0;
    while (timers.pop()) |entry| {
        drained += 1;
        py.decref(entry.handle);
    }
    try std.testing.expectEqual(deadlines.len, drained);
    try std.testing.expectEqual(deadlines.len, pool.release_count);

    timers.deinit();
}
