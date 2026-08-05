//! Data structures backing the loop's hot paths.

const std = @import("std");
const py = @import("py.zig");

const alloc = std.heap.c_allocator;

/// Power-of-two ring buffer of owned object references.
pub const Ready = struct {
    items: []?*py.Object,
    head: usize = 0,
    len: usize = 0,

    pub const empty: Ready = .{ .items = &.{} };

    pub fn deinit(self: *Ready) void {
        var i: usize = 0;
        while (i < self.len) : (i += 1) {
            py.xdecref(self.items[(self.head + i) & (self.items.len - 1)]);
        }
        if (self.items.len != 0) alloc.free(self.items);
        self.* = .empty;
    }

    /// Takes ownership of `item`.
    pub fn push(self: *Ready, item: *py.Object) error{OutOfMemory}!void {
        try self.ensureUnusedCapacity(1);
        self.pushAssumeCapacity(item);
    }

    pub fn ensureUnusedCapacity(self: *Ready, additional: usize) error{OutOfMemory}!void {
        if (additional <= self.items.len - self.len) return;
        var new_cap = if (self.items.len == 0) 64 else self.items.len;
        const needed = self.len + additional;
        while (new_cap < needed) new_cap *= 2;
        try self.grow(new_cap);
    }

    /// Takes ownership of `item`; capacity must have been reserved first.
    pub fn pushAssumeCapacity(self: *Ready, item: *py.Object) void {
        std.debug.assert(self.len < self.items.len);
        self.items[(self.head + self.len) & (self.items.len - 1)] = item;
        self.len += 1;
    }

    pub fn pop(self: *Ready) ?*py.Object {
        if (self.len == 0) return null;
        const item = self.items[self.head];
        self.head = (self.head + 1) & (self.items.len - 1);
        self.len -= 1;
        return item;
    }

    fn grow(self: *Ready, new_cap: usize) error{OutOfMemory}!void {
        const new_items = try alloc.alloc(?*py.Object, new_cap);
        var i: usize = 0;
        while (i < self.len) : (i += 1) {
            new_items[i] = self.items[(self.head + i) & (self.items.len - 1)];
        }
        if (self.items.len != 0) alloc.free(self.items);
        self.items = new_items;
        self.head = 0;
    }
};

pub const TimerEntry = struct {
    when: f64,
    seq: u64,
    handle: *py.Object,

    fn before(a: TimerEntry, b: TimerEntry) bool {
        if (a.when != b.when) return a.when < b.when;
        return a.seq < b.seq;
    }
};

/// Binary min-heap ordered by deadline, then insertion order.
///
/// Cancellation is lazy: entries stay until they surface, mirroring asyncio's
/// scheduler so that cancelling a timer stays O(1).
pub const Timers = struct {
    items: []TimerEntry,
    len: usize = 0,
    seq: u64 = 0,
    cancelled: usize = 0,

    pub const empty: Timers = .{ .items = &.{} };

    pub fn deinit(self: *Timers) void {
        var i: usize = 0;
        while (i < self.len) : (i += 1) py.decref(self.items[i].handle);
        if (self.items.len != 0) alloc.free(self.items);
        self.* = .empty;
    }

    /// Takes ownership of `handle`.
    pub fn push(self: *Timers, when: f64, handle: *py.Object) error{OutOfMemory}!void {
        if (self.len == self.items.len) try self.grow();
        self.seq += 1;
        self.items[self.len] = .{ .when = when, .seq = self.seq, .handle = handle };
        self.len += 1;
        self.siftUp(self.len - 1);
    }

    pub fn peek(self: *const Timers) ?TimerEntry {
        if (self.len == 0) return null;
        return self.items[0];
    }

    pub fn pop(self: *Timers) ?TimerEntry {
        if (self.len == 0) return null;
        const top = self.items[0];
        self.len -= 1;
        if (self.len != 0) {
            self.items[0] = self.items[self.len];
            self.siftDown(0);
        }
        return top;
    }

    /// Drops cancelled entries once they dominate the heap.
    /// Drops cancelled entries and restores the heap.
    ///
    /// The handles are released only once the heap is whole again. Releasing one
    /// runs its arguments' finalizers, and a finalizer calling `call_later`
    /// reaches `push`, which writes at `items[len]` and sifts - so a release from
    /// inside the pass would shuffle slots the pass has already moved, and one
    /// from inside the array would land on an entry not yet released. Which is
    /// also why the doomed handles are copied out rather than parked in the tail.
    pub fn compact(self: *Timers, isCancelled: *const fn (*py.Object) bool) void {
        // Sized by the heap rather than by `cancelled`, which is a hint and not a
        // guarantee: `Handle.cancel` only reports to the loop while it still has
        // one, so an entry can be cancelled without ever being counted. Nowhere
        // to set them aside means nothing can be released, so the heap is left
        // exactly as it was - count included - for the next cancellation to retry.
        const doomed = alloc.alloc(*py.Object, self.len) catch return;
        defer alloc.free(doomed);
        var dropped: usize = 0;
        var write: usize = 0;
        var read: usize = 0;
        while (read < self.len) : (read += 1) {
            const entry = self.items[read];
            if (isCancelled(entry.handle)) {
                doomed[dropped] = entry.handle;
                dropped += 1;
            } else {
                self.items[write] = entry;
                write += 1;
            }
        }
        self.len = write;
        self.cancelled = 0;
        if (write >= 2) {
            var i = write / 2;
            while (i > 0) {
                i -= 1;
                self.siftDown(i);
            }
        }
        for (doomed[0..dropped]) |handle| py.decref(handle);
    }

    fn grow(self: *Timers) error{OutOfMemory}!void {
        const new_cap = if (self.items.len == 0) 32 else self.items.len * 2;
        const new_items = try alloc.realloc(self.items, new_cap);
        self.items = new_items;
    }

    fn siftUp(self: *Timers, start: usize) void {
        var i = start;
        const item = self.items[i];
        while (i > 0) {
            const parent = (i - 1) / 2;
            if (!TimerEntry.before(item, self.items[parent])) break;
            self.items[i] = self.items[parent];
            i = parent;
        }
        self.items[i] = item;
    }

    fn siftDown(self: *Timers, start: usize) void {
        var i = start;
        const item = self.items[i];
        while (true) {
            const left = i * 2 + 1;
            if (left >= self.len) break;
            const right = left + 1;
            const child = if (right < self.len and TimerEntry.before(self.items[right], self.items[left])) right else left;
            if (!TimerEntry.before(self.items[child], item)) break;
            self.items[i] = self.items[child];
            i = child;
        }
        self.items[i] = item;
    }
};
