//! Unit tests for discarding a pending write batch.
//!
//! `discardPending` reaches CPython once per view it owns, through `PyBuffer_Release`, so
//! standing in for that one function is the whole of what the binary needs - and it is the
//! observable too: which views were released, and how many times each.
//!
//! What is worth pinning is the order. The counters are cleared *before* the releases, and
//! releasing a view runs the exporter's `__release_buffer__`, which is Python and may
//! reenter. Clear them afterwards instead and a reentrant call finds a batch that is still
//! there and releases it a second time.

const std = @import("std");

const py = @import("py.zig");
const transport = @import("transport.zig");

const c = py.c;

/// Which views were released, by index into the subject's array, in the order they went.
var released: [16]usize = undefined;
var release_count: usize = 0;

/// When set, the first release reenters `discardPending`, standing in for an exporter whose
/// `__release_buffer__` writes to the same transport.
var reenter: ?*transport.Transport = null;

// SAFETY: each test points this at its own transport before calling anything under test.
var subject: *transport.Transport = undefined;

export fn PyBuffer_Release(view: [*c]c.Py_buffer) void {
    const base = @intFromPtr(&subject.pending_views[0]);
    released[release_count] = (@intFromPtr(view) - base) / @sizeOf(c.Py_buffer);
    release_count += 1;
    if (reenter) |target| {
        reenter = null;
        transport.discardPending(target);
    }
}

fn batchOf(count: usize, size: usize) transport.Transport {
    var subject_transport = std.mem.zeroes(transport.Transport);
    subject_transport.pending_count = count;
    subject_transport.pending_size = size;
    return subject_transport;
}

fn begin(subject_transport: *transport.Transport) void {
    subject = subject_transport;
    release_count = 0;
    reenter = null;
}

test "discarding a batch releases every view in it, once each" {
    var batch = batchOf(3, 4096);
    begin(&batch);

    transport.discardPending(&batch);

    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, released[0..release_count]);
    try std.testing.expectEqual(0, batch.pending_count);
    try std.testing.expectEqual(0, batch.pending_size);
}

test "discarding an empty batch releases nothing" {
    var batch = batchOf(0, 0);
    begin(&batch);

    transport.discardPending(&batch);

    try std.testing.expectEqual(0, release_count);
}

test "a full batch is released in full" {
    var batch = batchOf(0, 0);
    batch.pending_count = batch.pending_views.len;
    begin(&batch);

    transport.discardPending(&batch);

    try std.testing.expectEqual(batch.pending_views.len, release_count);
}

test "the batch is emptied before anything is released" {
    // Releasing a view hands control to Python, which can come straight back here. The
    // batch has to be gone by then, or the reentrant call releases all of it a second time
    // and every exporter is left over-released.
    var batch = batchOf(3, 4096);
    begin(&batch);
    reenter = &batch;

    transport.discardPending(&batch);

    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, released[0..release_count]);
}

test "discarding twice releases nothing the second time" {
    var batch = batchOf(2, 512);
    begin(&batch);

    transport.discardPending(&batch);
    const after_first = release_count;
    transport.discardPending(&batch);

    try std.testing.expectEqual(after_first, release_count);
}
