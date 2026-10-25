//! Unit tests for address comparison.
//!
//! `addr.same` decides whether two `struct sockaddr` describe the same peer, and
//! `datagram.zig` asks it whether a `sendto` is allowed on a connected socket. A wrong
//! `true` there sends a datagram somewhere the caller did not connect to, so what it
//! ignores matters as much as what it compares: padding bytes are not part of an
//! address, and IPv6 ancillary fields are.
//!
//! Nothing here needs an interpreter. `fromPython` and `toPython` do, but they are not
//! referenced from this file, so Zig never analyses them and the binary links with no
//! CPython symbols at all - only the headers, for `PyObject`.

const std = @import("std");

const addr = @import("addr.zig");

const posix = std.posix;
const expect = std.testing.expect;

/// An address together with the length the kernel would report for it. The length is
/// only load-bearing for `AF_UNIX`, where it is the one thing that tells an abstract
/// name from a shorter path padded out with zeros.
const Address = struct {
    storage: addr.Storage = .{},
    len: c_int = 0,
};

fn matches(a: *const Address, b: *const Address) bool {
    return addr.same(a.storage.constPtr(), a.len, b.storage.constPtr(), b.len);
}

fn ipv4(port: u16, octets: [4]u8) Address {
    var address: Address = .{ .len = @sizeOf(posix.sockaddr.in) };
    const in: *posix.sockaddr.in = @ptrCast(&address.storage);
    in.* = .{
        .port = std.mem.nativeToBig(u16, port),
        .addr = @bitCast(octets),
    };
    return address;
}

fn ipv6(port: u16, group: u16, flowinfo: u32, scope_id: u32) Address {
    var address: Address = .{ .len = @sizeOf(posix.sockaddr.in6) };
    const in6: *posix.sockaddr.in6 = @ptrCast(&address.storage);
    var bytes: [16]u8 = @splat(0);
    std.mem.writeInt(u16, bytes[0..2], group, .big);
    bytes[15] = 1;
    in6.* = .{
        .port = std.mem.nativeToBig(u16, port),
        .flowinfo = flowinfo,
        .addr = bytes,
        .scope_id = scope_id,
    };
    return address;
}

/// A filesystem socket: the name runs up to its terminator, so trailing zeros inside
/// the reported length are padding rather than part of it.
fn unixPath(path: []const u8, len: c_int) Address {
    var address: Address = .{ .len = len };
    const un: *posix.sockaddr.un = @ptrCast(&address.storage);
    un.* = .{ .path = @splat(0) };
    @memcpy(un.path[0..path.len], path);
    return address;
}

/// A Linux abstract socket: a leading NUL, and then the name is exactly as long as the
/// address says, embedded zeros included.
fn unixAbstract(name: []const u8) Address {
    var address: Address = .{ .len = @intCast(@offsetOf(posix.sockaddr.un, "path") + 1 + name.len) };
    const un: *posix.sockaddr.un = @ptrCast(&address.storage);
    un.* = .{ .path = @splat(0) };
    @memcpy(un.path[1 .. 1 + name.len], name);
    return address;
}

test "an IPv4 address matches itself and nothing with a different host or port" {
    const base = ipv4(8080, .{ 127, 0, 0, 1 });
    try expect(matches(&base, &ipv4(8080, .{ 127, 0, 0, 1 })));
    try expect(!matches(&base, &ipv4(8081, .{ 127, 0, 0, 1 })));
    try expect(!matches(&base, &ipv4(8080, .{ 127, 0, 0, 2 })));
    try expect(!matches(&base, &ipv4(0, .{ 127, 0, 0, 1 })));
}

test "IPv4 padding is not part of the address" {
    // `sockaddr_in` carries eight bytes of padding that no peer identity depends on.
    // A comparison done over the whole struct would call these two different.
    var padded = ipv4(8080, .{ 10, 0, 0, 7 });
    const un: *posix.sockaddr.un = @ptrCast(&padded.storage);
    @memset(un.path[@sizeOf(posix.sockaddr.in) - @offsetOf(posix.sockaddr.un, "path") ..], 0xff);
    try expect(matches(&padded, &ipv4(8080, .{ 10, 0, 0, 7 })));
}

test "an IPv6 address states its scope and flow exactly" {
    // asyncio compares the whole four-tuple, so zero is a value here and not a wildcard:
    // an address scoped to one interface is not the same peer as the unscoped one.
    const scoped = ipv6(443, 0x2001, 0, 7);
    try expect(matches(&scoped, &ipv6(443, 0x2001, 0, 7)));
    try expect(!matches(&scoped, &ipv6(443, 0x2001, 0, 0)));
    try expect(!matches(&scoped, &ipv6(443, 0x2001, 9, 7)));
    try expect(!matches(&scoped, &ipv6(443, 0xfe80, 0, 7)));
    try expect(!matches(&scoped, &ipv6(444, 0x2001, 0, 7)));
}

test "addresses of different families never match" {
    try expect(!matches(&ipv4(80, .{ 127, 0, 0, 1 }), &ipv6(80, 0x2001, 0, 0)));
    try expect(!matches(&ipv4(80, .{ 127, 0, 0, 1 }), &unixPath("/tmp/s", 16)));
    try expect(!matches(&unixAbstract("n"), &ipv6(80, 0x2001, 0, 0)));
}

test "a family that is not IP or Unix never matches, even against itself" {
    // Deliberately conservative: `datagram.zig` uses this to decide whether a send is
    // allowed, so an address it cannot reason about has to be refused rather than waved
    // through on a byte comparison.
    var one: Address = .{ .len = 16 };
    one.storage.bytes[0] = 0x11; // AF_PACKET on Linux, whatever the host calls it
    var two = one;
    try expect(!matches(&one, &two));
}

test "a Unix path ends at its terminator, whatever length is reported" {
    // The two sides need not agree on how many trailing zeros they count.
    const exact = unixPath("/tmp/zuvloop.sock", 2 + 17);
    const padded = unixPath("/tmp/zuvloop.sock", 2 + 40);
    try expect(matches(&exact, &padded));
    try expect(matches(&padded, &exact));
    try expect(!matches(&exact, &unixPath("/tmp/zuvloop.soc", 2 + 16)));
    try expect(!matches(&exact, &unixPath("/tmp/other.sock", 2 + 15)));
}

test "an abstract Unix name is as long as the address says it is" {
    // A leading NUL means the rest is not a C string: embedded zeros belong to the name,
    // and the reported length is the only thing that says where it stops.
    const short = unixAbstract("zuv");
    try expect(matches(&short, &unixAbstract("zuv")));
    try expect(!matches(&short, &unixAbstract("zuvloop")));
    try expect(!matches(&short, &unixAbstract("zu")));

    const embedded = unixAbstract(&[_]u8{ 'a', 0, 'b' });
    try expect(matches(&embedded, &unixAbstract(&[_]u8{ 'a', 0, 'b' })));
    try expect(!matches(&embedded, &unixAbstract(&[_]u8{'a'})));
}

test "an abstract name is never the same as a path with the same bytes" {
    const abstract = unixAbstract("tmp/s");
    const path = unixPath("tmp/s", 2 + 5);
    try expect(!matches(&abstract, &path));
    try expect(!matches(&path, &abstract));
}

test "an unnamed Unix address matches another unnamed one" {
    // An autobind or unnamed socket reports no path at all, so there is nothing to
    // distinguish two of them by.
    const unnamed = unixPath("", @intCast(@offsetOf(posix.sockaddr.un, "path")));
    try expect(matches(&unnamed, &unixPath("", @intCast(@offsetOf(posix.sockaddr.un, "path")))));
    try expect(!matches(&unnamed, &unixPath("/tmp/s", 2 + 6)));
}
