//! Winsock declarations the standard library does not carry for Windows
//! targets. Shapes and values mirror `ws2tcpip.h`; everything here resolves
//! from `ws2_32.dll`, which the build links on Windows.

const std = @import("std");

pub const SOCKET = usize;

/// `ADDRINFOA`: `ai_canonname` precedes `ai_addr`, as on the BSDs, and
/// `ai_addrlen` is a `size_t` rather than a `socklen_t`.
pub const addrinfo = extern struct {
    flags: AI,
    family: c_int,
    socktype: c_int,
    protocol: c_int,
    addrlen: usize,
    canonname: ?[*:0]u8,
    addr: ?*std.posix.sockaddr,
    next: ?*addrinfo,
};

pub const AI = packed struct(u32) {
    PASSIVE: bool = false,
    CANONNAME: bool = false,
    NUMERICHOST: bool = false,
    NUMERICSERV: bool = false,
    _4: u28 = 0,
};

/// The subset of `EAI_*` Windows defines, at their Winsock values. `NODATA`
/// is deliberately absent: `ws2tcpip.h` aliases it to `NONAME`, and a Zig
/// enum cannot carry two names for one value.
pub const EAI = enum(c_int) {
    MEMORY = 8,
    BADFLAGS = 10022,
    SOCKTYPE = 10044,
    FAMILY = 10047,
    SERVICE = 10109,
    NONAME = 11001,
    AGAIN = 11002,
    FAIL = 11003,
    _,
};

pub fn gai_strerror(code: EAI) [*:0]const u8 {
    return switch (code) {
        .MEMORY => "A memory allocation failure occurred",
        .BADFLAGS => "An invalid value was provided for the ai_flags member",
        .SOCKTYPE => "The ai_socktype member is not supported",
        .FAMILY => "The ai_family member is not supported",
        .SERVICE => "The servname parameter is not supported for ai_socktype",
        .NONAME => "The name does not resolve for the supplied parameters",
        .AGAIN => "A temporary failure in name resolution occurred",
        .FAIL => "A nonrecoverable failure in name resolution occurred",
        else => "getaddrinfo failed",
    };
}

pub extern "c" fn getaddrinfo(
    node: ?[*:0]const u8,
    service: ?[*:0]const u8,
    hints: ?*const addrinfo,
    res: *?*addrinfo,
) EAI;
pub extern "c" fn freeaddrinfo(ai: ?*addrinfo) void;
pub extern "c" fn getsockopt(sock: SOCKET, level: c_int, optname: c_int, optval: [*]u8, optlen: *c_int) c_int;
