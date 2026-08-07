//! Hand-written declarations for the subset of libuv that zuvloop uses.
//!
//! `uv.h` cannot be run through translate-c: on Darwin it pulls in `mach/mach.h`,
//! whose message structs defeat the translator. Declaring the ABI here also keeps
//! handle sizes out of comptime, so Python types size themselves from
//! `uv.handleSize()` at import time and stay correct across libuv versions.

const std = @import("std");
const builtin = @import("builtin");
const win32 = @import("win32.zig");

pub const Loop = opaque {};
pub const Handle = opaque {};
pub const Stream = opaque {};
pub const Tcp = opaque {};
pub const Pipe = opaque {};
pub const Udp = opaque {};
pub const Timer = opaque {};
pub const Idle = opaque {};
pub const Prepare = opaque {};
pub const Check = opaque {};
pub const Async = opaque {};
pub const Poll = opaque {};
pub const Signal = opaque {};
pub const Process = opaque {};
pub const Req = opaque {};
pub const Write = opaque {};
pub const Connect = opaque {};
pub const Shutdown = opaque {};
pub const GetAddrInfo = opaque {};
pub const GetNameInfo = opaque {};
pub const UdpSend = opaque {};

pub const is_windows = builtin.os.tag == .windows;

/// `std.c`'s resolver declarations are holes on Windows targets, so the shapes
/// come from `win32.zig` there.
pub const AddrInfo = if (is_windows) win32.addrinfo else std.c.addrinfo;

pub const OsSock = if (is_windows) win32.SOCKET else c_int;
pub const File = c_int;
pub const OsFd = if (is_windows) std.os.windows.HANDLE else c_int;
pub const Pid = if (is_windows) c_int else std.c.pid_t;
pub const Uid = if (is_windows) u8 else std.c.uid_t;
pub const Gid = if (is_windows) u8 else std.c.gid_t;

pub const HandleType = enum(c_uint) {
    unknown = 0,
    async = 1,
    check = 2,
    fs_event = 3,
    fs_poll = 4,
    handle = 5,
    idle = 6,
    named_pipe = 7,
    poll = 8,
    prepare = 9,
    process = 10,
    stream = 11,
    tcp = 12,
    timer = 13,
    tty = 14,
    udp = 15,
    signal = 16,
    file = 17,
};

pub const ReqType = enum(c_uint) {
    unknown = 0,
    req = 1,
    connect = 2,
    write = 3,
    shutdown = 4,
    udp_send = 5,
    fs = 6,
    work = 7,
    getaddrinfo = 8,
    getnameinfo = 9,
    random = 10,
};

pub const RunMode = enum(c_uint) { default = 0, once = 1, nowait = 2 };

pub const LoopOption = enum(c_uint) { block_signal = 0, metrics_idle_time = 1, use_io_uring_sqpoll = 2 };

pub const READABLE: c_int = 1;
pub const WRITABLE: c_int = 2;
pub const DISCONNECT: c_int = 4;
pub const PRIORITIZED: c_int = 8;

pub const TCP_IPV6ONLY: c_uint = 1;
pub const UDP_IPV6ONLY: c_uint = 1;
pub const UDP_PARTIAL: c_uint = 2;
pub const UDP_REUSEADDR: c_uint = 4;
pub const UDP_MMSG_CHUNK: c_uint = 8;
pub const UDP_MMSG_FREE: c_uint = 16;

/// `uv_buf_t` mirrors the platform's scatter-gather element: `iovec` on POSIX
/// and `WSABUF` on Windows - whose fields are in the opposite order, and whose
/// length is 32-bit, so no single buffer above 4 GiB can be expressed there.
pub const Buf = if (is_windows) extern struct {
    len: c_ulong,
    base: [*]u8,

    /// The largest single buffer `WSABUF` can carry.
    pub const max_len: usize = std.math.maxInt(c_ulong);

    pub fn init(base: [*]u8, len: usize) Buf {
        return .{ .base = base, .len = @intCast(len) };
    }
} else extern struct {
    base: [*]u8,
    len: usize,

    pub const max_len: usize = std.math.maxInt(usize);

    pub fn init(base: [*]u8, len: usize) Buf {
        return .{ .base = base, .len = len };
    }
};

/// A descriptor from Python, as the socket `uv_*_open` takes. On Windows that
/// is the `SOCKET` itself; sign-extending keeps -1 equal to `INVALID_SOCKET`,
/// so a bad descriptor fails in libuv instead of tripping a cast.
pub inline fn asSock(fd: c_int) OsSock {
    return if (is_windows) @bitCast(@as(isize, fd)) else fd;
}

pub const Metrics = extern struct {
    loop_count: u64,
    events: u64,
    events_waiting: u64,
    reserved: [13]?*u64,
};

pub const StdioFlags = struct {
    pub const ignore: c_uint = 0;
    pub const create_pipe: c_uint = 1;
    pub const inherit_fd: c_uint = 2;
    pub const inherit_stream: c_uint = 4;
    pub const readable_pipe: c_uint = 16;
    pub const writable_pipe: c_uint = 32;
    pub const nonblock_pipe: c_uint = 64;
};

pub const ProcessFlags = struct {
    pub const setuid: c_uint = 1;
    pub const setgid: c_uint = 2;
    pub const windows_verbatim_arguments: c_uint = 4;
    pub const detached: c_uint = 8;
};

pub const StdioContainer = extern struct {
    flags: c_uint,
    data: extern union {
        stream: ?*Stream,
        fd: c_int,
    },
};

pub const ProcessOptions = extern struct {
    exit_cb: ?*const fn (?*Process, i64, c_int) callconv(.c) void,
    file: [*:0]const u8,
    args: [*]const ?[*:0]const u8,
    env: ?[*]const ?[*:0]const u8,
    cwd: ?[*:0]const u8,
    flags: c_uint,
    stdio_count: c_int,
    stdio: ?[*]StdioContainer,
    uid: Uid,
    gid: Gid,
};

/// On POSIX libuv maps system errors to their negated errno, so the values are
/// derived per-target rather than hard-coded. On Windows no errno corresponds,
/// and libuv assigns each error a fixed code of its own; those are mirrored
/// from `uv/errno.h`, which never changes a value once assigned.
fn negErrno(comptime e: anytype) c_int {
    return -@as(c_int, @intCast(@intFromEnum(@field(std.c.E, @tagName(e)))));
}

fn uvErr(comptime windows_code: c_int, comptime e: anytype) c_int {
    return if (is_windows) windows_code else negErrno(e);
}

pub const EOF: c_int = -4095;
/// libuv's own resolver codes, which are not errno values and do not vary by
/// platform - unlike the `EAI_*` they correspond to.
pub const EAI_ADDRFAMILY: c_int = -3000;
pub const EAI_AGAIN: c_int = -3001;
pub const EAI_BADFLAGS: c_int = -3002;
pub const EAI_FAIL: c_int = -3004;
pub const EAI_FAMILY: c_int = -3005;
pub const EAI_MEMORY: c_int = -3006;
pub const EAI_NODATA: c_int = -3007;
pub const EAI_NONAME: c_int = -3008;
pub const EAI_OVERFLOW: c_int = -3009;
pub const EAI_SERVICE: c_int = -3010;
pub const EAI_SOCKTYPE: c_int = -3011;
pub const EAI_BADHINTS: c_int = -3013;
pub const EAI_PROTOCOL: c_int = -3014;

pub const EAGAIN = uvErr(-4088, .AGAIN);
pub const ECANCELED = uvErr(-4081, .CANCELED);
pub const EINVAL = uvErr(-4071, .INVAL);
pub const ENOSYS = uvErr(-4054, .NOSYS);
pub const EBADF = uvErr(-4083, .BADF);
pub const ENOTCONN = uvErr(-4053, .NOTCONN);
pub const EISCONN = uvErr(-4069, .ISCONN);
pub const EALREADY = uvErr(-4084, .ALREADY);
pub const EPIPE = uvErr(-4047, .PIPE);
pub const ECONNRESET = uvErr(-4077, .CONNRESET);
pub const ENOBUFS = uvErr(-4060, .NOBUFS);
pub const EAI_CANCELED: c_int = -3003;

pub const CloseCb = *const fn (?*Handle) callconv(.c) void;
pub const IdleCb = *const fn (?*Idle) callconv(.c) void;
pub const PrepareCb = *const fn (?*Prepare) callconv(.c) void;
pub const CheckCb = *const fn (?*Check) callconv(.c) void;
pub const AsyncCb = *const fn (?*Async) callconv(.c) void;
pub const TimerCb = *const fn (?*Timer) callconv(.c) void;
pub const PollCb = *const fn (?*Poll, c_int, c_int) callconv(.c) void;
pub const SignalCb = *const fn (?*Signal, c_int) callconv(.c) void;
pub const AllocCb = *const fn (?*Handle, usize, *Buf) callconv(.c) void;
pub const ReadCb = *const fn (?*Stream, isize, *const Buf) callconv(.c) void;
pub const WriteCb = *const fn (?*Write, c_int) callconv(.c) void;
pub const ConnectCb = *const fn (?*Connect, c_int) callconv(.c) void;
pub const ShutdownCb = *const fn (?*Shutdown, c_int) callconv(.c) void;
pub const ConnectionCb = *const fn (?*Stream, c_int) callconv(.c) void;
pub const GetAddrInfoCb = *const fn (?*GetAddrInfo, c_int, ?*AddrInfo) callconv(.c) void;
pub const GetNameInfoCb = *const fn (?*GetNameInfo, c_int, ?[*:0]const u8, ?[*:0]const u8) callconv(.c) void;
pub const UdpSendCb = *const fn (?*UdpSend, c_int) callconv(.c) void;
pub const UdpRecvCb = *const fn (?*Udp, isize, *const Buf, ?*const std.posix.sockaddr, c_uint) callconv(.c) void;
pub const WalkCb = *const fn (?*Handle, ?*anyopaque) callconv(.c) void;

pub extern fn uv_version_string() [*:0]const u8;
pub extern fn uv_loop_init(loop: *Loop) c_int;
pub extern fn uv_loop_close(loop: *Loop) c_int;
pub extern fn uv_loop_size() usize;
pub extern fn uv_loop_alive(loop: *const Loop) c_int;
pub extern fn uv_loop_fork(loop: *Loop) c_int;
pub extern fn uv_loop_configure(loop: *Loop, option: LoopOption, ...) c_int;
pub extern fn uv_run(loop: *Loop, mode: RunMode) c_int;
pub extern fn uv_stop(loop: *Loop) void;
pub extern fn uv_now(loop: *const Loop) u64;
pub extern fn uv_update_time(loop: *Loop) void;
pub extern fn uv_hrtime() u64;
pub extern fn uv_backend_fd(loop: *const Loop) c_int;
pub extern fn uv_backend_timeout(loop: *const Loop) c_int;
pub extern fn uv_walk(loop: *Loop, walk_cb: WalkCb, arg: ?*anyopaque) void;
pub extern fn uv_metrics_info(loop: *Loop, metrics: *Metrics) c_int;
pub extern fn uv_metrics_idle_time(loop: *Loop) u64;

pub extern fn uv_handle_size(@"type": HandleType) usize;
pub extern fn uv_handle_get_type(handle: *const Handle) HandleType;
pub extern fn uv_req_size(@"type": ReqType) usize;
pub extern fn uv_close(handle: *Handle, close_cb: ?CloseCb) void;
pub extern fn uv_is_active(handle: *const Handle) c_int;
pub extern fn uv_is_closing(handle: *const Handle) c_int;
pub extern fn uv_ref(handle: *Handle) void;
pub extern fn uv_unref(handle: *Handle) void;
pub extern fn uv_has_ref(handle: *const Handle) c_int;
pub extern fn uv_fileno(handle: *const Handle, fd: *OsFd) c_int;
pub extern fn uv_handle_set_data(handle: *Handle, data: ?*anyopaque) void;
pub extern fn uv_handle_get_data(handle: *const Handle) ?*anyopaque;
pub extern fn uv_req_set_data(req: *Req, data: ?*anyopaque) void;
pub extern fn uv_req_get_data(req: *const Req) ?*anyopaque;
pub extern fn uv_cancel(req: *Req) c_int;

pub extern fn uv_idle_init(loop: *Loop, idle: *Idle) c_int;
pub extern fn uv_idle_start(idle: *Idle, cb: IdleCb) c_int;
pub extern fn uv_idle_stop(idle: *Idle) c_int;
pub extern fn uv_prepare_init(loop: *Loop, prepare: *Prepare) c_int;
pub extern fn uv_prepare_start(prepare: *Prepare, cb: PrepareCb) c_int;
pub extern fn uv_prepare_stop(prepare: *Prepare) c_int;
pub extern fn uv_check_init(loop: *Loop, check: *Check) c_int;
pub extern fn uv_check_start(check: *Check, cb: CheckCb) c_int;
pub extern fn uv_check_stop(check: *Check) c_int;
pub extern fn uv_async_init(loop: *Loop, handle: *Async, cb: AsyncCb) c_int;
pub extern fn uv_async_send(handle: *Async) c_int;

pub extern fn uv_timer_init(loop: *Loop, handle: *Timer) c_int;
pub extern fn uv_timer_start(handle: *Timer, cb: TimerCb, timeout: u64, repeat: u64) c_int;
pub extern fn uv_timer_stop(handle: *Timer) c_int;
pub extern fn uv_timer_get_due_in(handle: *const Timer) u64;

pub extern fn uv_poll_init(loop: *Loop, handle: *Poll, fd: c_int) c_int;
pub extern fn uv_poll_init_socket(loop: *Loop, handle: *Poll, socket: OsSock) c_int;
pub extern fn uv_poll_start(handle: *Poll, events: c_int, cb: PollCb) c_int;
pub extern fn uv_poll_stop(handle: *Poll) c_int;

pub extern fn uv_signal_init(loop: *Loop, handle: *Signal) c_int;
pub extern fn uv_signal_start(handle: *Signal, cb: SignalCb, signum: c_int) c_int;
pub extern fn uv_signal_stop(handle: *Signal) c_int;

pub extern fn uv_listen(stream: *Stream, backlog: c_int, cb: ConnectionCb) c_int;
pub extern fn uv_accept(server: *Stream, client: *Stream) c_int;
pub extern fn uv_read_start(stream: *Stream, alloc_cb: AllocCb, read_cb: ReadCb) c_int;
pub extern fn uv_read_stop(stream: *Stream) c_int;
pub extern fn uv_write(req: *Write, handle: *Stream, bufs: [*]const Buf, nbufs: c_uint, cb: WriteCb) c_int;
pub extern fn uv_try_write(handle: *Stream, bufs: [*]const Buf, nbufs: c_uint) c_int;
pub extern fn uv_is_readable(handle: *const Stream) c_int;
pub extern fn uv_is_writable(handle: *const Stream) c_int;
pub extern fn uv_shutdown(req: *Shutdown, handle: *Stream, cb: ShutdownCb) c_int;
pub extern fn uv_stream_get_write_queue_size(stream: *const Stream) usize;

pub extern fn uv_tcp_init_ex(loop: *Loop, handle: *Tcp, flags: c_uint) c_int;
pub extern fn uv_tcp_open(handle: *Tcp, sock: OsSock) c_int;
pub extern fn uv_tcp_nodelay(handle: *Tcp, enable: c_int) c_int;
pub extern fn uv_tcp_keepalive(handle: *Tcp, enable: c_int, delay: c_uint) c_int;
pub extern fn uv_tcp_bind(handle: *Tcp, addr: *const std.posix.sockaddr, flags: c_uint) c_int;
pub extern fn uv_tcp_getsockname(handle: *const Tcp, name: *std.posix.sockaddr, namelen: *c_int) c_int;
pub extern fn uv_tcp_getpeername(handle: *const Tcp, name: *std.posix.sockaddr, namelen: *c_int) c_int;
pub extern fn uv_tcp_connect(req: *Connect, handle: *Tcp, addr: *const std.posix.sockaddr, cb: ConnectCb) c_int;

pub extern fn uv_pipe_init(loop: *Loop, handle: *Pipe, ipc: c_int) c_int;
pub extern fn uv_pipe_open(handle: *Pipe, file: File) c_int;
pub extern fn uv_pipe_bind(handle: *Pipe, name: [*:0]const u8) c_int;
pub extern fn uv_pipe_connect(req: *Connect, handle: *Pipe, name: [*:0]const u8, cb: ConnectCb) void;
pub extern fn uv_pipe_getsockname(handle: *const Pipe, buffer: [*]u8, size: *usize) c_int;
pub extern fn uv_pipe_getpeername(handle: *const Pipe, buffer: [*]u8, size: *usize) c_int;

pub extern fn uv_udp_init_ex(loop: *Loop, handle: *Udp, flags: c_uint) c_int;
pub extern fn uv_udp_open(handle: *Udp, sock: OsSock) c_int;
pub extern fn uv_udp_bind(handle: *Udp, addr: *const std.posix.sockaddr, flags: c_uint) c_int;
pub extern fn uv_udp_connect(handle: *Udp, addr: ?*const std.posix.sockaddr) c_int;
pub extern fn uv_udp_getsockname(handle: *const Udp, name: *std.posix.sockaddr, namelen: *c_int) c_int;
pub extern fn uv_udp_send(req: *UdpSend, handle: *Udp, bufs: [*]const Buf, nbufs: c_uint, addr: ?*const std.posix.sockaddr, cb: UdpSendCb) c_int;
pub extern fn uv_udp_try_send(handle: *Udp, bufs: [*]const Buf, nbufs: c_uint, addr: ?*const std.posix.sockaddr) c_int;
pub extern fn uv_udp_recv_start(handle: *Udp, alloc_cb: AllocCb, recv_cb: UdpRecvCb) c_int;
pub extern fn uv_udp_recv_stop(handle: *Udp) c_int;
pub extern fn uv_udp_set_broadcast(handle: *Udp, on: c_int) c_int;

pub extern fn uv_getaddrinfo(loop: *Loop, req: *GetAddrInfo, cb: ?GetAddrInfoCb, node: ?[*:0]const u8, service: ?[*:0]const u8, hints: ?*const AddrInfo) c_int;
pub extern fn uv_freeaddrinfo(ai: ?*AddrInfo) void;
pub extern fn uv_getnameinfo(loop: *Loop, req: *GetNameInfo, cb: ?GetNameInfoCb, addr: *const std.posix.sockaddr, flags: c_int) c_int;
pub extern fn uv_ip4_addr(ip: [*:0]const u8, port: c_int, addr: *std.posix.sockaddr.in) c_int;
pub extern fn uv_ip6_addr(ip: [*:0]const u8, port: c_int, addr: *std.posix.sockaddr.in6) c_int;
pub extern fn uv_ip_name(src: *const std.posix.sockaddr, dst: [*]u8, size: usize) c_int;
pub extern fn uv_inet_ntop(af: c_int, src: *const anyopaque, dst: [*]u8, size: usize) c_int;

pub extern fn uv_spawn(loop: *Loop, handle: *Process, options: *const ProcessOptions) c_int;
pub extern fn uv_process_kill(handle: *Process, signum: c_int) c_int;
pub extern fn uv_process_get_pid(handle: *const Process) Pid;

pub extern fn uv_strerror_r(err: c_int, buf: [*]u8, buflen: usize) [*:0]u8;
pub extern fn uv_err_name_r(err: c_int, buf: [*]u8, buflen: usize) [*:0]u8;
pub extern fn uv_translate_sys_error(sys_errno: c_int) c_int;
pub extern fn uv_disable_stdio_inheritance() void;

pub inline fn asHandle(ptr: anytype) *Handle {
    return @ptrCast(ptr);
}

pub inline fn asStream(ptr: anytype) *Stream {
    return @ptrCast(ptr);
}

pub inline fn asReq(ptr: anytype) *Req {
    return @ptrCast(ptr);
}

/// Every libuv handle and request starts with `void* data`, so the field is
/// reachable without knowing the concrete type.
pub inline fn getData(ptr: anytype) ?*anyopaque {
    return @as(*const ?*anyopaque, @ptrCast(@alignCast(ptr))).*;
}

pub inline fn setData(ptr: anytype, data: ?*anyopaque) void {
    @as(*?*anyopaque, @ptrCast(@alignCast(ptr))).* = data;
}

pub fn strerror(err: c_int, buf: []u8) [:0]const u8 {
    return std.mem.span(uv_strerror_r(err, buf.ptr, buf.len));
}

pub fn errName(err: c_int, buf: []u8) [:0]const u8 {
    return std.mem.span(uv_err_name_r(err, buf.ptr, buf.len));
}

/// Maps a libuv status back to the errno the Python layer should raise with.
///
/// On Windows the status is one of libuv's own fixed codes, and the value
/// handed to `OSError` has to be one CPython's exception mapping recognises:
/// Winsock codes for socket errors - which is what the `socket` module itself
/// raises there - and the CRT's errno values for the rest.
pub fn toErrno(err: c_int) c_int {
    if (!is_windows) return -err;
    return switch (err) {
        -4093 => 7, // E2BIG
        -4092 => 13, // EACCES
        -4091 => 10048, // EADDRINUSE -> WSAEADDRINUSE
        -4090 => 10049, // EADDRNOTAVAIL -> WSAEADDRNOTAVAIL
        -4089 => 10047, // EAFNOSUPPORT -> WSAEAFNOSUPPORT
        -4088 => 10035, // EAGAIN -> WSAEWOULDBLOCK
        -4084 => 10037, // EALREADY -> WSAEALREADY
        -4083 => 9, // EBADF
        -4082 => 16, // EBUSY
        -4081 => 105, // ECANCELED
        -4080 => 42, // ECHARSET -> EILSEQ
        -4079 => 10053, // ECONNABORTED -> WSAECONNABORTED
        -4078 => 10061, // ECONNREFUSED -> WSAECONNREFUSED
        -4077 => 10054, // ECONNRESET -> WSAECONNRESET
        -4076 => 10039, // EDESTADDRREQ -> WSAEDESTADDRREQ
        -4075 => 17, // EEXIST
        -4074 => 14, // EFAULT
        -4073 => 10065, // EHOSTUNREACH -> WSAEHOSTUNREACH
        -4072 => 4, // EINTR
        -4071 => 22, // EINVAL
        -4070 => 5, // EIO
        -4069 => 10056, // EISCONN -> WSAEISCONN
        -4068 => 21, // EISDIR
        -4067 => 114, // ELOOP
        -4066 => 24, // EMFILE
        -4065 => 10040, // EMSGSIZE -> WSAEMSGSIZE
        -4064 => 38, // ENAMETOOLONG
        -4063 => 10050, // ENETDOWN -> WSAENETDOWN
        -4062 => 10051, // ENETUNREACH -> WSAENETUNREACH
        -4061 => 23, // ENFILE
        -4060 => 10055, // ENOBUFS -> WSAENOBUFS
        -4059 => 19, // ENODEV
        -4058 => 2, // ENOENT
        -4057 => 12, // ENOMEM
        -4056 => 10050, // ENONET -> WSAENETDOWN
        -4055 => 28, // ENOSPC
        -4054 => 40, // ENOSYS
        -4053 => 10057, // ENOTCONN -> WSAENOTCONN
        -4052 => 20, // ENOTDIR
        -4051 => 41, // ENOTEMPTY
        -4050 => 10038, // ENOTSOCK -> WSAENOTSOCK
        -4049 => 129, // ENOTSUP
        -4048 => 1, // EPERM
        -4047 => 32, // EPIPE
        -4046 => 134, // EPROTO
        -4045 => 10043, // EPROTONOSUPPORT -> WSAEPROTONOSUPPORT
        -4044 => 10041, // EPROTOTYPE -> WSAEPROTOTYPE
        -4043 => 30, // EROFS
        -4042 => 10058, // ESHUTDOWN -> WSAESHUTDOWN
        -4041 => 29, // ESPIPE
        -4040 => 3, // ESRCH
        -4039 => 10060, // ETIMEDOUT -> WSAETIMEDOUT
        -4038 => 139, // ETXTBSY
        -4037 => 18, // EXDEV
        -4036 => 27, // EFBIG
        -4035 => 10042, // ENOPROTOOPT -> WSAENOPROTOOPT
        -4034 => 34, // ERANGE
        -4033 => 6, // ENXIO
        -4032 => 31, // EMLINK
        -4030 => 5, // EREMOTEIO -> EIO
        -4027 => 42, // EILSEQ
        -4026 => 132, // EOVERFLOW
        -4025 => 10044, // ESOCKTNOSUPPORT -> WSAESOCKTNOSUPPORT
        -4022 => 8, // ENOEXEC
        -4024 => 120, // ENODATA
        else => 22, // EINVAL for anything with no Windows analogue
    };
}
