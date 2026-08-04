const std = @import("std");

const uv_common = [_][]const u8{
    "src/fs-poll.c",
    "src/idna.c",
    "src/inet.c",
    "src/random.c",
    "src/strscpy.c",
    "src/strtok.c",
    "src/thread-common.c",
    "src/threadpool.c",
    "src/timer.c",
    "src/uv-common.c",
    "src/uv-data-getter-setters.c",
    "src/version.c",
};

const uv_unix = [_][]const u8{
    "src/unix/async.c",
    "src/unix/core.c",
    "src/unix/dl.c",
    "src/unix/fs.c",
    "src/unix/getaddrinfo.c",
    "src/unix/getnameinfo.c",
    "src/unix/loop-watcher.c",
    "src/unix/loop.c",
    "src/unix/pipe.c",
    "src/unix/poll.c",
    "src/unix/process.c",
    "src/unix/random-devurandom.c",
    "src/unix/signal.c",
    "src/unix/stream.c",
    "src/unix/tcp.c",
    "src/unix/thread.c",
    "src/unix/tty.c",
    "src/unix/udp.c",
};

const uv_darwin = [_][]const u8{
    "src/unix/bsd-ifaddrs.c",
    "src/unix/darwin-proctitle.c",
    "src/unix/darwin.c",
    "src/unix/fsevents.c",
    "src/unix/kqueue.c",
    "src/unix/proctitle.c",
    "src/unix/random-getentropy.c",
};

const uv_linux = [_][]const u8{
    "src/unix/linux.c",
    "src/unix/procfs-exepath.c",
    "src/unix/proctitle.c",
    "src/unix/random-getrandom.c",
    "src/unix/random-sysctl-linux.c",
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const python_include = b.option([]const u8, "python-include", "CPython include directory") orelse
        @panic("-Dpython-include is required");
    const ext_path = b.option([]const u8, "ext-path", "Installed name of the extension module") orelse
        "_zuvloop.so";
    const os = target.result.os.tag;

    const mod = b.createModule(.{
        .root_source_file = b.path("zig/module.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .pic = true,
    });
    mod.addIncludePath(.{ .cwd_relative = python_include });
    mod.addIncludePath(b.path("vendor/libuv/include"));
    mod.addIncludePath(b.path("vendor/libuv/src"));

    var uv_flags: std.ArrayList([]const u8) = .empty;
    uv_flags.appendSlice(b.allocator, &.{
        "-std=gnu11",
        "-fno-strict-aliasing",
        "-D_FILE_OFFSET_BITS=64",
        "-D_LARGEFILE_SOURCE",
        "-Wno-unused-parameter",
    }) catch @panic("OOM");

    var uv_files: std.ArrayList([]const u8) = .empty;
    uv_files.appendSlice(b.allocator, &uv_common) catch @panic("OOM");
    uv_files.appendSlice(b.allocator, &uv_unix) catch @panic("OOM");
    switch (os) {
        .macos, .ios, .watchos, .tvos => {
            uv_files.appendSlice(b.allocator, &uv_darwin) catch @panic("OOM");
            uv_flags.appendSlice(b.allocator, &.{
                "-D_DARWIN_UNLIMITED_SELECT=1",
                "-D_DARWIN_USE_64_BIT_INODE=1",
            }) catch @panic("OOM");
        },
        .linux => {
            uv_files.appendSlice(b.allocator, &uv_linux) catch @panic("OOM");
            uv_flags.appendSlice(b.allocator, &.{
                "-D_GNU_SOURCE",
                "-D_POSIX_C_SOURCE=200112",
            }) catch @panic("OOM");
        },
        else => @panic("unsupported target: zuvloop builds on Linux and macOS"),
    }

    mod.addCSourceFiles(.{
        .root = b.path("vendor/libuv"),
        .files = uv_files.items,
        .flags = uv_flags.items,
    });

    const lib = b.addLibrary(.{
        .name = "_zuvloop",
        .root_module = mod,
        .linkage = .dynamic,
    });
    lib.linker_allow_shlib_undefined = true;
    lib.bundle_compiler_rt = true;
    if (os == .linux) mod.linkSystemLibrary("rt", .{});

    b.getInstallStep().dependOn(&b.addInstallFileWithDir(
        lib.getEmittedBin(),
        .prefix,
        ext_path,
    ).step);
}
