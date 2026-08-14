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

const uv_windows = [_][]const u8{
    "src/win/async.c",
    "src/win/core.c",
    "src/win/detect-wakeup.c",
    "src/win/dl.c",
    "src/win/error.c",
    "src/win/fs-event.c",
    "src/win/fs.c",
    "src/win/getaddrinfo.c",
    "src/win/getnameinfo.c",
    "src/win/handle.c",
    "src/win/loop-watcher.c",
    "src/win/pipe.c",
    "src/win/poll.c",
    "src/win/process-stdio.c",
    "src/win/process.c",
    "src/win/signal.c",
    "src/win/snprintf.c",
    "src/win/stream.c",
    "src/win/tcp.c",
    "src/win/thread.c",
    "src/win/tty.c",
    "src/win/udp.c",
    "src/win/util.c",
    "src/win/winapi.c",
    "src/win/winsock.c",
};

const windows_libraries = [_][]const u8{
    "psapi", "user32", "advapi32", "iphlpapi", "userenv", "ws2_32", "dbghelp", "ole32", "shell32",
};

pub fn build(b: *std.Build) void {
    // A wheel is built on whatever machine the job landed on, and without a
    // baseline default Zig compiles for that machine's CPU: the published
    // manylinux wheel carried AVX2, BMI and MOVBE, so it would raise SIGILL on
    // anything older than Haswell. Which instructions it needed depended on the
    // runner. `-Dcpu=native` is still there for a build meant for one machine.
    // Zig 0.16's native Windows ARM64 compiler crashes while compiling this
    // extension. CI runs the x86-64 compiler under Windows emulation and sets
    // the artifact target explicitly; every other build keeps its native OS
    // with a baseline CPU for portable wheels.
    const target_name = b.graph.environ_map.get("HATCH_ZIG_TARGET") orelse "";
    const default_target: std.Target.Query = if (target_name.len > 0) target: {
        var query = std.Target.Query.parse(.{ .arch_os_abi = target_name }) catch
            @panic("invalid HATCH_ZIG_TARGET");
        query.cpu_model = .baseline;
        break :target query;
    } else .{ .cpu_model = .baseline };
    const target = b.standardTargetOptions(.{ .default_target = default_target });
    const optimize = b.standardOptimizeOption(.{});
    const sanitize_c = b.option(std.zig.SanitizeC, "sanitize-c", "C undefined-behavior sanitizer mode");

    // hatch-ziglang passes the building interpreter through the environment, so a
    // wheel is always built against the interpreter that asked for it. The `-D`
    // options are for building by hand.
    const python_include = b.option([]const u8, "python-include", "CPython include directory") orelse
        (b.graph.environ_map.get("HATCH_ZIG_PYTHON_INCLUDE") orelse
            @panic("python-include is required: pass -Dpython-include or set HATCH_ZIG_PYTHON_INCLUDE"));
    const ext_suffix = b.option([]const u8, "ext-suffix", "Extension module suffix") orelse
        (b.graph.environ_map.get("HATCH_ZIG_EXT_SUFFIX") orelse ".so");
    const os = target.result.os.tag;

    const mod = b.createModule(.{
        .root_source_file = b.path("zig/module.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_c = sanitize_c,
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
    switch (os) {
        .macos, .ios, .watchos, .tvos => {
            uv_files.appendSlice(b.allocator, &uv_unix) catch @panic("OOM");
            uv_files.appendSlice(b.allocator, &uv_darwin) catch @panic("OOM");
            uv_flags.appendSlice(b.allocator, &.{
                "-D_DARWIN_UNLIMITED_SELECT=1",
                "-D_DARWIN_USE_64_BIT_INODE=1",
            }) catch @panic("OOM");
        },
        .linux => {
            uv_files.appendSlice(b.allocator, &uv_unix) catch @panic("OOM");
            uv_files.appendSlice(b.allocator, &uv_linux) catch @panic("OOM");
            uv_flags.appendSlice(b.allocator, &.{
                "-D_GNU_SOURCE",
                "-D_POSIX_C_SOURCE=200112",
            }) catch @panic("OOM");
        },
        .windows => {
            uv_files.appendSlice(b.allocator, &uv_windows) catch @panic("OOM");
            uv_flags.appendSlice(b.allocator, &.{
                "-DWIN32_LEAN_AND_MEAN",
                "-D_WIN32_WINNT=0x0A00",
                "-D_CRT_DECLARE_NONSTDC_NAMES=0",
            }) catch @panic("OOM");
        },
        else => @panic("unsupported target: zuvloop builds on Linux, macOS and Windows"),
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
    if (os == .windows) {
        for (windows_libraries) |name| mod.linkSystemLibrary(name, .{});
        // A `.pyd` resolves every symbol at link time, so the interpreter's
        // import library is required - unlike ELF and Mach-O, where the
        // dynamic loader binds the Python symbols at import.
        const libdir = b.option([]const u8, "python-libdir", "CPython import library directory") orelse
            (b.graph.environ_map.get("HATCH_ZIG_PYTHON_LIBDIR") orelse
                @panic("python-libdir is required on Windows: pass -Dpython-libdir or set HATCH_ZIG_PYTHON_LIBDIR"));
        const libname = b.option([]const u8, "python-lib", "CPython import library name") orelse
            (b.graph.environ_map.get("HATCH_ZIG_PYTHON_LIB") orelse
                @panic("python-lib is required on Windows: pass -Dpython-lib or set HATCH_ZIG_PYTHON_LIB"));
        mod.addLibraryPath(.{ .cwd_relative = libdir });
        mod.linkSystemLibrary(libname, .{});
    }

    // Relative to the install prefix, so with the default `zig-out` the module
    // lands beside the Python sources, which is where the build hook looks.
    b.getInstallStep().dependOn(&b.addInstallFileWithDir(
        lib.getEmittedBin(),
        .{ .custom = "../zuvloop" },
        b.fmt("_zuvloop{s}", .{ext_suffix}),
    ).step);
}
