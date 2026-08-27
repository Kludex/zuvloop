# Installation

```console
$ pip install zuvloop
```

zuvloop has two small runtime dependencies: `opentelemetry-api` for optional
instrumentation and `typing-extensions` for the shipped type declarations. It
does not install an OpenTelemetry SDK or anything vendor-specific — see
[Instrumentation](instrumentation.md) for why the API alone is enough.

## Requirements

| | |
| --- | --- |
| Python | CPython 3.14 or newer, including free-threaded builds |
| Platform | Linux, macOS, Windows |

Windows uses libuv's native Windows backend. Unix-domain sockets and
`SO_REUSEPORT` are unavailable there; the portable TCP, UDP, DNS, TLS,
scheduling and sendfile-fallback surfaces are exercised by native Windows CI. See
[Compatibility](../reference/compatibility.md#interpreter-and-platform-policy)
for the precise platform policy.

libuv is vendored and compiled into the extension. There is nothing to install
separately, and no system libuv is used even if one is present.

## From source

PEP 517 source builds install their own pinned Zig 0.16 toolchain in the isolated
build environment. A system Zig is not required:

```console
$ git clone https://github.com/Kludex/zuvloop
$ cd zuvloop
$ uv sync
```

The build is driven by a hatchling hook that runs `zig build` with
`-Doptimize=ReleaseFast`. libuv's C sources are compiled as part of the same Zig
module, so they get the same optimization level.

Direct native development commands such as `python scripts/build.py` do require
Zig 0.16 on `PATH`.

/// note | Verifying the build

A wheel built for the wrong interpreter fails at import, not at build time. If
you are unsure what you have, ask:

```console
$ python -c "import zuvloop; print(zuvloop.libuv_version())"
1.51.0
```
///

## Development

```console
$ uv run coverage run -m pytest
$ uv run coverage report # enforce 100% branch coverage
$ uv run mypy            # strict, including tests and scripts
$ uv run python -m mypy.stubtest zuvloop --allowlist stubtest_allowlist.txt
$ uv run ruff check .
```

The test suite is high-level: it drives the loop through the public asyncio API
rather than coupling to Zig internals. Property tests compare numeric DNS and
scheduling behaviour with the standard library. A scheduled ReleaseSafe build
also enables C undefined-behaviour checks and runs a repeated lifecycle, socket,
DNS and cross-thread soak test under Python's debug allocator.
