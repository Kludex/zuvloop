# Installation

```console
$ pip install zuv
```

zuv has one runtime dependency, `opentelemetry-api`. Not the SDK, and nothing
vendor-specific — see [Instrumentation](instrumentation.md) for why the API
alone is enough.

## Requirements

| | |
| --- | --- |
| Python | 3.14 or newer |
| Platform | Linux, macOS |

Windows is not supported. The loop is built on libuv's Unix backend, and the
socket setup is written against POSIX semantics.

libuv is vendored and compiled into the extension. There is nothing to install
separately, and no system libuv is used even if one is present.

## From source

Building from source needs [Zig](https://ziglang.org/download/) 0.16 on your
`PATH`:

```console
$ git clone https://github.com/Kludex/zuv
$ cd zuv
$ uv sync
```

The build is driven by a hatchling hook that runs `zig build` with
`-Doptimize=ReleaseFast`. libuv's C sources are compiled as part of the same Zig
module, so they get the same optimization level.

/// note | Verifying the build

A wheel built for the wrong interpreter fails at import, not at build time. If
you are unsure what you have, ask:

```console
$ python -c "import zuv; print(zuv.libuv_version())"
1.51.0
```
///

## Development

```console
$ uv run pytest          # tests, at 100% branch coverage
$ uv run mypy src tests  # strict
$ uv run ruff check .
```

The test suite is high-level: it drives the loop through the public asyncio API
rather than the native surface. There are no unit tests of Zig internals, on
purpose — the internals are free to change as long as asyncio's contract holds.
