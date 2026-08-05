# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
follows [semantic versioning](https://semver.org/spec/v2.0.0.html) - with the
usual caveat that anything below `1.0.0` may break between minor versions.

## Unreleased

### Fixed

- `connection_lost` receives the reason the transport closed rather than the
  `ECANCELED` libuv reports for writes cancelled by the close itself. `abort()`
  delivered `OSError(89)` where asyncio and uvloop deliver `None`, and a peer
  reset arrived as a cancellation instead of `BrokenPipeError`.
- `write()`, `writelines()` and `sendto()` drop what they are given on a closing
  transport rather than raising `RuntimeError`. Only `write_eof()` makes a later
  write an error, as in asyncio.
- A read callback that raises closes the connection and delivers the exception
  to `connection_lost`, instead of being reported and then handed the next chunk.
- A connected datagram endpoint accepts `sendto(data, addr)` when `addr` names
  the peer it is connected to.
- A port outside 0-65535 is refused rather than truncated: a datagram addressed
  to port 70000 was being delivered to port 4464.
- `getnameinfo` raises `socket.gaierror` for a host that is not an address
  literal, rather than the `OSError(EINVAL)` libuv reports for refusing to parse
  it.
- Three places in the native layer released a Python reference while the
  structure holding it was still half-updated, where a finaliser could reach it:
  the reader and writer registrations, a `uv_process_t` freed after a failed
  spawn while libuv still had it queued, and the timer heap's compaction.
- `create_unix_server` reports "address already in use" on platforms whose
  `EADDRINUSE` is not Linux's 98.

### Added

- The Zig is compiled in CI for every target a wheel is published for. Two
  Linux-only defects have shipped that this would have caught.
- `SECURITY.md`, `CONTRIBUTING.md` and this file.

## 0.0.1 - 2026-08-05

First release. A libuv event loop for CPython 3.14, written in Zig.

- The whole `AbstractEventLoop` surface: connections, servers, TLS, datagrams,
  pipes, subprocesses, signals, readers and writers, and name resolution.
- OpenTelemetry spans and metrics, sampled natively, with no runtime dependency
  on an SDK.
- Wheels for macOS and Linux, x86-64 and arm64, glibc and musl.
- 88 of CPython's 92 `test_asyncio` loop conformance tests pass; the four skips
  are white-box tests of CPython's own internals.
