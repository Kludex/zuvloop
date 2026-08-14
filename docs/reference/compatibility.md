# Compatibility

Compatibility is checked at three levels:

- Every change runs zuvloop's full suite on Linux and macOS and its portable
  suite on Windows. Linux also runs a subprocess-isolated selection of CPython's
  own `test_asyncio` mixins.
- A weekly job builds and tests the declared floor (CPython 3.14.0), the newest
  CPython 3.14 patch, and CPython 3.15 prereleases.
- The same weekly job runs the full test suites from immutable uvicorn 0.52.3 and
  aiohttp 3.14.3 commits, with `asyncio.new_event_loop` swapped for zuvloop before
  either project imports asyncio.

The pinned commits and exact commands live in
`.github/workflows/compatibility.yml`. Pinning makes a new upstream release an
explicit review instead of allowing an unrelated moving target to turn the signal
red. zuvloop's smaller in-repository aiohttp, anyio and uvicorn integration tests
still run on every change.

Tests that assert the identity of the platform's stock loop rather than an asyncio
behavior remain in the run as strict expected failures. Their exact node ids and
reasons are registered in `scripts/upstream/zuvloop_upstream_expectations.py`;
an unexpected pass is a failure too, so this list cannot quietly go stale.
aiohttp's optional `blockbuster` plugin is disabled for this run because it
exempts blocking calls by stdlib asyncio source filename and therefore reports
the equivalent `os.stat` and `os.sendfile` calls from every third-party loop.

For reference, uvloop cannot complete that suite: it fails fifteen tests in
`test_client_functional.py` and then hangs.

## Where each loop diverges from asyncio

These were measured, not asserted — each is a case where one loop disagrees with
the standard library.

| Behaviour | asyncio | uvloop | zuvloop |
| --- | --- | --- | --- |
| `isinstance(t, asyncio.Transport)` | True | False | True |
| `isinstance(t, asyncio.DatagramTransport)` | True | False | True |
| `isinstance(t, asyncio.ReadTransport)` for a read pipe | True | False | True |
| `get_extra_info("socket")` | `TransportSocket` | `PseudoSocket` | `TransportSocket` |
| `loop.time()` equals `time.monotonic()` | yes | yes | yes |
| `getaddrinfo("fe80::1%lo0")` keeps the zone | yes | **no** | yes |
| `connect_read_pipe` on a regular file | `ValueError` | **accepted** | `ValueError` |

The `isinstance` rows are not pedantry. aiohttp's test suite asserts
`isinstance(transport, asyncio.Transport)` inside `connection_made`; a loop that
fails it leaves the protocol half-initialised and the connection open with
nobody to answer on it.

The `getaddrinfo` row is the sharpest: uvloop's literal shortcut drops the IPv6
zone index, so `fe80::1%lo0` resolves to scope 0 — the wrong interface. Across
2430 combinations of host, port, family, type and flags, zuvloop disagrees with
`socket.getaddrinfo` on 81 and uvloop on 303.

## Known differences

**Patching `loop.time()` does not move the scheduler.** asyncio runs its timers
off `self.time()`, so replacing that method fast-forwards the loop — a trick test
suites use to expire timeouts without waiting. zuvloop keeps the timer heap in Zig
and reads the clock directly, so a patched `time()` changes what `loop.time()`
returns and nothing else.

Making the scheduler consult Python on every timer operation would cost more than
the compatibility is worth. Code that needs a controllable clock should schedule
against one explicitly.

**`host=""` is not treated as `NULL`.** `socket.getaddrinfo` resolves the empty
string as an unspecified host; zuvloop raises `OSError`. This is the whole of its
81-case disagreement above.

## Interpreter and platform policy

GIL-enabled CPython 3.14 and later is supported on Linux, macOS and Windows. The
minimum 3.14.0 release and the next CPython prerelease are continuously exercised
rather than inferred from the newest local interpreter.

Free-threaded CPython is not supported yet. The native loop saves a thread state
and deliberately releases and reacquires the GIL around `uv_run`; that ownership
model must be redesigned before a no-GIL build is safe. A free-threaded source
build fails at compile time with a direct explanation instead of producing a
wheel that fails later with an unresolved CPython symbol.

Linux glibc, Linux musl and macOS execute the full in-repository suite. Windows
AMD64 builds and runs the portable suite natively; tests that require Unix-domain
sockets, POSIX signal semantics, POSIX pipe descriptors or POSIX subprocess
expectations are explicitly skipped.
Windows does not provide Unix-domain socket methods or `SO_REUSEPORT` through
zuvloop. Published wheels cover Linux x86-64/AArch64, macOS x86-64/arm64 and
Windows AMD64.
