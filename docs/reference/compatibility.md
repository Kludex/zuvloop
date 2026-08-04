# Compatibility

zuv is checked by running the test suites of the projects that exercise an event
loop hardest, unmodified, with the loop swapped underneath.

| Suite | Result |
| --- | --- |
| uvicorn | 1257 passed, no failures |
| aiohttp | 4473 passed, 36 failed — 33 of which also fail on stock asyncio |

Three aiohttp failures are zuv's alone. Two are the `blockbuster` plugin flagging
`os.stat` inside `create_unix_server` — a call stdlib asyncio makes in the same
place, and which the plugin exempts by file path rather than by behaviour. The
third is a genuine difference, [below](#known-differences).

For reference, uvloop cannot complete that suite: it fails fifteen tests in
`test_client_functional.py` and then hangs.

## Where each loop diverges from asyncio

These were measured, not asserted — each is a case where one loop disagrees with
the standard library.

| Behaviour | asyncio | uvloop | zuv |
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
2430 combinations of host, port, family, type and flags, zuv disagrees with
`socket.getaddrinfo` on 81 and uvloop on 303.

## Known differences

**Patching `loop.time()` does not move the scheduler.** asyncio runs its timers
off `self.time()`, so replacing that method fast-forwards the loop — a trick test
suites use to expire timeouts without waiting. zuv keeps the timer heap in Zig
and reads the clock directly, so a patched `time()` changes what `loop.time()`
returns and nothing else.

Making the scheduler consult Python on every timer operation would cost more than
the compatibility is worth. Code that needs a controllable clock should schedule
against one explicitly.

**`sock_sendfile` and `sendfile` raise `NotImplementedError`.** asyncio falls
back to a read-and-write loop when a loop declines them, so this degrades rather
than breaks.

**`host=""` is not treated as `NULL`.** `socket.getaddrinfo` resolves the empty
string as an unspecified host; zuv raises `OSError`. This is the whole of its
81-case disagreement above.

## Not yet verified

CPython's own `test_asyncio` — the conformance suite — has not been run against
zuv. Everything on this page is a proxy for it.

Linux is exercised by CI on every commit, but the framework suites and every
benchmark here were run on macOS. libuv's Linux backend takes a different path
for stream I/O than kqueue does.
