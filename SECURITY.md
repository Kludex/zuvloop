# Security

## Reporting

Report a vulnerability through
[GitHub's advisory form](https://github.com/Kludex/zuvloop/security/advisories/new),
which keeps the report private until there is a fix. Please do not open a public
issue for one.

Expect an acknowledgement within a few days.

## Supported versions

Only the latest release. zuvloop is at `0.0.x` and nothing older is patched.

## What is in scope

zuvloop is a CPython extension written in Zig around a vendored libuv, so a
memory-safety fault here is reachable from ordinary Python:

- Reference counting: a leak, a double release, or a release that lets a
  finaliser see a structure mid-mutation.
- Descriptor ownership: a double close, or use of a descriptor libuv has closed.
- The hand-declared libuv ABI in `zig/uv.zig`, which is not generated from
  libuv's headers and can silently disagree with them.
- Anything that lets a peer's input reach memory it should not.

The vendored libuv is under `vendor/libuv`. Vulnerabilities in libuv itself
belong upstream; tell us as well, and the vendored copy will be updated.

Note that the released wheels are built `ReleaseFast`, which disables Zig's
bounds and overflow checks. That is deliberate for the hot paths, and it means a
bug that would trap in a debug build can corrupt memory in a released one.
