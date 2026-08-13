# Vendored dependencies

## libuv

- Version: **1.51.0**
- Source: <https://dist.libuv.org/dist/v1.51.0/libuv-v1.51.0.tar.gz>
- SHA-256: `5f0557b90b1106de71951a3c3931de5e0430d78da1d9a10287ebc7a3f78ef8eb`

The tree under `libuv/` is the upstream release with the patches under `patches/libuv/` applied.
**Never edit it directly.** Update the corresponding patch instead. `build.zig` compiles the vendored
sources with the same defines and file lists as upstream's `CMakeLists.txt`.

`0001-expose-udp-recv-address-length.patch` preserves the kernel-reported `sockaddr` length through
libuv's UDP receive callback. Linux needs that length to distinguish abstract UNIX names whose bytes
differ only by trailing NULs. The updater applies the patch before replacing the existing tree and
fails if a future libuv release no longer accepts it cleanly.

A weekly workflow checks for a new signed release, verifies its signer against
`libuv-maintainer-keys.txt`, tests every supported target, and opens an update pull request. For a
manual update, verify the checksum through an independent channel, then run
`./vendor/update-libuv.sh <version> <sha256>` and re-check the file lists at the top of `build.zig`
against the new `CMakeLists.txt`.
