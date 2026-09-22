# Vendored dependencies

## libuv

- Version: **1.52.1**
- Source: <https://dist.libuv.org/dist/v1.52.1/libuv-v1.52.1.tar.gz>
- SHA-256: `66d511b9e6e334c0e62279eb234fbfb2b3110b1479c09b95b44c7afca8cff9e7`

The tree under `libuv/` is the upstream release with the patches under `patches/libuv/` applied.
**Never edit it directly.** Update the corresponding patch instead. `build.zig` compiles the vendored
sources with the same defines and file lists as upstream's `CMakeLists.txt`.

`0001-expose-udp-recv-address-length.patch` preserves both the kernel-reported source length and the
caller-supplied destination length through libuv's UDP paths. Linux needs those lengths to distinguish
abstract UNIX names whose bytes differ only by trailing NULs. The updater applies the patch before
replacing the existing tree and fails if a future libuv release no longer accepts it cleanly.

Socket adoption uses upstream's `uv_udp_open_ex()` with no flags to preserve existing reuse options.
Asyncio enables port reuse only when you pass `reuse_port=True`; changing the option during adoption
would let another local process bind the endpoint's address.

A weekly workflow checks for a new signed release, verifies its signer against
`libuv-maintainer-keys.txt`, tests every supported target, and opens an update pull request. For a
manual update, verify the checksum through an independent channel, then run
`./vendor/update-libuv.sh <version> <sha256>` and re-check the file lists at the top of `build.zig`
against the new `CMakeLists.txt`.
