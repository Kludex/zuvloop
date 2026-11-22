# Vendored dependencies

## libuv

- Version: **1.53.0**
- Source: <https://dist.libuv.org/dist/v1.53.0/libuv-v1.53.0.tar.gz>
- SHA-256: `cb0d6dd2128d5a95bd242c6cc982a24fe608fa93da57b6b4ec763b0018c53e64`

The tree under `libuv/` is the upstream release with the patches under `patches/libuv/` applied.
**Never edit it directly.** Update the corresponding patch instead. `build.zig` compiles the vendored
sources with the same defines and file lists as upstream's `CMakeLists.txt`.

`0001-expose-udp-recv-address-length.patch` preserves both the kernel-reported source length and the
caller-supplied destination length through libuv's UDP paths. Linux needs those lengths to distinguish
abstract UNIX names whose bytes differ only by trailing NULs. The updater applies the patch before
replacing the existing tree and fails if a future libuv release no longer accepts it cleanly.

`0002-unregister-write-request-on-allocation-failure.patch` backports upstream's
[libuv#5291](https://github.com/libuv/libuv/pull/5291). `uv_write` counted a request as active before
allocating its buffer list and did not undo that when the allocation failed, so the loop never saw
itself as idle again and `loop.close()` hung. Drop the patch when updating to a release that includes
the fix: the updater fails on it then, because it no longer applies.

`0003-report-stream-write-progress.patch` adds `uv_stream_set_write_progress_cb()`. libuv reports a
write only when a whole request completes, so nothing tells the caller that `write_queue_size` has
fallen partway through one. The callback runs after each write the loop makes on the stream, which
is how a paused protocol is resumed at its low-water mark without the loop polling every paused
connection. It is Unix only: on Windows the queue size changes when a request completes, which the
request's own callback already reports.

Socket adoption uses upstream's `uv_udp_open_ex()` with no flags to preserve existing reuse options.
Asyncio enables port reuse only when you pass `reuse_port=True`; changing the option during adoption
would let another local process bind the endpoint's address.

A weekly workflow checks for a new signed release, verifies its signer against
`libuv-maintainer-keys.txt`, runs Linux tests and cross-compilation, and opens an update pull request.
After creating or updating the pull request, it explicitly dispatches CI, compatibility, and native
hardening on the update branch. Pushes made with `GITHUB_TOKEN` do not trigger workflows, and its
pull-request events can require manual approval. Explicit dispatch starts the required merge checks
automatically.

For a manual update, verify the checksum through an independent channel, then run
`./vendor/update-libuv.sh <version> <sha256>` and re-check the file lists at the top of `build.zig`
against the new `CMakeLists.txt`.
