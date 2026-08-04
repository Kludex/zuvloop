# Vendored dependencies

## libuv

- Version: **1.51.0**
- Source: <https://dist.libuv.org/dist/v1.51.0/libuv-v1.51.0.tar.gz>

The tree under `libuv/` is the upstream release tarball, extracted verbatim. **Never edit it.**
Everything zuv adds lives in `zig/`, and `build.zig` compiles the vendored sources with the same
defines and file lists as upstream's `CMakeLists.txt`.

To update, run `./vendor/update-libuv.sh <version>` and re-check the file lists at the top of
`build.zig` against the new `CMakeLists.txt`.
