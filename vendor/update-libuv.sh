#!/usr/bin/env bash
# Replace vendor/libuv with a pristine upstream release tarball.
set -euo pipefail

version="${1:?usage: update-libuv.sh <version>, e.g. 1.51.0}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
url="https://dist.libuv.org/dist/v${version}/libuv-v${version}.tar.gz"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

curl -fsSL "$url" -o "$tmp/libuv.tar.gz"
tar xzf "$tmp/libuv.tar.gz" -C "$tmp"
rm -rf "$here/libuv"
mv "$tmp/libuv-v${version}" "$here/libuv"

sed -i.bak -E "s/\*\*[0-9]+\.[0-9]+\.[0-9]+\*\*/**${version}**/; s|dist/v[0-9]+\.[0-9]+\.[0-9]+/libuv-v[0-9]+\.[0-9]+\.[0-9]+|dist/v${version}/libuv-v${version}|g" "$here/README.md"
rm -f "$here/README.md.bak"

echo "vendored libuv ${version} from ${url}"
echo "now diff vendor/libuv/CMakeLists.txt against the source lists in build.zig"
