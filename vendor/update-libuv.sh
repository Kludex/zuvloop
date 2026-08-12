#!/usr/bin/env bash
# Replace vendor/libuv with a pristine upstream release tarball.
set -euo pipefail

version="${1:?usage: update-libuv.sh <version> <sha256>, e.g. 1.51.0 5f0557...}"
checksum="${2:?supply the SHA-256 published for libuv ${version}}"
if [[ ! "$checksum" =~ ^[[:xdigit:]]{64}$ ]]; then
    echo "invalid SHA-256: $checksum" >&2
    exit 2
fi
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
url="https://dist.libuv.org/dist/v${version}/libuv-v${version}.tar.gz"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

curl -fsSL "$url" -o "$tmp/libuv.tar.gz"
printf '%s  %s\n' "$checksum" "$tmp/libuv.tar.gz" | shasum -a 256 -c -
tar xzf "$tmp/libuv.tar.gz" -C "$tmp"
rm -rf "$here/libuv"
mv "$tmp/libuv-v${version}" "$here/libuv"

sed -i.bak -E "s/\*\*[0-9]+\.[0-9]+\.[0-9]+\*\*/**${version}**/; s|dist/v[0-9]+\.[0-9]+\.[0-9]+/libuv-v[0-9]+\.[0-9]+\.[0-9]+|dist/v${version}/libuv-v${version}|g; s|SHA-256: `[[:xdigit:]]{64}`|SHA-256: `${checksum}`|" "$here/README.md"
rm -f "$here/README.md.bak"

echo "vendored libuv ${version} from ${url}"
echo "now diff vendor/libuv/CMakeLists.txt against the source lists in build.zig"
