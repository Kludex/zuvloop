$ErrorActionPreference = "Stop"

$version = "0.16.0"
$archive = Join-Path $env:RUNNER_TEMP "zig-x86_64-windows-$version.zip"
$destination = Join-Path $env:RUNNER_TEMP "zig-x86_64-windows"
$expectedSha256 = "68659eb5f1e4eb1437a722f1dd889c5a322c9954607f5edcf337bc3684a75a7e"

Invoke-WebRequest "https://ziglang.org/download/$version/zig-x86_64-windows-$version.zip" -OutFile $archive
$actualSha256 = (Get-FileHash $archive -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actualSha256 -ne $expectedSha256) {
    throw "Zig archive SHA-256 mismatch: expected $expectedSha256, got $actualSha256"
}

Expand-Archive -LiteralPath $archive -DestinationPath $destination -Force
$zigDirectory = Join-Path $destination "zig-x86_64-windows-$version"
$zigDirectory | Out-File -FilePath $env:GITHUB_PATH -Encoding utf8 -Append
