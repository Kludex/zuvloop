from __future__ import annotations

import argparse
import re
import tarfile
import zipfile
from email import message_from_bytes
from pathlib import Path

INTERPRETERS = ("cp314-cp314", "cp314-cp314t", "cp315-cp315", "cp315-cp315t")
PLATFORMS = (
    "manylinux_x86_64",
    "manylinux_aarch64",
    "musllinux_x86_64",
    "musllinux_aarch64",
    "macosx_x86_64",
    "macosx_arm64",
    "win_amd64",
    "win_arm64",
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Require exactly the distributions a zuvloop release ships.")
    parser.add_argument("dist", type=Path)
    parser.add_argument("--version", required=True)
    arguments = parser.parse_args()

    prefix = f"zuvloop-{arguments.version}"
    missing = {".tar.gz"} | {f"-{interpreter}-{platform}.whl" for interpreter in INTERPRETERS for platform in PLATFORMS}
    problems: list[str] = []
    for path in sorted(path for path in arguments.dist.rglob("*") if path.is_file()):
        # The platform's minimum OS version is the builder's choice, not part of what a release must ship.
        slot = re.sub(r"(manylinux|musllinux|macosx)_\d+_\d+_", r"\1_", path.name.removeprefix(prefix))
        if not path.name.startswith(prefix) or slot not in missing:
            problems.append(f"{path.name} is unexpected, duplicated, or not version {arguments.version}")
            continue
        missing.remove(slot)
        if path.suffix == ".whl":
            with zipfile.ZipFile(path) as wheel:
                metadata = b"".join(
                    wheel.read(name) for name in wheel.namelist() if name.endswith(".dist-info/METADATA")
                )
        else:
            with tarfile.open(path) as sdist:
                metadata = b"".join(
                    file.read()
                    for member in sdist
                    if member.name.count("/") == 1 and member.name.endswith("/PKG-INFO")
                    if (file := sdist.extractfile(member)) is not None
                )
        message = message_from_bytes(metadata)
        embedded = f"{message['Name']} {message['Version']}"
        if embedded != f"zuvloop {arguments.version}":
            problems.append(f"{path.name} embeds {embedded}, not zuvloop {arguments.version}")
    problems.extend(f"{prefix}{slot} is missing" for slot in sorted(missing))

    if problems:
        raise SystemExit("\n".join(problems))


if __name__ == "__main__":
    main()
