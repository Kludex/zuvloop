from __future__ import annotations

import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class ZigBuildHook(BuildHookInterface[Any]):
    """Compiles `zuvloop._zuvloop` (and the vendored libuv) with `zig build`."""

    PLUGIN_NAME = "zig"

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if shutil.which("zig") is None:
            raise RuntimeError("zuvloop is built with Zig; install it from https://ziglang.org/download/")

        suffix = sysconfig.get_config_var("EXT_SUFFIX")
        root = Path(self.root)
        command = [
            "zig",
            "build",
            f"-Dpython-include={sysconfig.get_paths()['include']}",
            f"-Dext-path=_zuvloop{suffix}",
            "-Doptimize=ReleaseFast",
            "--prefix",
            str(root / "src" / "zuvloop"),
        ]
        subprocess.run(command, cwd=root, check=True, stdout=sys.stderr)

        build_data["pure_python"] = False
        build_data["infer_tag"] = True
        build_data["artifacts"].append(f"src/zuvloop/_zuvloop{suffix}")
