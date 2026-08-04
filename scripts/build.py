from __future__ import annotations

import subprocess
import sys
import sysconfig
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    """Rebuild `zuv._zuv` in place, against the interpreter running this script."""
    suffix = sysconfig.get_config_var("EXT_SUFFIX")
    free_threaded = str(bool(sysconfig.get_config_var("Py_GIL_DISABLED"))).lower()
    command = [
        "zig",
        "build",
        f"-Dpython-include={sysconfig.get_paths()['include']}",
        f"-Dext-path=_zuv{suffix}",
        f"-Dfree-threaded={free_threaded}",
        f"-Doptimize={'Debug' if '--debug' in sys.argv else 'ReleaseFast'}",
        "--prefix",
        str(ROOT / "src" / "zuv"),
    ]
    return subprocess.run(command, cwd=ROOT).returncode


if __name__ == "__main__":
    raise SystemExit(main())
