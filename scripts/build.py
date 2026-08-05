from __future__ import annotations

import subprocess
import sys
import sysconfig
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    """Rebuild `zuvloop._zuvloop` in place, against the interpreter running this script."""
    command = [
        "zig",
        "build",
        f"-Dpython-include={sysconfig.get_paths()['include']}",
        f"-Dext-suffix={sysconfig.get_config_var('EXT_SUFFIX')}",
        f"-Doptimize={'Debug' if '--debug' in sys.argv else 'ReleaseFast'}",
    ]
    return subprocess.run(command, cwd=ROOT).returncode


if __name__ == "__main__":
    raise SystemExit(main())
