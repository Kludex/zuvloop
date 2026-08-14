from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]


def require_object(value: JsonValue, label: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def require_array(value: JsonValue, label: str) -> list[JsonValue]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a JSON array")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Add zuvloop's compiled native dependency to a uv CycloneDX SBOM.")
    parser.add_argument("sbom", type=Path)
    parser.add_argument("--version", required=True)
    arguments = parser.parse_args()

    document: JsonValue = json.loads(arguments.sbom.read_text())
    root = require_object(document, "SBOM")
    metadata = require_object(root.get("metadata"), "metadata")
    component = require_object(metadata.get("component"), "metadata.component")
    component["version"] = arguments.version
    component["purl"] = f"pkg:pypi/zuvloop@{arguments.version}"

    version_header = Path("vendor/libuv/include/uv/version.h").read_text()
    version_parts: list[str] = []
    for macro in ("UV_VERSION_MAJOR", "UV_VERSION_MINOR", "UV_VERSION_PATCH"):
        match = re.search(rf"^#define {macro} (\d+)$", version_header, re.MULTILINE)
        if match is None:
            raise ValueError(f"could not read {macro} from libuv's version header")
        version_parts.append(match.group(1))
    libuv_version = ".".join(version_parts)
    libuv_ref = f"libuv-{libuv_version}"

    components = require_array(root.get("components"), "components")
    components.append(
        {
            "type": "library",
            "bom-ref": libuv_ref,
            "name": "libuv",
            "version": libuv_version,
            "purl": f"pkg:github/libuv/libuv@v{libuv_version}",
            "licenses": [{"license": {"id": "MIT"}}],
            "properties": [{"name": "zuvloop:linkage", "value": "static"}],
        }
    )

    dependencies = require_array(root.get("dependencies"), "dependencies")
    root_dependency: dict[str, JsonValue] | None = None
    for dependency_value in dependencies:
        dependency = require_object(dependency_value, "dependency")
        if dependency.get("ref") == component.get("bom-ref"):
            root_dependency = dependency
            break
    if root_dependency is None:
        raise ValueError("SBOM has no dependency entry for zuvloop")
    depends_on = require_array(root_dependency.get("dependsOn"), "zuvloop dependsOn")
    depends_on.append(libuv_ref)
    dependencies.append({"ref": libuv_ref})

    arguments.sbom.write_text(json.dumps(root, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
