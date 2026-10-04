#!/usr/bin/env python3
"""Build the real Linux/x86_64 Lambda artifact, without Docker or AWS access.

Only small validation/transport modules are packaged; Spark, PyArrow and DuckDB
are deliberately excluded. Wheels are installed for Lambda's target platform.
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

DEPENDENCIES = [
    "jsonschema==4.26.0",
    "attrs==26.1.0",
    "jsonschema-specifications==2025.9.1",
    "referencing==0.37.0",
    "rpds-py==2026.6.3",
    "typing-extensions==4.16.0",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("dist/lambda.zip"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="commerce-lambda-") as scratch:
        stage = Path(scratch)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--target",
                str(stage),
                "--platform",
                "manylinux2014_x86_64",
                "--implementation",
                "cp",
                "--python-version",
                "3.11",
                "--only-binary=:all:",
                "--no-compile",
                *DEPENDENCIES,
            ],
            check=True,
        )
        package = stage / "src/commerce_platform"
        package.mkdir(parents=True)
        for filename in ("__init__.py", "aws.py", "validation.py", "observability.py"):
            shutil.copy2(root / "src/commerce_platform" / filename, package / filename)
        (stage / "contracts").mkdir()
        shutil.copy2(root / "contracts/event.schema.json", stage / "contracts/event.schema.json")
        (stage / "lambda_handler.py").write_text(
            "import sys\nfrom pathlib import Path\n"
            'sys.path.insert(0, str(Path(__file__).parent / "src"))\n'
            "from commerce_platform.aws import handler\n"
        )
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(stage.rglob("*")):
                if path.is_file() and "__pycache__" not in path.parts:
                    # Stable timestamps make repeated builds with identical wheels comparable.
                    entry = zipfile.ZipInfo(str(path.relative_to(stage)), (2026, 1, 1, 0, 0, 0))
                    entry.compress_type = zipfile.ZIP_DEFLATED
                    entry.external_attr = 0o644 << 16
                    archive.writestr(entry, path.read_bytes())
    print(f"{output}\nsha256={hashlib.sha256(output.read_bytes()).hexdigest()}")


if __name__ == "__main__":
    main()
